mod collector;

use actix_web::{
    get, http::header::ContentType, web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use collector::{PGLocksCollector, PGPostmasterCollector};
use config::Config;
use prometheus::{Encoder, Registry};

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct PGEConfig {
    listen_addr: String,
}

struct PGEApp {
    db: Pool<Postgres>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = Config::builder()
        // Add in `./pg_exporter.yml`
        .add_source(config::File::with_name("./pg_exporter.yml"))
        // Add in settings from the environment (with a prefix of PGE)
        // Eg.. `PGE_DEBUG=1 ./target/app` would set the `debug` key
        .add_source(config::Environment::with_prefix("PGE"))
        .build()
        .unwrap();

    let pge_config: PGEConfig = settings.try_deserialize().unwrap();

    println!("starting pg_exporter at {:?}", pge_config.listen_addr);

    let pool = match PgPoolOptions::new()
        .max_connections(10)
        .connect("postgres://postgres_exporter:postgres_exporter@pg:5432/db1")
        .await
    {
        Ok(pool) => {
            println!("✅Connection to the database is successful!");
            pool
        }
        Err(err) => {
            println!("🔥 Failed to connect to the database: {:?}", err);
            std::process::exit(1);
        }
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(PGEApp { db: pool.clone() }))
            .service(hello)
            .route("/metrics", web::get().to(metrics))
    })
    .bind(pge_config.listen_addr)?
    .run()
    .await
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("This is a pg_exporter for Prometheus written in Rust")
}

async fn metrics(req: HttpRequest, data: web::Data<PGEApp>) -> impl Responder {
    println!(
        "processing the request ua {:?}",
        req.headers()
            .get("user-agent")
            .expect("should be user-agent string")
    );

    let pc = PGLocksCollector::new("test_ns", data.db.clone());
    
    let res = pc.update().await;
    res.unwrap();

    let r = Registry::new();
    let _res = r.register(Box::new(pc)).unwrap();

    let pc_pstm = PGPostmasterCollector::new("test_ns", data.db.clone());

    let res2 = pc_pstm.update().await;
    res2.unwrap();
    let _res2 = r.register(Box::new(pc_pstm)).unwrap();

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let metric_families = r.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = String::from_utf8(buffer.clone()).expect("Failed to convert bytes to string");
    buffer.clear();

    HttpResponse::Ok()
            .insert_header(ContentType::plaintext())
            .body(response)
}

mod collectors;
mod instance;

use std::collections::HashMap;

use actix_web::{
    get, http::header::ContentType, web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};

use config::Config;
use prometheus::{Encoder, Registry};

use sqlx::{postgres::PgPoolOptions, Pool, Postgres};



#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct PGEConfig {
    listen_addr: String,
    instances: HashMap<String, instance::Config>,
}



#[derive(Debug, Clone)]
struct PGEApp {
    instances: Vec<instance::PostgresDB>,
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

    let mut app = PGEApp {
        instances: Vec::<instance::PostgresDB>::new(),
    };

    for instance in pge_config.instances {
        println!("starting connection for instance: {:?}", instance.0);
        let mut labels  = HashMap::<String, String>::new();
        
        labels.insert("namespace".to_string(), "test_ns".to_string());
        labels.insert("instance".to_string(), instance.0);

        let pg_instance = instance::new(instance.1.dsn, instance.1.exclude_db_names, labels);
        let pgi = pg_instance.await;

        app.instances.push(pgi);
    }

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app.clone()))
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
        "processing the request from {:?}",
        req.headers()
            .get("user-agent")
            .expect("should be user-agent string")
    );

    let r = Registry::new();
    for instance in &data.instances {
        let pc = collectors::pg_locks::new("test_ns", instance.db.clone());

        let res = pc.update().await;
        res.unwrap();

        let _res = r.register(Box::new(pc)).unwrap();

        let pc_pstm = collectors::pg_postmaster::new("test_ns", instance.db.clone());

        let res2 = pc_pstm.update().await;
        res2.unwrap();
        let _res2 = r.register(Box::new(pc_pstm)).unwrap();
    }

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

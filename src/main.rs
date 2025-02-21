mod collector;

use actix_web::{get, http::header::ContentType, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use collector::PGLocksCollector;
use config::Config;
use prometheus::{Registry, Encoder};

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct PGEConfig {
    listen_addr: String,
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

    HttpServer::new(|| {
        App::new()
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

async fn metrics(req: HttpRequest) -> impl Responder {
    println!("processing the request ua {:?}", req.headers().get("user-agent").expect("should be user-agent string"));

    let pc = PGLocksCollector::new("test_ns");

    let r = Registry::new();
    let _res = r.register(Box::new(pc)).unwrap();

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let metric_families = r.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = String::from_utf8(buffer.clone()).expect("Failed to convert bytes to string");
    buffer.clear();

    Ok::<HttpResponse, std::io::Error>(HttpResponse::Ok()
    .insert_header(ContentType::plaintext())
    .body(response))
}

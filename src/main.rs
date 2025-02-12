use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

mod config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = config::Config::from_config_file("/etc/myconfig.toml").unwrap();
    HttpServer::new(|| {
        App::new()
            .service(hello)
            .route("/metrics", web::get().to(metrics))
    })
    .bind(("0.0.0.0", 61488))?
    .run()
    .await
}




#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("This is a pg_exporter for Prometheus written in Rust")
}

async fn metrics() -> impl Responder {
    HttpResponse::Ok().body("This is metrics endpoint!")
}

mod collectors;
mod error;
mod instance;

use std::{collections::HashMap, sync::Arc};

use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer, Responder, get, http::header::ContentType, web,
};

use config::Config;
use prometheus::{Encoder, Registry};

use crate::error::MetricsError;

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct PGEConfig {
    listen_addr: String,
    instances: HashMap<String, instance::Config>,
}

#[derive(Clone)]
struct PGEApp {
    instances: Vec<Arc<instance::PostgresDB>>,
    collectors: Vec<Box<dyn collectors::PG>>,
    registry: Registry,
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
        .expect("settings should be initialized");

    let pge_config: PGEConfig = settings
        .try_deserialize()
        .expect("config should be initialized");

    println!("starting pg_exporter at {:?}", pge_config.listen_addr);

    let mut app = PGEApp {
        instances: Vec::<Arc<instance::PostgresDB>>::new(),
        collectors: Vec::new(),
        registry: Registry::new(),
    };

    for (instance, config) in pge_config.instances {
        println!("starting connection for instance: {instance}");

        let pgi = instance::new(
            config.dsn,
            config.exclude_db_names.clone(),
            config.const_labels.clone(),
        )
        .await
        .expect("postgres instance should be initialized");

        let arc_pgi = Arc::new(pgi);

        let pc = collectors::pg_locks::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pc.clone()))
            .expect("pg locks collector should be initialized");

        let pc_pstm = collectors::pg_postmaster::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pc_pstm.clone()))
            .expect("pg postmaster collector should be initialized");

        let pcdb = collectors::pg_database::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pcdb.clone()))
            .expect("pg database collector should be initialized");

        let pca = collectors::pg_activity::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pca.clone()))
            .expect("pg activity collector should be initialized");

        let pbgwr = collectors::pg_bgwirter::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pbgwr.clone()))
            .expect("pg bg writer collector should be initialized");

        let pgwalc = collectors::pg_wal::new(arc_pgi.clone());
        app.registry
            .register(Box::new(pgwalc.clone()))
            .expect("pg wal collector should be initialized");

        let pg_statio_c = collectors::pg_stat_io::new(arc_pgi.clone());
        // TODO:  change to if let Some(pg_statio_c) = pg_statio_c
        if pg_statio_c.is_some() {
            let c = pg_statio_c.expect("pg statio collector should be initialized");
            app.registry
                .register(Box::new(c.clone()))
                .expect("pg locks collector should be registered");
            app.collectors.push(Box::new(c.clone()));
        }

        app.collectors.push(Box::new(pc));
        app.collectors.push(Box::new(pc_pstm));
        app.collectors.push(Box::new(pcdb));
        app.collectors.push(Box::new(pca));
        app.collectors.push(Box::new(pbgwr));
        app.collectors.push(Box::new(pgwalc));

        app.instances.push(arc_pgi);
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

async fn metrics(req: HttpRequest, data: web::Data<PGEApp>) -> Result<HttpResponse, MetricsError> {
    println!(
        "processing the request from {:?}",
        req.headers()
            .get("user-agent")
            .expect("should be user-agent string")
    );

    let tasks: Vec<_> = data
        .collectors
        .clone()
        .into_iter()
        .map(|col| {
            actix_web::rt::spawn(async move {
                let update_result = col.update().await;
                match update_result {
                    Ok(update) => update,
                    Err(err) => println!("Problem running update collector: {err}"),
                };
            })
        })
        .collect();

    for task in tasks {
        task.await?;
    }

    let process_metrics = prometheus::gather();

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let postgres_metrics = data.registry.gather();
    encoder.encode(&postgres_metrics, &mut buffer)?;
    encoder.encode(&process_metrics, &mut buffer)?;

    let response = String::from_utf8(buffer.clone()).expect("Failed to convert bytes to string");
    buffer.clear();

    let resp = HttpResponse::Ok()
        .insert_header(ContentType::plaintext())
        .body(response);

    Ok(resp)
}

#![warn(clippy::unwrap_used)]
mod collectors;
mod error;
mod instance;

use std::{collections::HashMap, sync::Arc};

use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer, Responder, get, http::header::ContentType, web,
};

use config::Config;
use prometheus::{Encoder, Registry};
use tracing::info;

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

    tracing_subscriber::fmt()
        .with_writer(std::io::stdout) // Explicitly set stdout as the writer
        .init();

    info!("starting pg_exporter at {:?}", pge_config.listen_addr);

    let mut app = PGEApp {
        instances: Vec::<Arc<instance::PostgresDB>>::new(),
        collectors: Vec::new(),
        registry: Registry::new(),
    };

    for (instance, config) in pge_config.instances {
        info!("starting connection for instance: {instance}");

        let pgi = instance::new(&instance::Config {
            dsn: config.dsn,
            exclude_db_names: config.exclude_db_names.clone(),
            const_labels: config.const_labels.clone(),
            collect_top_query: config.collect_top_query,
            collect_top_index: config.collect_top_index,
            no_track_mode: config.no_track_mode,
        })
        .await
        .expect("postgres instance should be initialized");

        let arc_pgi = Arc::new(pgi);

        if let Some(pc_locks) = collectors::pg_locks::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pc_locks.clone()))
                .expect("pg locks collector should be initialized");
            app.collectors.push(Box::new(pc_locks));
        }

        if let Some(pc_pstm) = collectors::pg_postmaster::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pc_pstm.clone()))
                .expect("pg postmaster collector should be initialized");
            app.collectors.push(Box::new(pc_pstm));
        }

        if let Some(pcdb) = collectors::pg_database::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pcdb.clone()))
                .expect("pg database collector should be initialized");
            app.collectors.push(Box::new(pcdb));
        }

        if let Some(pac) = collectors::pg_activity::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pac.clone()))
                .expect("pg activity collector should be initialized");
            app.collectors.push(Box::new(pac));
        }

        if let Some(pbgwr) = collectors::pg_bgwirter::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pbgwr.clone()))
                .expect("pg bgwriter collector should be initialized");
            app.collectors.push(Box::new(pbgwr));
        }

        if let Some(pgwalc) = collectors::pg_wal::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pgwalc.clone()))
                .expect("pg wal collector should be initialized");
            app.collectors.push(Box::new(pgwalc));
        }

        if let Some(pg_statio_c) = collectors::pg_stat_io::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pg_statio_c.clone()))
                .expect("pg statio collector should be initialized");
            app.collectors.push(Box::new(pg_statio_c));
        }

        if let Some(pgarch_c) = collectors::pg_archiver::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pgarch_c.clone()))
                .expect("pg archiver collector should be initialized");
            app.collectors.push(Box::new(pgarch_c));
        }

        if let Some(pgconflc) = collectors::pg_conflict::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pgconflc.clone()))
                .expect("pg conflict collector should be initialized");
            app.collectors.push(Box::new(pgconflc));
        }

        if let Some(pgidx_c) = collectors::pg_indexes::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pgidx_c.clone()))
                .expect("pg indexes collector should be initialized");
            app.collectors.push(Box::new(pgidx_c));
        }

        if let Some(pgstmt_c) = collectors::pg_statements::new(Arc::clone(&arc_pgi)) {
            app.registry
                .register(Box::new(pgstmt_c.clone()))
                .expect("pg indexes collector should be initialized");
            app.collectors.push(Box::new(pgstmt_c));
        }

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

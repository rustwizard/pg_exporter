#![warn(clippy::unwrap_used)]
mod collectors;
mod config;
mod error;
mod instance;

use clap::Parser;
use pg_exporter::util::version;
use std::path::Path;
use std::sync::Arc;
use std::{io, process::exit};

use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer, Responder, get, http::header::ContentType, web,
};

use prometheus::{Encoder, Registry};
use tracing::{error, info};

use crate::config::{ExporterConfig, PGEConfig};
use crate::error::MetricsError;
use pg_exporter::cli::{self, Commands};

#[derive(Clone)]
struct PGEApp {
    instances: Vec<Arc<instance::PostgresDB>>,
    collectors: Vec<Box<dyn collectors::PG>>,
    registry: Registry,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = cli::Cli::parse();

    pg_exporter::logger_init();

    let mut overrides = PGEConfig::default();

    match args.command {
        Some(Commands::Configcheck) => {
            if let Err(e) = ExporterConfig::load(Path::new(&args.config)) {
                error!("{}", e);
                exit(1);
            }

            info!("✅ config valid");
            exit(0);
        }

        Some(Commands::Run {
            ref listen_addr,
            ref endpoint,
        }) => {
            overrides.listen_addr = listen_addr.clone();
            overrides.endpoint = endpoint.clone();

            info!(
                "🐘 PgExporter Run command executed. listen_addr: {:?}, endpoint: {:?}",
                overrides.listen_addr, overrides.endpoint
            );
        }

        _ => (),
    }

    let mut ec: ExporterConfig = match ExporterConfig::load(Path::new(&args.config)) {
        Ok(conf) => conf,
        Err(e) => {
            error!("can't load config. {}", e);
            return Err(io::Error::new(
                io::ErrorKind::InvalidFilename,
                "invalid file name",
            ));
        }
    };

    // TODO: maybe put this to the separtate method for override config
    if let Some(listen_addr) = overrides.listen_addr {
        ec.config.listen_addr = Some(listen_addr);
    }

    if let Some(endpoint) = overrides.endpoint {
        ec.config.endpoint = Some(endpoint);
    }

    info!(
        "🐘 PgExporter at http://{}{} with version {} and config({:?})",
        ec.config.listen_addr.clone().unwrap_or_default(),
        ec.config.endpoint.clone().unwrap_or_default(),
        version(),
        ec.config_path,
    );

    match pgexporter(args.command, ec).await {
        Ok(_) => return Ok(()),
        Err(e) => {
            error!("PgExporter crashed with error: {}", e);
            return Err(io::Error::new(io::ErrorKind::Interrupted, e));
        }
    };
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("This is a PgExporter for Prometheus written in Rust")
}

async fn metrics(req: HttpRequest, data: web::Data<PGEApp>) -> Result<HttpResponse, MetricsError> {
    info!(
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
                    Err(err) => error!("Problem running update collector: {err}"),
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

async fn pgexporter(command: Option<Commands>, ec: ExporterConfig) -> anyhow::Result<()> {
    match command {
        None | Some(Commands::Run { .. }) => {
            let mut app = PGEApp {
                instances: Vec::<Arc<instance::PostgresDB>>::new(),
                collectors: Vec::new(),
                registry: Registry::new(),
            };

            for (instance, config) in ec.config.instances.unwrap_or_default() {
                info!("starting connection for instance: {instance}");

                let pgi = instance::new(&config::Instance {
                    dsn: config.dsn,
                    exclude_db_names: config.exclude_db_names.clone(),
                    const_labels: config.const_labels.clone(),
                    collect_top_query: config.collect_top_query,
                    collect_top_index: config.collect_top_index,
                    no_track_mode: config.no_track_mode,
                })
                .await?;

                let arc_pgi = Arc::new(pgi);

                if let Some(pc_locks) = collectors::pg_locks::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pc_locks.clone()))?;
                    app.collectors.push(Box::new(pc_locks));
                }

                if let Some(pc_pstm) = collectors::pg_postmaster::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pc_pstm.clone()))?;
                    app.collectors.push(Box::new(pc_pstm));
                }

                if let Some(pcdb) = collectors::pg_database::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pcdb.clone()))?;
                    app.collectors.push(Box::new(pcdb));
                }

                if let Some(pac) = collectors::pg_activity::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pac.clone()))?;
                    app.collectors.push(Box::new(pac));
                }

                if let Some(pbgwr) = collectors::pg_bgwirter::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pbgwr.clone()))?;
                    app.collectors.push(Box::new(pbgwr));
                }

                if let Some(pgwalc) = collectors::pg_wal::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pgwalc.clone()))?;
                    app.collectors.push(Box::new(pgwalc));
                }

                if let Some(pg_statio_c) = collectors::pg_stat_io::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pg_statio_c.clone()))?;
                    app.collectors.push(Box::new(pg_statio_c));
                }

                if let Some(pgarch_c) = collectors::pg_archiver::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pgarch_c.clone()))?;
                    app.collectors.push(Box::new(pgarch_c));
                }

                if let Some(pgconflc) = collectors::pg_conflict::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pgconflc.clone()))?;
                    app.collectors.push(Box::new(pgconflc));
                }

                if let Some(pgidx_c) = collectors::pg_indexes::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pgidx_c.clone()))?;
                    app.collectors.push(Box::new(pgidx_c));
                }

                if let Some(pgstmt_c) = collectors::pg_statements::new(Arc::clone(&arc_pgi)) {
                    app.registry.register(Box::new(pgstmt_c.clone()))?;
                    app.collectors.push(Box::new(pgstmt_c));
                }

                app.instances.push(arc_pgi);
            }

            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(app.clone()))
                    .service(hello)
                    .route(
                        &ec.config.endpoint.clone().unwrap_or_default(),
                        web::get().to(metrics),
                    )
            })
            .bind(ec.config.listen_addr.unwrap_or_default())?
            .run()
            .await?
        }

        Some(ref _command) => {}
    }

    info!("🐘 PgExporter shutting down");

    Ok(())
}

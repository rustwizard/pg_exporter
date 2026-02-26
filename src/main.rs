#![warn(clippy::unwrap_used)]
mod app;
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

use prometheus::Encoder;
use prometheus::core::Collector;
use tracing::{error, info};

use crate::app::PGEApp;
use crate::config::{ExporterConfig, Overrides};
use crate::error::MetricsError;
use pg_exporter::cli::{self, Commands};

fn register_collector<C>(
    app: &mut PGEApp,
    dbi: Arc<instance::PostgresDB>,
    new_fn: fn(Arc<instance::PostgresDB>) -> Option<C>,
) -> anyhow::Result<()>
where
    C: collectors::PG + Collector + Clone + 'static,
{
    if let Some(c) = new_fn(dbi) {
        let boxed: Box<dyn Collector> = Box::new(c.clone());
        app.registry.register(boxed)?;
        app.add_collector(Box::new(c));
    }
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = cli::Cli::parse();

    pg_exporter::logger_init();

    let mut overrides = Overrides::default();

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

    ec.config.overrides(overrides);

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
            .map(|v| v.to_str().unwrap_or("<invalid utf-8>"))
            .unwrap_or("<unknown>")
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
            let mut app = PGEApp::new();

            for (instance, config) in ec.config.instances.unwrap_or_default() {
                info!("starting connection for instance: {instance}");

                let pgi = match instance::new(&instance::Config {
                    dsn: config.dsn,
                    exclude_db_names: config.exclude_db_names.clone(),
                    const_labels: config.const_labels.clone(),
                    collect_top_query: config.collect_top_query,
                    collect_top_index: config.collect_top_index,
                    collect_top_table: config.collect_top_table,
                    no_track_mode: config.no_track_mode,
                })
                .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        error!("failed to initialize instance {instance}: {e}");
                        continue;
                    }
                };

                let arc_pgi = Arc::new(pgi);

                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_locks::new)?;
                register_collector(
                    &mut app,
                    Arc::clone(&arc_pgi),
                    collectors::pg_postmaster::new,
                )?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_database::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_activity::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_bgwirter::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_wal::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_stat_io::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_archiver::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_conflict::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_indexes::new)?;
                register_collector(
                    &mut app,
                    Arc::clone(&arc_pgi),
                    collectors::pg_statements::new,
                )?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_tables::new)?;
                register_collector(&mut app, Arc::clone(&arc_pgi), collectors::pg_storage::new)?;
                register_collector(
                    &mut app,
                    Arc::clone(&arc_pgi),
                    collectors::pg_replication::new,
                )?;
                register_collector(
                    &mut app,
                    Arc::clone(&arc_pgi),
                    collectors::pg_replication_slots::new,
                )?;

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

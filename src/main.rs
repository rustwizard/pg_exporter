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
use std::time::Duration;

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
    name: &str,
    new_fn: fn(Arc<instance::PostgresDB>) -> Option<C>,
) -> anyhow::Result<()>
where
    C: collectors::PG + Collector + Clone + 'static,
{
    if let Some(c) = new_fn(dbi) {
        let boxed: Box<dyn Collector> = Box::new(c.clone());
        app.registry.register(boxed)?;
        app.add_collector(name, Box::new(c));
    }
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = cli::Cli::parse();

    let log_level = ExporterConfig::load(Path::new(&args.config))
        .ok()
        .and_then(|c| c.config.log_level);
    pg_exporter::logger_init(log_level.as_deref());

    let mut overrides = Overrides::default();

    match args.command {
        Some(Commands::Configcheck) => {
            let ec = match ExporterConfig::load(Path::new(&args.config)) {
                Ok(ec) => ec,
                Err(e) => {
                    error!("{}", e);
                    exit(1);
                }
            };
            if let Err(e) = ec.config.validate() {
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

async fn health(data: web::Data<PGEApp>) -> HttpResponse {
    let all_up = data.instances.iter().all(|i| !i.db.is_closed());
    if all_up {
        HttpResponse::Ok()
            .insert_header(ContentType::json())
            .body(r#"{"status":"ok"}"#)
    } else {
        HttpResponse::ServiceUnavailable()
            .insert_header(ContentType::json())
            .body(r#"{"status":"degraded"}"#)
    }
}

async fn metrics(req: HttpRequest, data: web::Data<PGEApp>) -> Result<HttpResponse, MetricsError> {
    info!(
        "processing the request from {:?}",
        req.headers()
            .get("user-agent")
            .map(|v| v.to_str().unwrap_or("<invalid utf-8>"))
            .unwrap_or("<unknown>")
    );

    for instance in &data.instances {
        data.pool_size
            .with_label_values(&[&instance.name])
            .set(instance.db.size() as f64);
        data.pool_idle
            .with_label_values(&[&instance.name])
            .set(instance.db.num_idle() as f64);
    }

    let timeout = Duration::from_millis(data.scrape_timeout_ms);

    let tasks: Vec<_> = data
        .collectors
        .clone()
        .into_iter()
        .map(|(name, col)| {
            let duration = data.scrape_duration.with_label_values(&[name.as_str()]);
            let errors = data.scrape_errors.with_label_values(&[name.as_str()]);
            actix_web::rt::spawn(async move {
                let start = std::time::Instant::now();
                if let Err(err) = col.update().await {
                    errors.inc();
                    error!("collector {name} update failed: {err}");
                }
                duration.observe(start.elapsed().as_secs_f64());
            })
        })
        .collect();

    match actix_web::rt::time::timeout(timeout, async {
        for task in tasks {
            task.await?;
        }
        Ok::<(), MetricsError>(())
    })
    .await
    {
        Ok(result) => result?,
        Err(_elapsed) => {
            tracing::warn!(
                "scrape timeout ({} ms) exceeded, returning partial metrics",
                data.scrape_timeout_ms
            );
        }
    }

    let process_metrics = prometheus::gather();

    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let postgres_metrics = data.registry.gather();
    encoder.encode(&postgres_metrics, &mut buffer)?;
    encoder.encode(&process_metrics, &mut buffer)?;

    let response = String::from_utf8(buffer).map_err(anyhow::Error::from)?;

    let resp = HttpResponse::Ok()
        .insert_header(ContentType::plaintext())
        .body(response);

    Ok(resp)
}

async fn pgexporter(command: Option<Commands>, mut ec: ExporterConfig) -> anyhow::Result<()> {
    match command {
        None | Some(Commands::Run { .. }) => {
            let mut app = PGEApp::new()?;
            app.scrape_timeout_ms = ec
                .config
                .scrape_timeout_ms
                .unwrap_or(app::DEFAULT_SCRAPE_TIMEOUT_MS);

            for (instance, config) in ec.config.instances.take().unwrap_or_default() {
                info!("starting connection for instance: {instance}");

                let disabled: std::collections::HashSet<String> = config
                    .disable_collectors
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .cloned()
                    .collect();

                let pgi = match instance::new(&ec.config.merge_pool_defaults(config)).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("failed to initialize instance {instance}: {e}");
                        continue;
                    }
                };

                let mut pgi = pgi;
                pgi.name = instance.clone();
                let arc_pgi = Arc::new(pgi);

                macro_rules! reg {
                    ($name:literal, $new_fn:expr) => {
                        if !disabled.contains($name) {
                            register_collector(&mut app, Arc::clone(&arc_pgi), $name, $new_fn)?;
                        }
                    };
                }

                reg!("pg_locks", collectors::pg_locks::new);
                reg!("pg_postmaster", collectors::pg_postmaster::new);
                reg!("pg_database", collectors::pg_database::new);
                reg!("pg_activity", collectors::pg_activity::new);
                reg!("pg_bgwriter", collectors::pg_bgwriter::new);
                reg!("pg_wal", collectors::pg_wal::new);
                reg!("pg_stat_io", collectors::pg_stat_io::new);
                reg!("pg_stat_slru", collectors::pg_stat_slru::new);
                reg!("pg_archiver", collectors::pg_archiver::new);
                reg!("pg_conflict", collectors::pg_conflict::new);
                reg!("pg_indexes", collectors::pg_indexes::new);
                reg!("pg_statements", collectors::pg_statements::new);
                reg!("pg_tables", collectors::pg_tables::new);
                reg!("pg_storage", collectors::pg_storage::new);
                reg!("pg_replication", collectors::pg_replication::new);
                reg!("pg_replication_slots", collectors::pg_replication_slots::new);
                reg!("pg_settings", collectors::pg_settings::new);

                app.instances.push(arc_pgi);
            }

            // Give in-flight scrapes time to finish on SIGTERM/SIGINT.
            // We add 5 s on top of scrape_timeout_ms so the timeout handler
            // inside metrics() always fires before the server kills workers.
            let shutdown_timeout_secs = (app.scrape_timeout_ms / 1000) + 5;

            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(app.clone()))
                    .service(hello)
                    .route("/health", web::get().to(health))
                    .route(
                        &ec.config
                            .endpoint
                            .clone()
                            .unwrap_or_else(|| "/metrics".to_string()),
                        web::get().to(metrics),
                    )
            })
            .shutdown_timeout(shutdown_timeout_secs)
            .bind(
                ec.config
                    .listen_addr
                    .unwrap_or_else(|| "0.0.0.0:61488".to_string()),
            )?
            .run()
            .await?
        }

        Some(ref _command) => {}
    }

    info!("🐘 PgExporter shutting down");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{dev::ServiceResponse, test};

    macro_rules! health_app {
        ($instances:expr) => {{
            let mut app = PGEApp::new().expect("PGEApp::new failed");
            app.instances = $instances;
            test::init_service(
                App::new()
                    .app_data(web::Data::new(app))
                    .route("/health", web::get().to(health)),
            )
            .await
        }};
    }

    #[actix_web::test]
    async fn test_health_ok_no_instances() {
        let app = health_app!(vec![]);
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp: ServiceResponse = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body = test::read_body(resp).await;
        assert_eq!(body, r#"{"status":"ok"}"#);
    }

    #[actix_web::test]
    async fn test_health_ok_with_open_pool() {
        let pgi = instance::new(&instance::Config {
            dsn: "postgres://postgres:postgres@localhost:5432/postgres".to_string(),
            ..Default::default()
        })
        .await
        .expect("instance::new failed");
        let app = health_app!(vec![Arc::new(pgi)]);
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp: ServiceResponse = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let body = test::read_body(resp).await;
        assert_eq!(body, r#"{"status":"ok"}"#);
    }

    #[actix_web::test]
    async fn test_health_degraded_when_pool_closed() {
        let pgi = instance::new(&instance::Config {
            dsn: "postgres://postgres:postgres@localhost:5432/postgres".to_string(),
            ..Default::default()
        })
        .await
        .expect("instance::new failed");
        pgi.db.close().await;
        let app = health_app!(vec![Arc::new(pgi)]);
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp: ServiceResponse = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 503);
        let body = test::read_body(resp).await;
        assert_eq!(body, r#"{"status":"degraded"}"#);
    }
}

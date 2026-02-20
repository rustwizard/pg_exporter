use std::sync::Arc;

use pg_exporter::instance;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn setup_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Starts a PostgreSQL container and returns it together with a connected
/// instance. The container must be kept alive for the duration of the test —
/// dropping it stops the database.
pub async fn create_test_instance()
-> Result<(ContainerAsync<Postgres>, Arc<instance::PostgresDB>), Box<dyn std::error::Error>> {
    let container: ContainerAsync<Postgres> =
        Postgres::default().with_tag("latest").start().await?;
    let host_port = container.get_host_port_ipv4(5432).await?;
    let dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        host_port
    );
    let cfg = instance::Config {
        dsn,
        ..Default::default()
    };
    let pgi = instance::new(&cfg).await?;
    Ok((container, Arc::new(pgi)))
}

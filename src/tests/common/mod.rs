use std::sync::Arc;

use pg_exporter::instance;
use sqlx::postgres::PgPoolOptions;
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
    create_test_instance_with_exclusions(&[]).await
}

/// Creates a second database on an already-running container, then inserts
/// enough data to make the size visible via `pg_database_size()`.
pub async fn create_second_database(
    container: &ContainerAsync<Postgres>,
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let host_port = container.get_host_port_ipv4(5432).await?;
    let postgres_dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        host_port
    );

    // Connect to the default database to issue CREATE DATABASE (must be in autocommit).
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&postgres_dsn)
        .await?;
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await?;
    admin_pool.close().await;

    // Connect to the new database, create a table, and populate it.
    let new_db_dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/{}",
        host_port, db_name
    );
    let new_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&new_db_dsn)
        .await?;
    sqlx::query(
        "CREATE TABLE test_data (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)",
    )
    .execute(&new_pool)
    .await?;
    sqlx::query(
        "INSERT INTO test_data (payload) \
         SELECT repeat('x', 1000) FROM generate_series(1, 1000)",
    )
    .execute(&new_pool)
    .await?;
    new_pool.close().await;

    Ok(())
}

/// Like [`create_test_instance`] but pre-populates the excluded database list.
pub async fn create_test_instance_with_exclusions(
    excluded: &[String],
) -> Result<(ContainerAsync<Postgres>, Arc<instance::PostgresDB>), Box<dyn std::error::Error>> {
    let container: ContainerAsync<Postgres> =
        Postgres::default().with_tag("latest").start().await?;
    let host_port = container.get_host_port_ipv4(5432).await?;
    let dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        host_port
    );
    let cfg = instance::Config {
        dsn,
        exclude_db_names: if excluded.is_empty() {
            None
        } else {
            Some(excluded.to_vec())
        },
        ..Default::default()
    };
    let pgi = instance::new(&cfg).await?;
    Ok((container, Arc::new(pgi)))
}

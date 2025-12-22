use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod integration_tests {
    use std::sync::Arc;

    use pg_exporter::{
        collectors::{self, PG},
        instance,
    };
    use prometheus::{Encoder, Registry};
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{runners::AsyncRunner, *};
    use testcontainers_modules::postgres::Postgres;

    use crate::setup_tracing;

    #[tokio::test]
    async fn test_database_interaction() -> Result<(), Box<dyn std::error::Error>> {
        // 1. Start the PostgreSQL container
        let container = Postgres::default()
            .with_tag("latest") // Specify the image tag
            .start()
            .await?;

        // 2. Get the dynamic host port and construct the database URL
        let host_port = container.get_host_port_ipv4(5432).await?;
        let database_url = &format!(
            "postgresql://postgres:postgres@localhost:{}/postgres", // Default user/password for testcontainers postgres module
            host_port
        );

        // 3. Create a sqlx connection pool
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // 5. Perform your test logic using the `pool`
        let row: (i32,) = sqlx::query_as("SELECT 1 + 1").fetch_one(&pool).await?;

        assert_eq!(row.0, 2);

        Ok(())
        // The container is automatically stopped when the `container` variable goes out of scope (Drop trait)
    }

    #[tokio::test]
    async fn test_pg_activity_collector() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();

        // 1. Start the PostgreSQL container
        let container = Postgres::default()
            .with_tag("latest") // Specify the image tag
            .start()
            .await?;

        // 2. Get the dynamic host port and construct the database URL
        let host_port = container.get_host_port_ipv4(5432).await?;
        let dsn = &format!(
            "postgresql://postgres:postgres@localhost:{}/postgres", // Default user/password for testcontainers postgres module
            host_port
        );

        let cfg = instance::Config {
            dsn: dsn.to_string(),
            ..Default::default()
        };

        let pgi = instance::new(&cfg).await?;

        let registry = Registry::new();
        let mut collectors = Vec::new();

        if let Some(pac) = collectors::pg_activity::new(Arc::clone(&Arc::new(pgi))) {
            registry.register(Box::new(pac.clone()))?;
            collectors.push(Box::new(pac));
        }

        let tasks: Vec<_> = collectors
            .clone()
            .into_iter()
            .map(|col| {
                tokio::spawn(async move {
                    let update_result = col.update().await;
                    assert!(update_result.is_ok())
                })
            })
            .collect();

        for task in tasks {
            task.await?;
        }

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();

        assert_eq!(postgres_metrics.len(), 8);

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response =
            String::from_utf8(buffer.clone()).expect("Failed to convert bytes to string");

        assert!(!response.is_empty());

        Ok(())
    }
}

pub fn setup_tracing() {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than INFO (e.g, info, error, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder.
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}

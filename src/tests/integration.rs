mod common;

mod integration_tests {
    use pg_exporter::collectors::{self, PG};
    use prometheus::{Encoder, Registry};

    use crate::common;

    #[tokio::test]
    async fn test_pg_activity_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        let pac = collectors::pg_activity::new(pgi).expect("pg_activity collector should init");
        registry.register(Box::new(pac.clone()))?;

        pac.update().await?;

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_up"));
        assert!(metric_names.contains(&"pg_start_time_seconds"));
        assert!(metric_names.contains(&"pg_activity_connections_all_in_flight"));
        assert!(metric_names.contains(&"pg_activity_prepared_transactions_in_flight"));

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_locks_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        let pc_locks = collectors::pg_locks::new(pgi).expect("pg_locks collector should init");
        registry.register(Box::new(pc_locks.clone()))?;

        pc_locks.update().await?;

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_locks_total"));
        assert!(metric_names.contains(&"pg_locks_not_granted"));
        assert!(metric_names.contains(&"pg_locks_access_share_lock"));
        assert!(metric_names.contains(&"pg_locks_access_exclusive_lock"));
        assert!(metric_names.contains(&"pg_locks_exclusive_lock"));
        assert!(metric_names.contains(&"pg_locks_row_exclusive_lock"));
        assert!(metric_names.contains(&"pg_locks_row_share_lock"));
        assert!(metric_names.contains(&"pg_locks_share_lock"));
        assert!(metric_names.contains(&"pg_locks_share_row_exclusive_lock"));
        assert!(metric_names.contains(&"pg_locks_share_update_exclusive_lock"));

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }
}

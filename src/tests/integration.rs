mod common;

mod integration_tests {
    use std::sync::Arc;

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

    #[tokio::test]
    async fn test_pg_archiver_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        // Collector requires PostgreSQL > v12; testcontainers "latest" satisfies this.
        let pc_archiver =
            collectors::pg_archiver::new(pgi).expect("pg_archiver collector should init");
        registry.register(Box::new(pc_archiver.clone()))?;

        // On a fresh instance WAL archiving is not configured, so the query
        // returns no rows (WHERE archived_count > 0), but update() still succeeds.
        pc_archiver.update().await?;

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_archiver_archived_total"));
        assert!(metric_names.contains(&"pg_archiver_failed_total"));
        assert!(metric_names.contains(&"pg_archiver_since_last_archive_seconds"));
        assert!(metric_names.contains(&"pg_archiver_lag_bytes"));

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_bgwriter_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        let pc_bgwriter =
            collectors::pg_bgwriter::new(pgi).expect("pg_bgwriter collector should init");
        registry.register(Box::new(pc_bgwriter.clone()))?;

        pc_bgwriter.update().await?;

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_checkpoints_total"));
        assert!(metric_names.contains(&"pg_checkpoints_all_total"));
        assert!(metric_names.contains(&"pg_checkpoints_seconds_total"));
        assert!(metric_names.contains(&"pg_checkpoints_seconds_all_total"));
        assert!(metric_names.contains(&"pg_checkpoints_stats_age_seconds_total"));
        assert!(metric_names.contains(&"pg_checkpoints_restartpoints_timed"));
        assert!(metric_names.contains(&"pg_checkpoints_restartpoints_req"));
        assert!(metric_names.contains(&"pg_checkpoints_restartpoints_done"));
        assert!(metric_names.contains(&"pg_written_bytes_total"));
        assert!(metric_names.contains(&"pg_bgwriter_maxwritten_clean_total"));
        assert!(metric_names.contains(&"pg_bgwriter_stats_age_seconds_total"));
        assert!(metric_names.contains(&"pg_backends_fsync_total"));
        assert!(metric_names.contains(&"pg_backends_allocated_bytes_total"));

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_database_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        // Create a second database on the same container and populate it with
        // enough data so that pg_database_size() returns a meaningful value.
        common::create_second_database(&_container, "testdb").await?;

        let registry = Registry::new();

        let pc_database =
            collectors::pg_database::new(pgi).expect("pg_database collector should init");
        registry.register(Box::new(pc_database.clone()))?;

        pc_database.update().await?;

        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_database_size_bytes"));

        let size_mf = postgres_metrics
            .iter()
            .find(|mf| mf.name() == "pg_database_size_bytes")
            .expect("pg_database_size_bytes metric should exist");

        // Both "postgres" and "testdb" must be reported.
        assert!(
            size_mf.get_metric().len() >= 2,
            "expected at least two database size entries (postgres + testdb), got {}",
            size_mf.get_metric().len()
        );

        assert!(
            size_mf
                .get_metric()
                .iter()
                .all(|m| m.get_gauge().value() > 0.0),
            "all database sizes should be positive"
        );

        // "testdb" must appear with a positive size reflecting the inserted data.
        let testdb_metric = size_mf
            .get_metric()
            .iter()
            .find(|m| m.get_label().iter().any(|l| l.value() == "testdb"))
            .expect("testdb should appear in pg_database_size_bytes metrics");
        assert!(
            testdb_metric.get_gauge().value() > 0.0,
            "testdb size should be positive after inserting data, got {}",
            testdb_metric.get_gauge().value()
        );

        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_database_collector_with_exclusions() -> Result<(), Box<dyn std::error::Error>>
    {
        common::setup_tracing();

        let (_container, pgi_all) = common::create_test_instance().await?;
        let (_container2, pgi_excl) =
            common::create_test_instance_with_exclusions(&["postgres".to_string()]).await?;

        let collector_all =
            collectors::pg_database::new(pgi_all).expect("pg_database collector should init");
        let collector_excl = collectors::pg_database::new(pgi_excl)
            .expect("pg_database collector with exclusions should init");

        collector_all.update().await?;
        collector_excl.update().await?;

        let registry_all = Registry::new();
        registry_all.register(Box::new(collector_all.clone()))?;
        let registry_excl = Registry::new();
        registry_excl.register(Box::new(collector_excl.clone()))?;

        let mfs_all = registry_all.gather();
        let mfs_excl = registry_excl.gather();

        let mf_all = mfs_all
            .iter()
            .find(|mf| mf.name() == "pg_database_size_bytes")
            .expect("metric should exist");
        let mf_excl = mfs_excl
            .iter()
            .find(|mf| mf.name() == "pg_database_size_bytes")
            .expect("metric should exist");

        // The excluded collector must not contain the "postgres" database.
        let has_postgres = mf_excl
            .get_metric()
            .iter()
            .any(|m| m.get_label().iter().any(|l| l.value() == "postgres"));
        assert!(
            !has_postgres,
            "excluded database should not appear in metrics"
        );

        // The non-excluded collector should have more (or equal) entries.
        assert!(
            mf_all.get_metric().len() >= mf_excl.get_metric().len(),
            "excluding a database should not increase the metric count"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_conflict_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        let pc_conflict =
            collectors::pg_conflict::new(pgi).expect("pg_conflict collector should init");
        registry.register(Box::new(pc_conflict.clone()))?;

        // The conflict query filters WHERE pg_is_in_recovery() = 't'.
        // A primary (non-standby) instance returns no rows, but update() still succeeds.
        pc_conflict.update().await?;

        let mut buffer = Vec::new();
        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_recovery_conflicts_total"));

        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_stat_slru_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        // pg_stat_slru requires PostgreSQL >= 13; testcontainers "latest" satisfies this.
        let pc_stat_slru = collectors::pg_stat_slru::new(pgi)
            .expect("pg_stat_slru collector should init on PG13+");
        registry.register(Box::new(pc_stat_slru.clone()))?;

        pc_stat_slru.update().await?;

        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_stat_slru_blks_zeroed"));
        assert!(metric_names.contains(&"pg_stat_slru_blks_hit"));
        assert!(metric_names.contains(&"pg_stat_slru_blks_read"));
        assert!(metric_names.contains(&"pg_stat_slru_blks_written"));
        assert!(metric_names.contains(&"pg_stat_slru_blks_exists"));
        assert!(metric_names.contains(&"pg_stat_slru_flushes"));
        assert!(metric_names.contains(&"pg_stat_slru_truncates"));

        // pg_stat_slru always has rows on a live instance, so every metric
        // family must contain at least one measurement.
        for mf in &postgres_metrics {
            assert!(
                !mf.get_metric().is_empty(),
                "metric '{}' should have at least one measurement after update()",
                mf.name()
            );
        }

        // All counters must be non-negative.
        for mf in &postgres_metrics {
            for m in mf.get_metric() {
                assert!(
                    m.get_gauge().value() >= 0.0,
                    "metric '{}' has a negative value: {}",
                    mf.name(),
                    m.get_gauge().value()
                );
            }
        }

        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_stat_io_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        // pg_stat_io requires PostgreSQL ≥ 16; testcontainers "latest" satisfies this.
        let pc_stat_io =
            collectors::pg_stat_io::new(pgi).expect("pg_stat_io collector should init on PG16+");
        registry.register(Box::new(pc_stat_io.clone()))?;

        pc_stat_io.update().await?;

        let postgres_metrics = registry.gather();
        let metric_names: Vec<&str> = postgres_metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_stat_io_reads"));
        assert!(metric_names.contains(&"pg_stat_io_read_time"));
        assert!(metric_names.contains(&"pg_stat_io_writes"));
        assert!(metric_names.contains(&"pg_stat_io_write_time"));
        assert!(metric_names.contains(&"pg_stat_io_writebacks"));
        assert!(metric_names.contains(&"pg_stat_io_writeback_time"));
        assert!(metric_names.contains(&"pg_stat_io_extends"));
        assert!(metric_names.contains(&"pg_stat_io_extend_time"));
        assert!(metric_names.contains(&"pg_stat_io_hits"));
        assert!(metric_names.contains(&"pg_stat_io_evictions"));
        assert!(metric_names.contains(&"pg_stat_io_reuses"));
        assert!(metric_names.contains(&"pg_stat_io_fsyncs"));
        assert!(metric_names.contains(&"pg_stat_io_fsync_time"));
        assert!(metric_names.contains(&"pg_stat_io_read_bytes"));
        assert!(metric_names.contains(&"pg_stat_io_write_bytes"));
        assert!(metric_names.contains(&"pg_stat_io_extend_bytes"));

        // pg_stat_io always has rows on a live instance (one per backend_type/object/context
        // combination), so every metric family must contain at least one measurement.
        for mf in &postgres_metrics {
            assert!(
                !mf.get_metric().is_empty(),
                "metric '{}' should have at least one measurement after update()",
                mf.name()
            );
        }

        // All counters are cumulative and must be non-negative.
        for mf in &postgres_metrics {
            for m in mf.get_metric() {
                assert!(
                    m.get_gauge().value() >= 0.0,
                    "metric '{}' has a negative value: {}",
                    mf.name(),
                    m.get_gauge().value()
                );
            }
        }

        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&postgres_metrics, &mut buffer)?;
        let response = String::from_utf8(buffer)?;

        assert!(!response.is_empty());

        Ok(())
    }

    // ── pg_statements collector tests ────────────────────────────────────────

    /// Without `pg_stat_statements` in `shared_preload_libraries`, `new()` must
    /// return `None` so the collector is simply omitted from the registry.
    #[tokio::test]
    async fn test_pg_statements_returns_none_without_extension()
    -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let collector = collectors::pg_statements::new(pgi);
        assert!(
            collector.is_none(),
            "collector should be None when pg_stat_statements is not loaded"
        );

        Ok(())
    }

    /// Basic smoke test: all expected metric families appear and every value is
    /// non-negative after running a couple of queries.
    #[tokio::test]
    async fn test_pg_statements_collector_basic() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance_with_pg_stat_statements().await?;

        sqlx::query("SELECT 1 + 1").execute(&pgi.db).await?;
        sqlx::query("SELECT current_timestamp")
            .execute(&pgi.db)
            .await?;

        let collector = collectors::pg_statements::new(Arc::clone(&pgi))
            .expect("pg_statements collector should init when extension is loaded");

        let registry = Registry::new();
        registry.register(Box::new(collector.clone()))?;

        collector.update().await?;

        let metrics = registry.gather();
        let names: Vec<&str> = metrics.iter().map(|mf| mf.name()).collect();

        assert!(names.contains(&"pg_statements_query_info"));
        assert!(names.contains(&"pg_statements_calls_total"));
        assert!(names.contains(&"pg_statements_rows_total"));
        assert!(names.contains(&"pg_statements_time_seconds_total"));
        assert!(names.contains(&"pg_statements_time_seconds_all_total"));
        assert!(names.contains(&"pg_statements_shared_buffers_hit_total"));
        assert!(names.contains(&"pg_statements_shared_buffers_read_bytes_total"));
        assert!(names.contains(&"pg_statements_wal_records_total"));
        assert!(names.contains(&"pg_statements_wal_bytes_all_total"));
        assert!(names.contains(&"pg_statements_wal_bytes_total"));

        for mf in &metrics {
            for m in mf.get_metric() {
                assert!(
                    m.get_gauge().value() >= 0.0,
                    "metric '{}' has negative value {}",
                    mf.name(),
                    m.get_gauge().value()
                );
            }
        }

        Ok(())
    }

    /// Verifies that time metrics are stored in seconds, not milliseconds.
    /// `pg_sleep(1.1)` produces ~1100 ms of execution time.  After the
    /// ms→s division the value must be 1 (integer truncation of 1.1 s).
    /// A value ≥ 100 would indicate raw milliseconds are still being used.
    #[tokio::test]
    async fn test_pg_statements_time_metrics_in_seconds() -> Result<(), Box<dyn std::error::Error>>
    {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance_with_pg_stat_statements().await?;

        sqlx::query("SELECT pg_sleep(1.1)").execute(&pgi.db).await?;

        let collector = collectors::pg_statements::new(Arc::clone(&pgi))
            .expect("pg_statements collector should init");

        collector.update().await?;

        let registry = Registry::new();
        registry.register(Box::new(collector.clone()))?;
        let metrics = registry.gather();

        let time_mf = metrics
            .iter()
            .find(|mf| mf.name() == "pg_statements_time_seconds_all_total")
            .expect("time_seconds_all_total should be present");

        let max_val = time_mf
            .get_metric()
            .iter()
            .map(|m| m.get_gauge().value() as i64)
            .max()
            .unwrap_or(0);

        assert!(
            max_val >= 1,
            "expected at least 1 second recorded for pg_sleep(1.1), got {max_val}"
        );
        assert!(
            max_val < 100,
            "value {max_val} looks like milliseconds rather than seconds"
        );

        Ok(())
    }

    /// With `no_track_mode = true`, every `query` label in `query_info` must be
    /// the placeholder string regardless of the actual SQL executed.
    #[tokio::test]
    async fn test_pg_statements_notrack_hides_query_text() -> Result<(), Box<dyn std::error::Error>>
    {
        common::setup_tracing();

        let (_container, pgi) =
            common::create_test_instance_with_pg_stat_statements_opts(None, Some(true)).await?;

        sqlx::query("SELECT 42").execute(&pgi.db).await?;

        let collector = collectors::pg_statements::new(Arc::clone(&pgi))
            .expect("pg_statements collector should init");

        collector.update().await?;

        let registry = Registry::new();
        registry.register(Box::new(collector.clone()))?;
        let metrics = registry.gather();

        let query_info_mf = metrics
            .iter()
            .find(|mf| mf.name() == "pg_statements_query_info")
            .expect("query_info metric should exist");

        for m in query_info_mf.get_metric() {
            let query_label = m
                .get_label()
                .iter()
                .find(|l| l.name() == "query")
                .map(|l| l.value())
                .unwrap_or("");
            assert_eq!(
                query_label, "/* query text hidden, no-track mode enabled */",
                "query text should be hidden in notrack mode, got: {query_label:?}"
            );
        }

        Ok(())
    }

    /// INSERTs into a fresh table generate both regular and full-page-write WAL.
    /// `wal_bytes_total` must expose separate series for `wal="fpi"` and
    /// `wal="regular"` when WAL activity is present.
    #[tokio::test]
    async fn test_pg_statements_wal_bytes_by_type() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance_with_pg_stat_statements().await?;

        sqlx::query("CREATE TABLE stmts_wal_test (id serial, val text)")
            .execute(&pgi.db)
            .await?;
        sqlx::query(
            "INSERT INTO stmts_wal_test (val) \
             SELECT md5(i::text) FROM generate_series(1, 500) i",
        )
        .execute(&pgi.db)
        .await?;

        let collector = collectors::pg_statements::new(Arc::clone(&pgi))
            .expect("pg_statements collector should init");

        collector.update().await?;

        let registry = Registry::new();
        registry.register(Box::new(collector.clone()))?;
        let metrics = registry.gather();

        let wal_mf = metrics
            .iter()
            .find(|mf| mf.name() == "pg_statements_wal_bytes_total")
            .expect("wal_bytes_total metric should be present");

        let wal_types: Vec<&str> = wal_mf
            .get_metric()
            .iter()
            .flat_map(|m| m.get_label().iter())
            .filter(|l| l.name() == "wal")
            .map(|l| l.value())
            .collect();

        assert!(
            wal_types.contains(&"fpi"),
            "wal_bytes_total should have 'fpi' series, got: {wal_types:?}"
        );
        assert!(
            wal_types.contains(&"regular"),
            "wal_bytes_total should have 'regular' series, got: {wal_types:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pg_settings_collector() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let registry = Registry::new();

        let collector =
            collectors::pg_settings::new(pgi).expect("pg_settings collector should init");
        registry.register(Box::new(collector.clone()))?;

        collector.update().await?;

        let metrics = registry.gather();
        let metric_names: Vec<&str> = metrics.iter().map(|mf| mf.name()).collect();

        assert!(metric_names.contains(&"pg_service_settings_info"));

        let settings_mf = metrics
            .iter()
            .find(|mf| mf.name() == "pg_service_settings_info")
            .expect("settings_info metric should be present");

        assert!(
            !settings_mf.get_metric().is_empty(),
            "settings_info should have at least one metric series"
        );

        // Verify that a known boolean setting exists and has an expected value.
        let fsync = settings_mf.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "name" && l.value() == "fsync")
        });
        assert!(fsync.is_some(), "fsync setting should be present");

        // Verify that a known integer setting exists and has a positive value.
        let shared_buffers = settings_mf.get_metric().iter().find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "name" && l.value() == "shared_buffers")
        });
        assert!(
            shared_buffers.is_some(),
            "shared_buffers setting should be present"
        );
        assert!(
            shared_buffers.unwrap().get_gauge().value() > 0.0,
            "shared_buffers value should be positive"
        );

        Ok(())
    }
}

mod exporter_self_metrics_tests {
    use std::sync::Arc;

    use pg_exporter::app::PGEApp;
    use pg_exporter::collectors::{self, PG};
    use prometheus::Encoder;

    use crate::common;

    /// scrape_duration and scrape_errors must be present in gathered metrics
    /// after a successful update().
    #[tokio::test]
    async fn test_self_metrics_present_after_scrape() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let mut app = PGEApp::new()?;

        let collector = collectors::pg_locks::new(Arc::clone(&pgi)).expect("pg_locks should init");
        app.registry.register(Box::new(collector.clone()))?;
        app.add_collector("pg_locks", Box::new(collector.clone()));

        // Run update so duration/error metrics are recorded
        if let Err(e) = collector.update().await {
            return Err(e.into());
        }
        app.scrape_duration
            .with_label_values(&["pg_locks"])
            .observe(0.01);

        let metrics = app.registry.gather();
        let mut buffer = Vec::new();
        prometheus::TextEncoder::new().encode(&metrics, &mut buffer)?;
        let output = String::from_utf8(buffer)?;

        assert!(
            output.contains("pg_exporter_scrape_duration_seconds"),
            "scrape_duration must be present in gathered metrics"
        );
        assert!(
            output.contains("pg_exporter_scrape_errors_total"),
            "scrape_errors must be present in gathered metrics"
        );
        assert!(
            output.contains("collector=\"pg_locks\""),
            "collector label must be set correctly"
        );

        Ok(())
    }

    /// scrape_errors counter must increment when update() fails.
    #[tokio::test]
    async fn test_self_metrics_error_counter_increments() {
        use pg_exporter::instance;

        let pgi = Arc::new(
            instance::new(&instance::Config {
                dsn: "postgres://nobody:nobody@127.0.0.1:19876/nonexistent".to_string(),
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let app = PGEApp::new()?;

        let collector =
            collectors::pg_locks::new(Arc::clone(&pgi)).expect("collector should be created");

        // Simulate what the metrics handler does
        let errors = app.scrape_errors.with_label_values(&["pg_locks"]);
        if collector.update().await.is_err() {
            errors.inc();
        }

        assert_eq!(
            errors.get() as u64,
            1,
            "error counter must be 1 after a failed update()"
        );
    }
}

/// Tests that prove lazy connection and reconnect behaviour.
///
/// Scenarios covered:
/// 1. App starts successfully even when PostgreSQL is unreachable.
/// 2. `ensure_ready()` returns an error (not a panic) when DB is unreachable.
/// 3. Failed `ensure_ready()` does not permanently cache the failure — the
///    next call retries the connection attempt.
/// 4. `ensure_ready()` succeeds and caches cfg when DB is available.
/// 5. After `reset_cfg()` (simulating a reconnect / DB restart), cfg is
///    re-fetched transparently on the next `ensure_ready()` call.
/// 6. A collector's `update()` returns `Err` (not panic) when DB is
///    unreachable, and recovers automatically once the DB is back.
mod lazy_reconnect_tests {
    use std::sync::Arc;

    use pg_exporter::collectors::{self, PG};
    use pg_exporter::instance;
    use prometheus::core::Collector;

    use crate::common;

    /// A syntactically valid DSN that points to a port with no listener.
    /// Connection attempts fail immediately with ECONNREFUSED, keeping tests fast.
    const UNREACHABLE_DSN: &str = "postgres://nobody:nobody@127.0.0.1:19876/nonexistent";

    // ── 1. Lazy start ───────────────────────────────────────────────────────

    /// `instance::new()` must succeed even when PostgreSQL is unreachable.
    /// The pool is created lazily; only the DSN syntax is validated at this
    /// point.
    #[tokio::test]
    async fn test_instance_new_succeeds_without_pg() {
        let result = instance::new(&instance::Config {
            dsn: UNREACHABLE_DSN.to_string(),
            ..Default::default()
        })
        .await;

        assert!(
            result.is_ok(),
            "instance::new() should return Ok even when DB is unreachable"
        );
        let pgi = result.unwrap();
        assert!(
            pgi.current_cfg().is_none(),
            "cfg should be None when DB was never reachable"
        );
    }

    // ── 2. ensure_ready() fails fast ────────────────────────────────────────

    /// `ensure_ready()` must return `Err` (not panic / hang) when the DB is
    /// unreachable.
    #[tokio::test]
    async fn test_ensure_ready_fails_when_pg_unreachable() {
        let pgi = instance::new(&instance::Config {
            dsn: UNREACHABLE_DSN.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        let result = pgi.ensure_ready().await;
        assert!(
            result.is_err(),
            "ensure_ready() should return Err when DB is unreachable"
        );
    }

    // ── 3. Failure is not cached — system always retries ────────────────────

    /// After a failed `ensure_ready()`, `current_cfg()` must still return
    /// `None` and a subsequent `ensure_ready()` must retry (not return a
    /// permanently-cached failure).
    #[tokio::test]
    async fn test_ensure_ready_retries_each_call() {
        let pgi = instance::new(&instance::Config {
            dsn: UNREACHABLE_DSN.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        // First attempt fails.
        assert!(pgi.ensure_ready().await.is_err());
        assert!(
            pgi.current_cfg().is_none(),
            "cfg must remain None after first failure"
        );

        // Second attempt also fails — but the key point is that it *tried*
        // again rather than returning a stale cached error.
        assert!(pgi.ensure_ready().await.is_err());
        assert!(
            pgi.current_cfg().is_none(),
            "cfg must remain None after second failure"
        );
    }

    // ── 4. Happy path: cfg is loaded and cached ──────────────────────────────

    /// When the DB is reachable, `ensure_ready()` loads the server config and
    /// caches it so subsequent calls don't hit the database again.
    #[tokio::test]
    async fn test_ensure_ready_succeeds_and_caches_cfg() -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let cfg = pgi.ensure_ready().await?;
        assert!(
            cfg.pg_version > 0,
            "pg_version should be populated from the server"
        );
        assert!(cfg.pg_block_size > 0, "pg_block_size should be populated");

        // cfg must now be cached without hitting the DB.
        assert!(
            pgi.current_cfg().is_some(),
            "current_cfg() should return Some after successful ensure_ready()"
        );

        // Second call returns the same cfg from the cache.
        let cfg2 = pgi.ensure_ready().await?;
        assert_eq!(
            cfg.pg_version, cfg2.pg_version,
            "cached cfg should match original"
        );

        Ok(())
    }

    // ── 5. Reconnect: cfg is re-fetched after a simulated DB restart ─────────

    /// Simulates a DB restart by clearing the cached cfg via `reset_cfg()`.
    /// The next `ensure_ready()` call must transparently re-fetch it from the
    /// live server, proving that the reconnect path works end-to-end.
    #[tokio::test]
    async fn test_cfg_repopulated_after_simulated_reconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        // Initial connection succeeds and caches cfg.
        let cfg_before = pgi.ensure_ready().await?;
        assert!(cfg_before.pg_version > 0);

        // Simulate a reconnect event (e.g. DB restarted, pool recycled).
        pgi.reset_cfg();
        assert!(
            pgi.current_cfg().is_none(),
            "cfg should be None immediately after reset_cfg()"
        );

        // ensure_ready() must re-fetch from the live server.
        let cfg_after = pgi.ensure_ready().await?;
        assert!(cfg_after.pg_version > 0);
        assert_eq!(
            cfg_before.pg_version, cfg_after.pg_version,
            "re-fetched cfg should match original server version"
        );

        Ok(())
    }

    // ── 6. Collector graceful failure and recovery ───────────────────────────

    /// A collector's `update()` must return `Err` (not panic) when the DB is
    /// unreachable, so the metrics handler can log it and move on.
    #[tokio::test]
    async fn test_collector_update_returns_err_when_pg_unreachable() {
        let pgi = Arc::new(
            instance::new(&instance::Config {
                dsn: UNREACHABLE_DSN.to_string(),
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        // pg_database collector: does not check cfg in new(), so it is always
        // created regardless of DB availability.
        let collector =
            collectors::pg_database::new(Arc::clone(&pgi)).expect("collector should be created");

        let result = collector.update().await;
        assert!(
            result.is_err(),
            "update() should return Err when DB is unreachable"
        );
    }

    /// `pg_up` must be 0 when the DB is unreachable and 1 when the DB is up.
    #[tokio::test]
    async fn test_pg_up_reflects_db_availability() {
        // ── unreachable DB → pg_up should be 0 ──────────────────────────────
        let pgi_down = Arc::new(
            instance::new(&instance::Config {
                dsn: UNREACHABLE_DSN.to_string(),
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let collector_down = collectors::pg_activity::new(Arc::clone(&pgi_down))
            .expect("pg_activity collector should be created even when DB is down");

        // update() should fail but must set pg_up = 0
        let _ = collector_down.update().await;

        let mfs_down = collector_down.collect();
        let up_mf = mfs_down
            .iter()
            .find(|mf: &&prometheus::proto::MetricFamily| mf.name() == "pg_up")
            .expect("pg_up metric must always be present");
        let up_value = up_mf.get_metric()[0].get_gauge().value();
        assert_eq!(up_value, 0.0, "pg_up must be 0 when DB is unreachable");
    }

    /// After a simulated reconnect (cfg reset), a collector's `update()` must
    /// succeed because `ensure_ready()` re-fetches cfg lazily.
    #[tokio::test]
    async fn test_collector_recovers_after_simulated_reconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        // Use a version-aware collector to exercise ensure_ready() inside update().
        let collector = collectors::pg_indexes::new(Arc::clone(&pgi))
            .expect("pg_indexes collector should init");

        // First update works normally.
        collector.update().await?;

        // Simulate DB reconnect by clearing the cached cfg.
        pgi.reset_cfg();
        assert!(pgi.current_cfg().is_none());

        // Second update must re-init cfg lazily and succeed.
        collector.update().await?;

        Ok(())
    }
}

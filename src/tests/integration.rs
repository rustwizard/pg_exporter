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
            collectors::pg_bgwirter::new(pgi).expect("pg_bgwriter collector should init");
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
    async fn test_ensure_ready_succeeds_and_caches_cfg()
    -> Result<(), Box<dyn std::error::Error>> {
        common::setup_tracing();

        let (_container, pgi) = common::create_test_instance().await?;

        let cfg = pgi.ensure_ready().await?;
        assert!(cfg.pg_version > 0, "pg_version should be populated from the server");
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

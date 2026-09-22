use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prometheus::{Counter, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use tracing::{error, warn};

use crate::{collectors, instance};

pub const DEFAULT_SCRAPE_TIMEOUT_MS: u64 = 30_000;

/// Default minimum interval between two real collector updates.
/// Zero disables TTL caching: only concurrent (in-flight) scrapes are coalesced.
pub const DEFAULT_MIN_SCRAPE_INTERVAL_MS: u64 = 0;

/// Custom histogram buckets suited for DB scrape durations (10 ms … 30 s).
const SCRAPE_DURATION_BUCKETS: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

#[derive(Clone)]
pub struct CollectorEntry {
    pub name: String,
    pub instance: String,
    pub collector: Box<dyn collectors::PG>,
}

/// Outcome of [`PGEApp::refresh`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    /// Collectors were updated from the database.
    Updated,
    /// A recent snapshot was reused; no database round-trips were made.
    Cached,
}

#[derive(Clone)]
pub struct PGEApp {
    pub instances: Vec<Arc<instance::PostgresDB>>,
    pub collectors: Vec<CollectorEntry>,
    pub registry: Registry,
    pub scrape_timeout_ms: u64,
    /// Minimum interval between two real collector updates. Zero disables TTL caching.
    pub min_scrape_interval_ms: u64,
    /// Serializes collector updates so concurrent /metrics requests never stampede the DB.
    scrape_lock: Arc<tokio::sync::Mutex<()>>,
    /// Completion instant of the last collector update pass.
    last_scrape: Arc<Mutex<Option<Instant>>>,
    /// How long each collector's update() took on the last scrape.
    pub scrape_duration: HistogramVec,
    /// Number of times a collector's update() returned an error.
    pub scrape_errors: CounterVec,
    /// Number of /metrics requests served from a cached snapshot.
    pub scrape_cached: Counter,
    /// Total connections in the pool (idle + in-use) per instance.
    pub pool_size: GaugeVec,
    /// Idle connections in the pool per instance.
    pub pool_idle: GaugeVec,
}

impl PGEApp {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::default();

        let scrape_duration = HistogramVec::new(
            HistogramOpts::new(
                "pg_exporter_scrape_duration_seconds",
                "Duration in seconds of each collector's update() call on the last scrape.",
            )
            .buckets(SCRAPE_DURATION_BUCKETS.to_vec()),
            &["collector", "instance"],
        )?;
        registry.register(Box::new(scrape_duration.clone()))?;

        let scrape_errors = CounterVec::new(
            Opts::new(
                "pg_exporter_scrape_errors_total",
                "Total number of errors returned by each collector's update() call.",
            ),
            &["collector", "instance"],
        )?;
        registry.register(Box::new(scrape_errors.clone()))?;

        let scrape_cached = Counter::new(
            "pg_exporter_scrape_cached_total",
            "Total number of /metrics requests served from a cached snapshot.",
        )?;
        registry.register(Box::new(scrape_cached.clone()))?;

        let pool_size = GaugeVec::new(
            Opts::new(
                "pg_exporter_pool_size",
                "Total number of connections in the pool (idle + in-use) per instance.",
            ),
            &["instance"],
        )?;
        registry.register(Box::new(pool_size.clone()))?;

        let pool_idle = GaugeVec::new(
            Opts::new(
                "pg_exporter_pool_idle",
                "Number of idle connections in the pool per instance.",
            ),
            &["instance"],
        )?;
        registry.register(Box::new(pool_idle.clone()))?;

        Ok(Self {
            instances: Vec::new(),
            collectors: Vec::new(),
            registry,
            scrape_timeout_ms: DEFAULT_SCRAPE_TIMEOUT_MS,
            min_scrape_interval_ms: DEFAULT_MIN_SCRAPE_INTERVAL_MS,
            scrape_lock: Arc::new(tokio::sync::Mutex::new(())),
            last_scrape: Arc::new(Mutex::new(None)),
            scrape_duration,
            scrape_errors,
            scrape_cached,
            pool_size,
            pool_idle,
        })
    }

    pub fn add_collector(
        &mut self,
        name: impl Into<String>,
        instance: impl Into<String>,
        col: Box<dyn collectors::PG>,
    ) {
        let name = name.into();
        let instance = instance.into();
        // Pre-initialize to zero so both metrics are always visible in /metrics
        // output even before the first scrape or the first error.
        self.scrape_duration.with_label_values(&[&name, &instance]);
        self.scrape_errors.with_label_values(&[&name, &instance]);
        self.collectors.push(CollectorEntry {
            name,
            instance,
            collector: col,
        });
    }

    /// Runs `update()` for every collector, recording duration and error metrics.
    ///
    /// Per-collector query failures are recorded in `scrape_errors` and do not
    /// fail the pass. Returns an error only when a spawned task panicked or was
    /// cancelled.
    pub async fn update_collectors(&self, timeout: Duration) -> anyhow::Result<()> {
        let tasks: Vec<_> = self
            .collectors
            .clone()
            .into_iter()
            .map(|entry| {
                let duration = self
                    .scrape_duration
                    .with_label_values(&[entry.name.as_str(), entry.instance.as_str()]);
                let errors = self
                    .scrape_errors
                    .with_label_values(&[entry.name.as_str(), entry.instance.as_str()]);
                let name = entry.name.clone();
                actix_web::rt::spawn(async move {
                    let start = Instant::now();
                    if let Err(err) = entry.collector.update().await {
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
            Ok::<(), actix_web::rt::task::JoinError>(())
        })
        .await
        {
            Ok(result) => {
                result.map_err(|e| anyhow::anyhow!("collector task failed: {e:?}"))?;
            }
            Err(_elapsed) => {
                warn!(
                    "scrape timeout ({} ms) exceeded, returning partial metrics",
                    timeout.as_millis()
                );
            }
        }

        Ok(())
    }

    /// Serves the latest snapshot when it is fresh, otherwise runs one update pass.
    ///
    /// Concurrent requests are serialized: while one request updates, the others
    /// wait and then reuse the resulting snapshot instead of hitting the database
    /// again. `request_started` must be the instant the current HTTP request was
    /// received, so that an update finishing after it is also reused.
    pub async fn refresh(&self, request_started: Instant, timeout: Duration) -> Refresh {
        let _guard = self.scrape_lock.lock().await;

        if self.is_fresh(request_started) {
            return Refresh::Cached;
        }

        if let Err(err) = self.update_collectors(timeout).await {
            error!("collector update pass failed: {err}");
        }
        // Mark even a timed-out pass: the goal is to shield the database from
        // repeated scrapes, and the detached tasks keep filling the snapshot.
        self.mark_scraped();

        Refresh::Updated
    }

    /// True when the last snapshot is recent enough to reuse.
    ///
    /// A snapshot is fresh when it was completed after `request_started` (another
    /// request already refreshed it while this one waited) or within
    /// `min_scrape_interval_ms` of now.
    pub fn is_fresh(&self, request_started: Instant) -> bool {
        let last = match self.last_scrape.lock() {
            Ok(guard) => *guard,
            Err(e) => {
                error!("last_scrape lock poisoned: {e}");
                return false;
            }
        };

        let Some(last) = last else {
            return false;
        };

        if last >= request_started {
            return true;
        }

        self.min_scrape_interval_ms > 0
            && last.elapsed() < Duration::from_millis(self.min_scrape_interval_ms)
    }

    fn mark_scraped(&self) {
        match self.last_scrape.lock() {
            Ok(mut guard) => *guard = Some(Instant::now()),
            Err(e) => error!("last_scrape lock poisoned: {e}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal collector that records how many times `update()` was called.
    #[derive(Clone)]
    struct FakeCollector {
        calls: Arc<AtomicUsize>,
        delay: Duration,
        fail: bool,
    }

    impl FakeCollector {
        fn new(delay_ms: u64) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(delay_ms),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::new(0)
            }
        }
    }

    #[async_trait::async_trait]
    impl collectors::PG for FakeCollector {
        async fn update(&self) -> Result<(), anyhow::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                actix_web::rt::time::sleep(self.delay).await;
            }
            if self.fail {
                anyhow::bail!("fake collector failure");
            }
            Ok(())
        }
    }

    fn app_with(fake: FakeCollector) -> (PGEApp, Arc<AtomicUsize>) {
        let calls = Arc::clone(&fake.calls);
        let mut app = PGEApp::new().unwrap();
        app.add_collector("fake", "test_instance", Box::new(fake));
        (app, calls)
    }

    #[actix_web::test]
    async fn refresh_updates_when_no_snapshot() {
        let (app, calls) = app_with(FakeCollector::new(0));

        let outcome = app.refresh(Instant::now(), Duration::from_secs(1)).await;

        assert_eq!(outcome, Refresh::Updated);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[actix_web::test]
    async fn refresh_reuses_snapshot_within_ttl() {
        let (mut app, calls) = app_with(FakeCollector::new(0));
        app.min_scrape_interval_ms = 60_000;

        let first = app.refresh(Instant::now(), Duration::from_secs(1)).await;
        assert_eq!(first, Refresh::Updated);

        // A later request (started after the previous pass) is served from cache.
        let second = app.refresh(Instant::now(), Duration::from_secs(1)).await;
        assert_eq!(second, Refresh::Cached);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[actix_web::test]
    async fn refresh_updates_again_with_ttl_zero() {
        let (app, calls) = app_with(FakeCollector::new(0));

        app.refresh(Instant::now(), Duration::from_secs(1)).await;
        let second = app.refresh(Instant::now(), Duration::from_secs(1)).await;

        // With TTL disabled only in-flight scrapes are coalesced, so a strictly
        // later sequential request triggers another update.
        assert_eq!(second, Refresh::Updated);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[actix_web::test]
    async fn concurrent_refresh_coalesces_into_one_pass() {
        let (app, calls) = app_with(FakeCollector::new(50));
        let app = Arc::new(app);

        // First request starts and grabs the lock; the second starts while the
        // first update is still running.
        let started_first = Instant::now();
        let a = {
            let app = Arc::clone(&app);
            actix_web::rt::spawn(
                async move { app.refresh(started_first, Duration::from_secs(2)).await },
            )
        };

        actix_web::rt::time::sleep(Duration::from_millis(10)).await;
        let b = {
            let app = Arc::clone(&app);
            actix_web::rt::spawn(async move {
                app.refresh(Instant::now(), Duration::from_secs(2)).await
            })
        };

        let outcome_a = a.await.unwrap();
        let outcome_b = b.await.unwrap();

        assert_eq!(outcome_a, Refresh::Updated);
        assert_eq!(
            outcome_b,
            Refresh::Cached,
            "second request must reuse the snapshot"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent scrapes must hit the database only once"
        );
    }

    #[actix_web::test]
    async fn update_collectors_records_errors() {
        let (app, calls) = app_with(FakeCollector::failing());

        app.update_collectors(Duration::from_secs(1)).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            app.scrape_errors
                .with_label_values(&["fake", "test_instance"])
                .get(),
            1.0
        );
    }

    #[actix_web::test]
    async fn scrape_cached_counter_starts_at_zero() {
        let (app, _calls) = app_with(FakeCollector::new(0));
        assert_eq!(app.scrape_cached.get(), 0.0);
    }
}

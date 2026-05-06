use std::sync::Arc;

use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts, Registry};

use crate::{collectors, instance};

pub const DEFAULT_SCRAPE_TIMEOUT_MS: u64 = 30_000;

/// Custom histogram buckets suited for DB scrape durations (10 ms … 30 s).
const SCRAPE_DURATION_BUCKETS: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

#[derive(Clone)]
pub struct PGEApp {
    pub instances: Vec<Arc<instance::PostgresDB>>,
    /// Collectors paired with their short names (e.g. "pg_locks") used as
    /// labels on the self-monitoring metrics.
    pub collectors: Vec<(String, Box<dyn collectors::PG>)>,
    pub registry: Registry,
    pub scrape_timeout_ms: u64,
    /// How long each collector's update() took on the last scrape.
    pub scrape_duration: HistogramVec,
    /// Number of times a collector's update() returned an error.
    pub scrape_errors: CounterVec,
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
            &["collector"],
        )?;
        registry.register(Box::new(scrape_duration.clone()))?;

        let scrape_errors = CounterVec::new(
            Opts::new(
                "pg_exporter_scrape_errors_total",
                "Total number of errors returned by each collector's update() call.",
            ),
            &["collector"],
        )?;
        registry.register(Box::new(scrape_errors.clone()))?;

        Ok(Self {
            instances: Vec::new(),
            collectors: Vec::new(),
            registry,
            scrape_timeout_ms: DEFAULT_SCRAPE_TIMEOUT_MS,
            scrape_duration,
            scrape_errors,
        })
    }

    pub fn add_collector(&mut self, name: impl Into<String>, col: Box<dyn collectors::PG>) {
        let name = name.into();
        // Pre-initialize to zero so both metrics are always visible in /metrics
        // output even before the first scrape or the first error.
        self.scrape_duration.with_label_values(&[&name]);
        self.scrape_errors.with_label_values(&[&name]);
        self.collectors.push((name, col));
    }
}

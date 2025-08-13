use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Gauge, GaugeVec, IntCounter, IntGauge, IntGaugeVec};

use crate::collectors::POSTGRES_V12;
use crate::instance;

const POSTGRES_WAL_ARCHIVING_QUERY: &str = "SELECT archived_count, failed_count, 
	EXTRACT(EPOCH FROM now() - last_archived_time) AS since_last_archive_seconds, 
	(SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name ~'.ready') AS lag_files 
	FROM pg_stat_archiver WHERE archived_count > 0";

#[derive(sqlx::FromRow, Debug)]
pub struct PGArchiverStats {
    archived: f64,
    failed: f64,
    since_archived_secinds: f64,
    lag_files: f64,
}

impl PGArchiverStats {
    fn new() -> Self {
        Self {
            archived: 0.0,
            failed: 0.0,
            since_archived_secinds: 0.0,
            lag_files: 0.0,
        }
    }
}

pub struct PGArchiverCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGArchiverStats>>,
    descs: Vec<Desc>,
    archived_total: IntCounter,
    failed_total: Gauge,
    since_last_archive_seconds: Gauge,
    lag_bytes: Gauge,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGArchiverCollector> {
    // some system functions are not available, required Postgres 12 or newer
    if dbi.cfg.pg_version < POSTGRES_V12 {
        Some(PGArchiverCollector::new(dbi))
    } else {
        None
    }
}

impl PGArchiverCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> Self {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(PGArchiverStats::new()));

        let archived_total = IntCounter::with_opts(
            Opts::new(
                "archived_total",
                "Total number of WAL segments had been successfully archived.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("archiver")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(archived_total.desc().into_iter().cloned());

        Self {
            dbi,
            data,
            descs,
            archived_total,
            failed_total: todo!(),
            since_last_archive_seconds: todo!(),
            lag_bytes: todo!(),
        }
    }
}

impl Collector for PGArchiverCollector {
    fn desc(&self) -> std::vec::Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> std::vec::Vec<MetricFamily> {
        todo!()
    }
}

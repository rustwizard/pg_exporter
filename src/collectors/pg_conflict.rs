use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;
use prometheus::{Gauge, IntCounter, IntCounterVec, IntGauge};

use crate::collectors::{PG, POSTGRES_V15};
use crate::instance;

const POSTGRES_DATABASE_CONFLICT15: &str = "SELECT datname AS database, 
    confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock
    FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'";

const POSTGRES_DATABASE_CONFLICT_LATEST: &str = "SELECT datname AS database, 
confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock, confl_active_logicalslot
		FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGConflictStats {
    database: String,
    #[sqlx(rename = "confl_tablespace")]
    tablespace: i64,
    #[sqlx(rename = "confl_lock")]
    lock: i64,
    #[sqlx(rename = "confl_snapshot")]
    snapshot: i64,
    #[sqlx(rename = "confl_bufferpin")]
    bufferpin: i64,
    #[sqlx(rename = "confl_deadlock")]
    deadlock: i64,
    #[sqlx(default)]
    active_logical_slot: i64,
}

impl PGConflictStats {
    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone)]
pub struct PGConflictCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGConflictStats>>,
    descs: Vec<Desc>,
    conflicts_total: IntCounterVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> PGConflictCollector {
    PGConflictCollector::new(dbi)
}

impl PGConflictCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> PGConflictCollector {
        let conflicts_total = IntCounterVec::new(
            Opts::new(
                "conflicts_total",
                "Total number of recovery conflicts occurred by each conflict type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("recovery")
            .const_labels(dbi.labels.clone()),
            &["database", "conflict"],
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(conflicts_total.desc().into_iter().cloned());

        PGConflictCollector {
            dbi,
            data: Arc::new(RwLock::new(PGConflictStats::new())),
            descs,
            conflicts_total,
        }
    }
}

impl Collector for PGConflictCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(1);

        let data_lock = self.data.read().unwrap();
        self.conflicts_total
            .with_label_values(&[&data_lock.database, "tablespace"])
            .inc_by(data_lock.tablespace as u64);

        mfs.extend(self.conflicts_total.collect());
        mfs
    }
}

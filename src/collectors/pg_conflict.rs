use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::IntCounterVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;

use crate::collectors::{PG, POSTGRES_V16};
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
    #[sqlx(default, rename = "confl_active_logicalslot")]
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

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGConflictCollector> {
    match PGConflictCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            eprintln!("error when create pg conflicts collector: {}", e);
            None
        }
    }
}

impl PGConflictCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGConflictCollector> {
        let conflicts_total = IntCounterVec::new(
            Opts::new(
                "conflicts_total",
                "Total number of recovery conflicts occurred by each conflict type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("recovery")
            .const_labels(dbi.labels.clone()),
            &["database", "conflict"],
        )?;

        let mut descs = Vec::new();
        descs.extend(conflicts_total.desc().into_iter().cloned());

        Ok(PGConflictCollector {
            dbi,
            data: Arc::new(RwLock::new(PGConflictStats::new())),
            descs,
            conflicts_total,
        })
    }
}

impl Collector for PGConflictCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(1);

        let data_lock = self
            .data
            .read()
            .expect("pg conflicts collector: should aquire lock for read");
        let database = data_lock.database.as_str();
        self.conflicts_total
            .with_label_values(&[database, "tablespace"])
            .inc_by(data_lock.tablespace as u64);
        self.conflicts_total
            .with_label_values(&[database, "lock"])
            .inc_by(data_lock.lock as u64);
        self.conflicts_total
            .with_label_values(&[database, "snapshot"])
            .inc_by(data_lock.snapshot as u64);
        self.conflicts_total
            .with_label_values(&[database, "bufferpin"])
            .inc_by(data_lock.bufferpin as u64);
        self.conflicts_total
            .with_label_values(&[database, "deadlock"])
            .inc_by(data_lock.deadlock as u64);
        self.conflicts_total
            .with_label_values(&[database, "active_logicalslot"])
            .inc_by(data_lock.active_logical_slot as u64);

        mfs.extend(self.conflicts_total.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGConflictCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_conflict_stats = if self.dbi.cfg.pg_version < POSTGRES_V16 {
            sqlx::query_as::<_, PGConflictStats>(POSTGRES_DATABASE_CONFLICT15)
                .fetch_optional(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGConflictStats>(POSTGRES_DATABASE_CONFLICT_LATEST)
                .fetch_optional(&self.dbi.db)
                .await?
        };

        if let Some(conflict_stats) = maybe_conflict_stats {
            if conflict_stats.database.is_empty() {
                return Ok(());
            }

            let mut data_lock = match self.data.write() {
                Ok(data_lock) => data_lock,
                Err(e) => bail!("can't unwrap lock. {}", e),
            };

            data_lock.database = conflict_stats.database;
            data_lock.deadlock = conflict_stats.deadlock;
            data_lock.active_logical_slot = conflict_stats.active_logical_slot;
            data_lock.bufferpin = conflict_stats.bufferpin;
            data_lock.snapshot = conflict_stats.snapshot;
            data_lock.tablespace = conflict_stats.tablespace;
            data_lock.lock = conflict_stats.lock;
        }

        Ok(())
    }
}

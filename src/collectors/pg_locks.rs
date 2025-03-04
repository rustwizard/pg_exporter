use async_trait::async_trait;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;
use prometheus::IntGauge;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::PG;

const LOCKSQUERY: &str = "SELECT  \
		count(*) FILTER (WHERE mode = 'AccessShareLock') AS access_share_lock,  \
		count(*) FILTER (WHERE mode = 'RowShareLock') AS row_share_lock, \
		count(*) FILTER (WHERE mode = 'RowExclusiveLock') AS row_exclusive_lock, \
		count(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock') AS share_update_exclusive_lock, \
		count(*) FILTER (WHERE mode = 'ShareLock') AS share_lock, \
		count(*) FILTER (WHERE mode = 'ShareRowExclusiveLock') AS share_row_exclusive_lock, \
		count(*) FILTER (WHERE mode = 'ExclusiveLock') AS exclusive_lock, \
		count(*) FILTER (WHERE mode = 'AccessExclusiveLock') AS access_exclusive_lock, \
		count(*) FILTER (WHERE not granted) AS not_granted, \
		count(*) AS total \
		FROM pg_locks";

/// 10 metrics per PGLocksCollector.
const LOCKS_METRICS_NUMBER: usize = 10;
const PGLOCKS_SUBSYSTEM: &str = "locks";

#[derive(Debug, Clone)]
pub struct PGLocksCollector {
    db: PgPool,
    data: Arc<Mutex<LocksStat>>,
    descs: Vec<Desc>,
    access_share_lock: IntGauge,
    row_share_lock: IntGauge,
    row_exclusive_lock: IntGauge,
    share_update_exclusive_lock: IntGauge,
    share_lock: IntGauge,
    share_row_exclusive_lock: IntGauge,
    exclusive_lock: IntGauge,
    access_exclusive_lock: IntGauge,
    not_granted: IntGauge,
    total: IntGauge,
}

#[derive(sqlx::FromRow, Debug, Default)]
pub struct LocksStat {
    access_share_lock: i64,
    row_share_lock: i64,
    row_exclusive_lock: i64,
    share_update_exclusive_lock: i64,
    share_lock: i64,
    share_row_exclusive_lock: i64,
    exclusive_lock: i64,
    access_exclusive_lock: i64,
    not_granted: i64,
    total: i64,
}

impl LocksStat {
    pub fn new() -> LocksStat {
        LocksStat {
            access_share_lock: (0),
            row_share_lock: (0),
            row_exclusive_lock: (0),
            share_update_exclusive_lock: (0),
            share_lock: (0),
            share_row_exclusive_lock: (0),
            exclusive_lock: (0),
            access_exclusive_lock: (0),
            not_granted: (0),
            total: (0),
        }
    }
}

pub fn new(db: PgPool, labels: HashMap<String, String>) -> PGLocksCollector {
    PGLocksCollector::new(db, labels)
}

impl PGLocksCollector {
    pub fn new(db: PgPool, labels: HashMap<String, String>) -> PGLocksCollector {
        let mut descs = Vec::new();

        let access_share_lock = IntGauge::with_opts(
            Opts::new("access_share_lock", "Total AccessShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(access_share_lock.desc().into_iter().cloned());

        let row_share_lock = IntGauge::with_opts(
            Opts::new("row_share_lock", "Total RowShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(row_share_lock.desc().into_iter().cloned());

        let row_exclusive_lock = IntGauge::with_opts(
            Opts::new("row_exclusive_lock", "Total RowExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(row_exclusive_lock.desc().into_iter().cloned());

        let share_update_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "share_update_exclusive_lock",
                "Total ShareUpdateExclusiveLock",
            )
            .namespace(super::NAMESPACE)
            .subsystem(PGLOCKS_SUBSYSTEM)
            .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(share_update_exclusive_lock.desc().into_iter().cloned());

        let share_lock = IntGauge::with_opts(
            Opts::new("share_lock", "Total ShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(share_lock.desc().into_iter().cloned());

        let share_row_exclusive_lock = IntGauge::with_opts(
            Opts::new("share_row_exclusive_lock", "Total ShareRowExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(share_row_exclusive_lock.desc().into_iter().cloned());

        let exclusive_lock = IntGauge::with_opts(
            Opts::new("exclusive_lock", "Total ExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(exclusive_lock.desc().into_iter().cloned());

        let access_exclusive_lock = IntGauge::with_opts(
            Opts::new("access_exclusive_lock", "Total AccessExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(access_exclusive_lock.desc().into_iter().cloned());

        let not_granted = IntGauge::with_opts(
            Opts::new("not_granted", "Total not granted")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels.clone()),
        )
        .unwrap();
        descs.extend(not_granted.desc().into_iter().cloned());

        let total = IntGauge::with_opts(
            Opts::new("total", "Total locks")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(labels),
        )
        .unwrap();
        descs.extend(total.desc().into_iter().cloned());

        PGLocksCollector {
            db: db,
            data: Arc::new(Mutex::new(LocksStat::new())),
            descs: descs,
            access_share_lock: access_share_lock,
            row_share_lock: row_share_lock,
            row_exclusive_lock: row_exclusive_lock,
            share_update_exclusive_lock: share_update_exclusive_lock,
            share_lock: share_lock,
            share_row_exclusive_lock: share_row_exclusive_lock,
            exclusive_lock: exclusive_lock,
            access_exclusive_lock: access_exclusive_lock,
            not_granted: not_granted,
            total: total,
        }
    }
}

impl Collector for PGLocksCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilys.
        let mut mfs = Vec::with_capacity(LOCKS_METRICS_NUMBER);

        let data_lock = self.data.lock().unwrap();

        self.access_share_lock.set(data_lock.access_share_lock);
        self.access_exclusive_lock.set(data_lock.access_exclusive_lock);
        self.exclusive_lock.set(data_lock.exclusive_lock);
        self.row_exclusive_lock.set(data_lock.row_exclusive_lock);
        self.row_share_lock.set(data_lock.row_share_lock);
        self.not_granted.set(data_lock.not_granted);
        self.share_lock.set(data_lock.share_lock);
        self.share_row_exclusive_lock.set(data_lock.share_row_exclusive_lock);
        self.share_update_exclusive_lock.set(data_lock.share_update_exclusive_lock);
        self.total.set(data_lock.total);

        mfs.extend(self.access_exclusive_lock.collect());
        mfs.extend(self.access_share_lock.collect());
        mfs.extend(self.exclusive_lock.collect());
        mfs.extend(self.row_exclusive_lock.collect());
        mfs.extend(self.row_share_lock.collect());
        mfs.extend(self.not_granted.collect());
        mfs.extend(self.share_lock.collect());
        mfs.extend(self.share_row_exclusive_lock.collect());
        mfs.extend(self.share_update_exclusive_lock.collect());
        mfs.extend(self.total.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGLocksCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_locks_stats = sqlx::query_as::<_, LocksStat>(LOCKSQUERY)
            .fetch_optional(&self.db)
            .await?;

        if let Some(locks_stats) = maybe_locks_stats {
            let mut data_lock = self.data.lock().unwrap();
            data_lock.access_exclusive_lock = locks_stats.access_exclusive_lock;
            data_lock.access_share_lock = locks_stats.access_share_lock;
            data_lock.exclusive_lock = locks_stats.exclusive_lock;
            data_lock.not_granted = locks_stats.not_granted;
            data_lock.row_exclusive_lock = locks_stats.row_exclusive_lock;
            data_lock.row_share_lock = locks_stats.row_share_lock;
            data_lock.share_lock = locks_stats.share_lock;
            data_lock.share_row_exclusive_lock = locks_stats.share_row_exclusive_lock;
            data_lock.share_update_exclusive_lock = locks_stats.share_update_exclusive_lock;
            data_lock.total = locks_stats.total;
        }

        Ok(())
    }
}

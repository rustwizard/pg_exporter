use anyhow::bail;
use async_trait::async_trait;
use prometheus::IntGauge;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;
use std::sync::{Arc, RwLock};
use tracing::error;

use crate::instance;

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
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<LocksStat>>,
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
    access_share_lock: Option<i64>,
    row_share_lock: Option<i64>,
    row_exclusive_lock: Option<i64>,
    share_update_exclusive_lock: Option<i64>,
    share_lock: Option<i64>,
    share_row_exclusive_lock: Option<i64>,
    exclusive_lock: Option<i64>,
    access_exclusive_lock: Option<i64>,
    not_granted: Option<i64>,
    total: Option<i64>,
}

impl LocksStat {
    pub fn new() -> LocksStat {
        LocksStat::default()
    }
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGLocksCollector> {
    match PGLocksCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            error!("error when create pg locks collector: {}", e);
            None
        }
    }
}

impl PGLocksCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGLocksCollector> {
        let mut descs = Vec::new();

        let access_share_lock = IntGauge::with_opts(
            Opts::new("access_share_lock", "Total AccessShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(access_share_lock.desc().into_iter().cloned());

        let row_share_lock = IntGauge::with_opts(
            Opts::new("row_share_lock", "Total RowShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(row_share_lock.desc().into_iter().cloned());

        let row_exclusive_lock = IntGauge::with_opts(
            Opts::new("row_exclusive_lock", "Total RowExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(row_exclusive_lock.desc().into_iter().cloned());

        let share_update_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "share_update_exclusive_lock",
                "Total ShareUpdateExclusiveLock",
            )
            .namespace(super::NAMESPACE)
            .subsystem(PGLOCKS_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(share_update_exclusive_lock.desc().into_iter().cloned());

        let share_lock = IntGauge::with_opts(
            Opts::new("share_lock", "Total ShareLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(share_lock.desc().into_iter().cloned());

        let share_row_exclusive_lock = IntGauge::with_opts(
            Opts::new("share_row_exclusive_lock", "Total ShareRowExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(share_row_exclusive_lock.desc().into_iter().cloned());

        let exclusive_lock = IntGauge::with_opts(
            Opts::new("exclusive_lock", "Total ExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(exclusive_lock.desc().into_iter().cloned());

        let access_exclusive_lock = IntGauge::with_opts(
            Opts::new("access_exclusive_lock", "Total AccessExclusiveLock")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(access_exclusive_lock.desc().into_iter().cloned());

        let not_granted = IntGauge::with_opts(
            Opts::new("not_granted", "Total not granted")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(not_granted.desc().into_iter().cloned());

        let total = IntGauge::with_opts(
            Opts::new("total", "Total locks")
                .namespace(super::NAMESPACE)
                .subsystem(PGLOCKS_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;
        descs.extend(total.desc().into_iter().cloned());

        let data = Arc::new(RwLock::new(LocksStat::new()));

        Ok(PGLocksCollector {
            dbi,
            data,
            descs,
            access_share_lock,
            row_share_lock,
            row_exclusive_lock,
            share_update_exclusive_lock,
            share_lock,
            share_row_exclusive_lock,
            exclusive_lock,
            access_exclusive_lock,
            not_granted,
            total,
        })
    }
}

impl Collector for PGLocksCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(LOCKS_METRICS_NUMBER);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg locks collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        let access_share_lock = data_lock.access_share_lock.unwrap_or_default();
        if access_share_lock > 0 {
            self.access_share_lock.set(access_share_lock);
        }

        let access_exclusive_lock = data_lock.access_exclusive_lock.unwrap_or_default();
        if access_exclusive_lock > 0 {
            self.access_exclusive_lock.set(access_exclusive_lock);
        }

        let exclusive_lock = data_lock.exclusive_lock.unwrap_or_default();
        if exclusive_lock > 0 {
            self.exclusive_lock.set(exclusive_lock);
        }

        let row_exclusive_lock = data_lock.row_exclusive_lock.unwrap_or_default();
        if row_exclusive_lock > 0 {
            self.row_exclusive_lock.set(row_exclusive_lock);
        }
        let row_share_lock = data_lock.row_share_lock.unwrap_or_default();
        if row_share_lock > 0 {
            self.row_share_lock.set(row_share_lock);
        }

        let not_granted = data_lock.not_granted.unwrap_or_default();
        if not_granted > 0 {
            self.not_granted.set(not_granted);
        }

        let share_lock = data_lock.share_lock.unwrap_or_default();
        if share_lock > 0 {
            self.share_lock.set(share_lock);
        }

        let share_row_exclusive_lock = data_lock.share_row_exclusive_lock.unwrap_or_default();
        if share_row_exclusive_lock > 0 {
            self.share_row_exclusive_lock.set(share_row_exclusive_lock);
        }

        let share_update_exclusive_lock = data_lock.share_update_exclusive_lock.unwrap_or_default();
        if share_update_exclusive_lock > 0 {
            self.share_update_exclusive_lock
                .set(share_update_exclusive_lock);
        }

        let total = data_lock.total.unwrap_or_default();
        if total > 0 {
            self.total.set(total);
        }

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
            .fetch_optional(&self.dbi.db)
            .await?;

        if let Some(locks_stats) = maybe_locks_stats {
            let mut data_lock = match self.data.write() {
                Ok(data_lock) => data_lock,
                Err(e) => bail!("pg indexes collector: can't acquire write lock. {}", e),
            };

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

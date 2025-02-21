use prometheus::core::{Desc, Opts, Collector};
use prometheus::IntGauge;
use prometheus::proto;

pub const LOCKSQUERY: &str = "SELECT  \
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
const METRICS_NUMBER: usize = 10;
#[derive(Debug)]
pub struct PGLocksCollector {
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

#[derive(sqlx::FromRow)]
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

impl PGLocksCollector {
    pub fn new<S: Into<String>>(namespace: S) -> PGLocksCollector {
        let namespace = namespace.into();
        let mut descs = Vec::new();

        let access_share_lock = IntGauge::with_opts(
            Opts::new(
                "access_share_lock",
                "Total AccessShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(access_share_lock.desc().into_iter().cloned());

        let row_share_lock = IntGauge::with_opts(
            Opts::new(
                "row_share_lock",
                "Total RowShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(row_share_lock.desc().into_iter().cloned());

        let row_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "row_exclusive_lock",
                "Total RowExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(row_exclusive_lock.desc().into_iter().cloned());

        let share_update_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "share_update_exclusive_lock",
                "Total ShareUpdateExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_update_exclusive_lock.desc().into_iter().cloned());

        let share_lock = IntGauge::with_opts(
            Opts::new(
                "share_lock",
                "Total ShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_lock.desc().into_iter().cloned());

        let share_row_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "share_row_exclusive_lock",
                "Total ShareRowExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_row_exclusive_lock.desc().into_iter().cloned());

        let exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "exclusive_lock",
                "Total ExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(exclusive_lock.desc().into_iter().cloned());

        let access_exclusive_lock = IntGauge::with_opts(
            Opts::new(
                "access_exclusive_lock",
                "Total AccessExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(access_exclusive_lock.desc().into_iter().cloned());

        let not_granted = IntGauge::with_opts(
            Opts::new(
                "not_granted",
                "Total not granted",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(not_granted.desc().into_iter().cloned());

        let total = IntGauge::with_opts(
            Opts::new(
                "total",
                "Total locks",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(total.desc().into_iter().cloned());

        PGLocksCollector{
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

    /// Return a `ProcessCollector` of the calling process.
    pub fn for_self() -> PGLocksCollector {
        PGLocksCollector::new("")
    }
}

impl Collector for PGLocksCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilys.
        let mut mfs = Vec::with_capacity(METRICS_NUMBER);
        
        // TODO: query postgres and set metics       
        self.access_share_lock.set(11 as i64);
        self.access_exclusive_lock.set(12 as i64);
        self.exclusive_lock.set(13 as i64);
        self.row_exclusive_lock.set(14 as i64);
        self.row_share_lock.set(15 as i64);
        self.not_granted.set(16 as i64);
        self.share_lock.set(17 as i64);
        self.share_row_exclusive_lock.set(18 as i64);
        self.share_update_exclusive_lock.set(19 as i64);
        self.total.set(20 as i64);

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


#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;
    use prometheus::{Registry, Encoder};

    #[test]
    fn test_pg_locks_collector() {
        let pc = PGLocksCollector::for_self();
        {
            let descs = pc.desc();
            assert_eq!(descs.len(), super::METRICS_NUMBER);
  
            let mfs = pc.collect();
            assert_eq!(mfs.len(), super::METRICS_NUMBER);
        }

        let r = Registry::new();
        let res = r.register(Box::new(pc));
        assert!(res.is_ok());

        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();

        let metric_families = r.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        // Output to the standard output.
        println!("test encoder: {:?}", String::from_utf8(buffer.clone()).unwrap());
    }
}
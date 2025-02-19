use prometheus::core::{Desc, Opts, Collector};
use prometheus::IntCounter;

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

#[derive(Debug)]
pub struct PGLocksCollector {
    descs: Vec<Desc>,
    access_share_lock: IntCounter,
    row_share_lock: IntCounter,
    row_exclusive_lock: IntCounter,
    share_update_exclusive_lock: IntCounter,
    share_lock: IntCounter,
    share_row_exclusive_lock: IntCounter,
    exclusive_lock: IntCounter,
    access_exclusive_lock: IntCounter,
    not_granted: IntCounter,
    total: IntCounter,
}

impl PGLocksCollector {
    pub fn new<S: Into<String>>(namespace: S) -> PGLocksCollector {
        let namespace = namespace.into();
        let mut descs = Vec::new();

        let access_share_lock = IntCounter::with_opts(
            Opts::new(
                "access_share_lock",
                "Total AccessShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(access_share_lock.desc().into_iter().cloned());

        let row_share_lock = IntCounter::with_opts(
            Opts::new(
                "row_share_lock",
                "Total RowShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(row_share_lock.desc().into_iter().cloned());

        let row_exclusive_lock = IntCounter::with_opts(
            Opts::new(
                "row_exclusive_lock",
                "Total RowExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(row_exclusive_lock.desc().into_iter().cloned());

        let share_update_exclusive_lock = IntCounter::with_opts(
            Opts::new(
                "share_update_exclusive_lock",
                "Total ShareUpdateExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_update_exclusive_lock.desc().into_iter().cloned());

        let share_lock = IntCounter::with_opts(
            Opts::new(
                "share_lock",
                "Total ShareLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_lock.desc().into_iter().cloned());

        let share_row_exclusive_lock = IntCounter::with_opts(
            Opts::new(
                "share_row_exclusive_lock",
                "Total ShareRowExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(share_row_exclusive_lock.desc().into_iter().cloned());

        let exclusive_lock = IntCounter::with_opts(
            Opts::new(
                "exclusive_lock",
                "Total ExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(exclusive_lock.desc().into_iter().cloned());

        let access_exclusive_lock = IntCounter::with_opts(
            Opts::new(
                "access_exclusive_lock",
                "Total AccessExclusiveLock",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(access_exclusive_lock.desc().into_iter().cloned());

        let not_granted = IntCounter::with_opts(
            Opts::new(
                "not_granted",
                "Total not granted",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(not_granted.desc().into_iter().cloned());

        let total = IntCounter::with_opts(
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

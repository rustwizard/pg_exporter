use prometheus::core::Desc;
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
}

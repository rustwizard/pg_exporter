use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, IntGaugeVec};

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

use std::sync::{Arc, RwLock};

use bigdecimal::{BigDecimal, FromPrimitive};
use prometheus::{core::Desc, Counter, Gauge, IntCounter};

use crate::instance;

const POSTGRES_WAL_QUERY96: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_xlog_receive_location() ELSE pg_current_xlog_location() END) - '0/00000000' AS wal_written";

const POSTGRES_WAL_QUERY13: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END) - '0/00000000' AS wal_written";

const POSTGRES_WAL_QUERY_LATEST: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, wal_records, wal_fpi, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() - '0/00000000' ELSE pg_current_wal_lsn() - '0/00000000' END) AS wal_written, 
		wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time, wal_sync_time, extract('epoch' from stats_reset) as reset_time 
		FROM pg_stat_wal";

#[derive(sqlx::FromRow, Debug)]
pub struct PGWALStats {
    recovery: i64,
    wal_records: i64,
    wal_fpi: i64,
    wal_written: BigDecimal,
    wal_bytes: BigDecimal,
    wal_buffers_full: i64,
    wal_write: i64,
    wal_sync: i64,
    wal_write_time: f64,
    reset_time: BigDecimal,
}

impl PGWALStats {
    fn new() -> Self {
        PGWALStats{ 
            recovery: (0), 
            wal_records: (0), 
            wal_fpi: (0), 
            wal_written: BigDecimal::from_i64(0).unwrap(),  
            wal_bytes: BigDecimal::from_i64(0).unwrap(), 
            wal_buffers_full: (0), 
            wal_write: (0), 
            wal_sync: (0), 
            wal_write_time: (0.0), 
            reset_time: BigDecimal::from_i64(0).unwrap(), 
        }
    }
}

pub struct PGWALCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGWALStats>>,
    descs: Vec<Desc>,
    recovery_info: Gauge,
    records_total: IntCounter,
    fpi_total: IntCounter,
    bytes_total: Counter,
    written_bytes_total: Counter,
    buffers_full_total: IntCounter,
    write_total: IntCounter,
    sync_total: IntCounter,
    seconds_all_total: Counter,
    seconds_total: Counter,
    stats_reset_time: Counter
}
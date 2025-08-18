use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Gauge, IntCounter, IntGauge};

use crate::collectors::{PG, POSTGRES_V15};
use crate::instance;

const POSTGRES_DATABASE_CONFLICT15: &str = "SELECT datname AS database, 
    confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock
    FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'";

const POSTGRES_DATABASE_CONFLICT_LATEST: &str = "SELECT datname AS database, 
confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock, confl_active_logicalslot
		FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'";

#[derive(sqlx::FromRow, Debug)]
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

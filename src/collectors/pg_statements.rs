use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntCounterVec};
use prometheus::{IntGaugeVec, proto};

use crate::collectors::PG;
use crate::instance;

const STATEMENTS_QUERY16: &str = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, 
		NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes 
		FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid";

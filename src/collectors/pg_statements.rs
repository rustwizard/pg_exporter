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
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0)::FLOAT8 AS wal_bytes 
		FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid";

const STATEMENTS_QUERY16_TOPK: &str = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, 
		NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, 
		(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_write_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
		SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, 
		shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
		temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
		'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), 
		NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), 
		NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)";

const STATEMENTS_QUERY17: &str = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes 
		FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid";

const STATEMENTS_QUERY17_TOPK: &str = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, 
		(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blk_write_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
		SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, 
		shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
		temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
		'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), 
		NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), 
		NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)";

const	STATEMENTS_QUERY_LATEST: &str = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, 
		NULLIF(p.wal_buffers_full, 0) AS wal_buffers_full 
		FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid";

const STATEMENTS_QUERY_LATEST_TOP_K: &str = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
    COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
    p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
    NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
    NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
    NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
    NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
    NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, 
    NULLIF(p.wal_buffers_full, 0) AS wal_buffers_full, 
    (ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.shared_blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blk_write_time DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR 
    (ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_buffers_full DESC NULLS LAST) < $1) AS visible 
    FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
    SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, 
    shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
    temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
    'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), 
    NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
    NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
    NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
    NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
    NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), 
    NULLIF(SUM(COALESCE(wal_bytes, 0)), 0), NULLIF(SUM(COALESCE(wal_buffers_full, 0)), 0) FROM stat WHERE NOT visible 
    GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)";

#[derive(sqlx::FromRow, Debug)]
pub struct PGStatementsStat {
    database: String,
    user: String,
    queryid: i64,
    query: String,
    calls: i64,
    rows: i64,
    total_exec_time: f64,
    total_plan_time: f64,
    blk_read_time: f64,
    blk_write_time: f64,
    shared_blks_hit: i64,
    shared_blks_read: i64,
    shared_blks_dirtied: i64,
    shared_blks_written: i64,
    local_blks_hit: i64,
    local_blks_read: i64,
    local_blks_dirtied: i64,
    local_blks_written: i64,
    temp_blks_read: i64,
    temp_blks_written: i64,
    wal_records: f64,
    wal_fpi: f64,
    wal_bytes: f64,
    wal_buffers: f64,
}

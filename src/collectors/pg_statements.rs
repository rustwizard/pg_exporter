use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use crate::collectors::{PG, POSTGRES_V12, POSTGRES_V13, POSTGRES_V16, POSTGRES_V17, POSTGRES_V18};
use crate::instance;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntCounterVec, IntGaugeVec, proto};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

// defines query for querying statements metrics for PG12 and older.
macro_rules! statements_query12 {
() =>  {
	"SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_time, p.blk_read_time, p.blk_write_time, 
		NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written 
		FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"
	}
}

macro_rules! statements_query12_topk {
    () => { "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_time, p.blk_read_time, p.blk_write_time, 
		NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.total_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_write_time DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR 
		(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) AS visible 
		FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
		SELECT DATABASE, \"user\", queryid, query, calls, rows, total_time, blk_read_time, blk_write_time, shared_blks_hit, 
		shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
		temp_blks_read, temp_blks_written FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
		'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_time, 0)), 0), 
		NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
	}
}

macro_rules! statements_query16 {
() =>  {
	"SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, 
		NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0)::FLOAT8 AS wal_bytes 
		FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"
	}
}

macro_rules! statements_query16_topk {
() =>  {
	"WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, 
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
		(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
		SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, 
		shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
		temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
		'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), 
		NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), 
		NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
	}
}

macro_rules! statements_query17 {
() =>  {
	"SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes 
		FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"
	}
}

macro_rules! statements_query17_topk {
() =>  {
	"WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
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
		(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
		SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, 
		shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, 
		temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, 
		'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), 
		NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), 
		NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), 
		NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), 
		NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
	}
}

macro_rules! statements_query_latest {
() =>  {
	"SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
		COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, 
		NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, 
		NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, 
		NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, 
		NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, 
		NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, 
		NULLIF(p.wal_buffers_full, 0) AS wal_buffers_full 
		FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"
	}
}

macro_rules! statements_query_latest_topk {
() =>  {
	"WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, 
    COALESCE({}, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, 
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
    FROM {}.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) 
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
    GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
	}
}

#[derive(sqlx::FromRow, Debug)]
pub struct PGStatementsStat {
    database: Option<String>,
    user: Option<String>,
    queryid: Option<i64>,
    query: Option<String>,
    calls: Option<Decimal>,
    rows: Option<Decimal>,
    total_exec_time: Option<f64>,
    total_plan_time: Option<f64>,
    blk_read_time: Option<f64>,
    blk_write_time: Option<f64>,
    shared_blks_hit: Option<Decimal>,
    shared_blks_read: Option<Decimal>,
    shared_blks_dirtied: Option<Decimal>,
    shared_blks_written: Option<Decimal>,
    local_blks_hit: Option<Decimal>,
    local_blks_read: Option<Decimal>,
    local_blks_dirtied: Option<Decimal>,
    local_blks_written: Option<Decimal>,
    temp_blks_read: Option<Decimal>,
    temp_blks_written: Option<Decimal>,
    wal_records: Option<Decimal>,
    wal_fpi: Option<Decimal>,
    wal_bytes: Option<Decimal>,
    #[sqlx(default)]
    wal_buffers: Option<Decimal>,
}

impl PGStatementsStat {
    fn new() -> Self {
        Self {
            database: None,
            user: None,
            queryid: None,
            query: None,
            calls: None,
            rows: None,
            total_exec_time: None,
            total_plan_time: None,
            blk_read_time: None,
            blk_write_time: None,
            shared_blks_hit: None,
            shared_blks_read: None,
            shared_blks_dirtied: None,
            shared_blks_written: None,
            local_blks_hit: None,
            local_blks_read: None,
            local_blks_dirtied: None,
            local_blks_written: None,
            temp_blks_read: None,
            temp_blks_written: None,
            wal_records: None,
            wal_fpi: None,
            wal_bytes: None,
            wal_buffers: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGStatementsCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGStatementsStat>>>,
    descs: Vec<Desc>,
    query: IntGaugeVec,
    calls: IntGaugeVec,
    rows: IntGaugeVec,
    times: IntGaugeVec,
    all_times: IntGaugeVec,
    shared_hit: IntGaugeVec,
    shared_read: IntGaugeVec,
    shared_dirtied: IntGaugeVec,
    shared_written: IntGaugeVec,
}

impl PGStatementsCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> Self {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGStatementsStat::new()]));

        let query = IntGaugeVec::new(
            Opts::new(
                "query_info",
                "Labeled info about statements has been executed.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid", "query"],
        )
        .unwrap();
        descs.extend(query.desc().into_iter().cloned());

        let calls = IntGaugeVec::new(
            Opts::new(
                "calls_total",
                "Total number of times statement has been executed.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(calls.desc().into_iter().cloned());

        let rows = IntGaugeVec::new(
            Opts::new(
                "rows_total",
                "Total number of rows retrieved or affected by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(rows.desc().into_iter().cloned());

        let times = IntGaugeVec::new(
            Opts::new(
                "time_seconds_total",
                "Time spent by the statement in each mode, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid", "mode"],
        )
        .unwrap();
        descs.extend(times.desc().into_iter().cloned());

        let all_times = IntGaugeVec::new(
            Opts::new(
                "time_seconds_all_total",
                "Total time spent by the statement, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(all_times.desc().into_iter().cloned());

        let shared_hit = IntGaugeVec::new(
            Opts::new(
                "shared_buffers_hit_total",
                "Total number of blocks have been found in shared buffers by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(shared_hit.desc().into_iter().cloned());

        let shared_read = IntGaugeVec::new(
            Opts::new(
                "shared_buffers_read_bytes_total",
                "Total number of bytes read from disk or OS page cache by the statement when block not found in shared buffers.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(shared_read.desc().into_iter().cloned());

        let shared_dirtied = IntGaugeVec::new(
            Opts::new(
                "shared_buffers_dirtied_total",
                "Total number of blocks have been dirtied in shared buffers by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(shared_dirtied.desc().into_iter().cloned());

        let shared_written = IntGaugeVec::new(
            Opts::new(
                "shared_buffers_written_bytes_total",
                "Total number of bytes written from shared buffers to disk by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )
        .unwrap();
        descs.extend(shared_written.desc().into_iter().cloned());

        Self {
            dbi,
            data,
            descs,
            query,
            calls,
            rows,
            times,
            all_times,
            shared_hit,
            shared_read,
            shared_dirtied,
            shared_written,
        }
    }

    fn select_query(&self) -> String {
        let query_column = if self.dbi.cfg.notrack {
            "null"
        } else {
            "p.query"
        };

        if self.dbi.cfg.pg_version < POSTGRES_V13 {
            if self.dbi.cfg.pg_collect_topq > 0 {
                format!(
                    statements_query12_topk!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query12!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            }
        } else if self.dbi.cfg.pg_version > POSTGRES_V12 && self.dbi.cfg.pg_version < POSTGRES_V17 {
            if self.dbi.cfg.pg_collect_topq > 0 {
                format!(
                    statements_query16_topk!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query16!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            }
        } else if self.dbi.cfg.pg_version > POSTGRES_V16 && self.dbi.cfg.pg_version < POSTGRES_V18 {
            if self.dbi.cfg.pg_collect_topq > 0 {
                format!(
                    statements_query17_topk!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query17!(),
                    query_column, self.dbi.cfg.pg_stat_statements_schema
                )
            }
        } else if self.dbi.cfg.pg_collect_topq > 0 {
            format!(
                statements_query_latest_topk!(),
                query_column, self.dbi.cfg.pg_stat_statements_schema
            )
        } else {
            format!(
                statements_query_latest!(),
                query_column, self.dbi.cfg.pg_stat_statements_schema
            )
        }
    }
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStatementsCollector> {
    // Collecting since Postgres 12.
    if dbi.cfg.pg_version >= POSTGRES_V12 {
        Some(PGStatementsCollector::new(dbi))
    } else {
        None
    }
}

impl Collector for PGStatementsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = self.data.read().expect("can't acuire lock");

        for row in data_lock.iter() {
            // TODO: remove all unwraps later
            let q = row.query.as_ref().unwrap();
            let qq = q.as_str();
            let query = if self.dbi.cfg.notrack {
                "/* query text hidden, no-track mode enabled */"
            } else {
                qq
            };

            self.query
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(), // we could do unwrap(), because user field if always not null
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                    query,
                ])
                .set(1);

            self.calls
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(),
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                ])
                .set(row.calls.unwrap_or_default().to_i64().unwrap_or_default());

            self.rows
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(),
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                ])
                .set(row.rows.unwrap_or_default().to_i64().unwrap_or_default());

            // total = planning + execution; execution already includes io time.
            let total_plan_time = row
                .total_plan_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            let total_exec_time = row
                .total_exec_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            self.all_times
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(),
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                ])
                .set(total_plan_time + total_exec_time);

            let blk_read_time = row
                .blk_read_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            let blk_write_time = row
                .blk_write_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            self.times
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(),
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                    "planning",
                ])
                .set(total_plan_time);

            // execution time = execution - io times.
            self.times
                .with_label_values(&[
                    row.user.clone().unwrap().as_str(),
                    row.database.clone().unwrap().as_str(),
                    row.queryid.unwrap_or_default().to_string().as_str(),
                    "executing",
                ])
                .set(total_exec_time - (blk_read_time + blk_write_time));

            // avoid metrics spamming and send metrics only if they greater than zero.
            if blk_read_time > 0 {
                self.times
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                        "ioread",
                    ])
                    .set(blk_read_time);
            }

            if blk_write_time > 0 {
                self.times
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                        "iowrite",
                    ])
                    .set(blk_write_time);
            }

            let shared_blks_hit = row
                .shared_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_hit > 0 {
                self.shared_hit
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                    ])
                    .set(shared_blks_hit);
            }

            let shared_blks_read = row
                .shared_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_read > 0 {
                self.shared_read
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                    ])
                    .set(shared_blks_read);
            }

            let shared_blks_dirtied = row
                .shared_blks_dirtied
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_dirtied > 0 {
                self.shared_dirtied
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                    ])
                    .set(shared_blks_dirtied);
            }

            let shared_blks_written = row
                .shared_blks_written
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_written > 0 {
                self.shared_written
                    .with_label_values(&[
                        row.user.clone().unwrap().as_str(),
                        row.database.clone().unwrap().as_str(),
                        row.queryid.unwrap_or_default().to_string().as_str(),
                    ])
                    .set(shared_blks_written);
            }
        }

        mfs.extend(self.query.collect());
        mfs.extend(self.calls.collect());
        mfs.extend(self.rows.collect());
        mfs.extend(self.all_times.collect());
        mfs.extend(self.times.collect());
        mfs.extend(self.shared_hit.collect());
        mfs.extend(self.shared_read.collect());
        mfs.extend(self.shared_dirtied.collect());
        mfs.extend(self.shared_written.collect());

        mfs
    }
}
#[async_trait]
impl PG for PGStatementsCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let query = self.select_query();

        let mut pg_statemnts_rows = sqlx::query_as::<_, PGStatementsStat>(&query)
            .bind(self.dbi.cfg.pg_collect_topq)
            .fetch_all(&self.dbi.db)
            .await?;

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("can't unwrap lock. {}", e),
        };

        data_lock.clear();

        data_lock.append(&mut pg_statemnts_rows);

        Ok(())
    }
}

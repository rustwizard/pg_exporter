use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use tracing::error;

use crate::collectors::{PG, POSTGRES_V12, POSTGRES_V13, POSTGRES_V16, POSTGRES_V17, POSTGRES_V18};
use crate::instance;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

// defines query for querying statements metrics for PG12 and older.
macro_rules! statements_query12 {
() =>  {
	"SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid,
		COALESCE({}, '') AS query, p.calls::numeric, p.rows::numeric, p.total_time, p.blk_read_time, p.blk_write_time,
		NULLIF(p.shared_blks_hit, 0)::numeric AS shared_blks_hit, NULLIF(p.shared_blks_read, 0)::numeric AS shared_blks_read,
		NULLIF(p.shared_blks_dirtied, 0)::numeric AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0)::numeric AS shared_blks_written,
		NULLIF(p.local_blks_hit, 0)::numeric AS local_blks_hit, NULLIF(p.local_blks_read, 0)::numeric AS local_blks_read,
		NULLIF(p.local_blks_dirtied, 0)::numeric AS local_blks_dirtied, NULLIF(p.local_blks_written, 0)::numeric AS local_blks_written,
		NULLIF(p.temp_blks_read, 0)::numeric AS temp_blks_read, NULLIF(p.temp_blks_written, 0)::numeric AS temp_blks_written
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
		COALESCE({}, '') AS query, p.calls::numeric, p.rows::numeric, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time,
		NULLIF(p.shared_blks_hit, 0)::numeric AS shared_blks_hit, NULLIF(p.shared_blks_read, 0)::numeric AS shared_blks_read,
		NULLIF(p.shared_blks_dirtied, 0)::numeric AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0)::numeric AS shared_blks_written,
		NULLIF(p.local_blks_hit, 0)::numeric AS local_blks_hit, NULLIF(p.local_blks_read, 0)::numeric AS local_blks_read,
		NULLIF(p.local_blks_dirtied, 0)::numeric AS local_blks_dirtied, NULLIF(p.local_blks_written, 0)::numeric AS local_blks_written,
		NULLIF(p.temp_blks_read, 0)::numeric AS temp_blks_read, NULLIF(p.temp_blks_written, 0)::numeric AS temp_blks_written,
		NULLIF(p.wal_records, 0)::numeric AS wal_records, NULLIF(p.wal_fpi, 0)::numeric AS wal_fpi, NULLIF(p.wal_bytes, 0)::numeric AS wal_bytes
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
		COALESCE({}, '') AS query, p.calls::numeric, p.rows::numeric, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time,
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0)::numeric AS shared_blks_hit, NULLIF(p.shared_blks_read, 0)::numeric AS shared_blks_read,
		NULLIF(p.shared_blks_dirtied, 0)::numeric AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0)::numeric AS shared_blks_written,
		NULLIF(p.local_blks_hit, 0)::numeric AS local_blks_hit, NULLIF(p.local_blks_read, 0)::numeric AS local_blks_read,
		NULLIF(p.local_blks_dirtied, 0)::numeric AS local_blks_dirtied, NULLIF(p.local_blks_written, 0)::numeric AS local_blks_written,
		NULLIF(p.temp_blks_read, 0)::numeric AS temp_blks_read, NULLIF(p.temp_blks_written, 0)::numeric AS temp_blks_written,
		NULLIF(p.wal_records, 0)::numeric AS wal_records, NULLIF(p.wal_fpi, 0)::numeric AS wal_fpi, NULLIF(p.wal_bytes, 0)::numeric AS wal_bytes
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
		COALESCE({}, '') AS query, p.calls::numeric, p.rows::numeric, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time,
		p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0)::numeric AS shared_blks_hit, NULLIF(p.shared_blks_read, 0)::numeric AS shared_blks_read,
		NULLIF(p.shared_blks_dirtied, 0)::numeric AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0)::numeric AS shared_blks_written,
		NULLIF(p.local_blks_hit, 0)::numeric AS local_blks_hit, NULLIF(p.local_blks_read, 0)::numeric AS local_blks_read,
		NULLIF(p.local_blks_dirtied, 0)::numeric AS local_blks_dirtied, NULLIF(p.local_blks_written, 0)::numeric AS local_blks_written,
		NULLIF(p.temp_blks_read, 0)::numeric AS temp_blks_read, NULLIF(p.temp_blks_written, 0)::numeric AS temp_blks_written,
		NULLIF(p.wal_records, 0)::numeric AS wal_records, NULLIF(p.wal_fpi, 0)::numeric AS wal_fpi, NULLIF(p.wal_bytes, 0)::numeric AS wal_bytes,
		NULLIF(p.wal_buffers_full, 0)::numeric AS wal_buffers_full
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
    local_hit: IntGaugeVec,
    local_read: IntGaugeVec,
    local_dirtied: IntGaugeVec,
    local_written: IntGaugeVec,
    temp_read: IntGaugeVec,
    temp_written: IntGaugeVec,
    wal_records: IntGaugeVec,
    wal_buffers: IntGaugeVec,
    wal_all_bytes: IntGaugeVec,
}

impl PGStatementsCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
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
        )?;
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
        )?;
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
        )?;
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
        )?;
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
        )?;
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
        )?;
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
        )?;
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
        )?;
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
        )?;
        descs.extend(shared_written.desc().into_iter().cloned());

        let local_hit = IntGaugeVec::new(
            Opts::new(
                "local_buffers_hit_total",
                "Total number of blocks have been found in local buffers by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(local_hit.desc().into_iter().cloned());

        let local_read = IntGaugeVec::new(
            Opts::new(
                "local_buffers_read_bytes_total",
                "Total number of bytes read from disk or OS page cache by the statement when block not found in local buffers.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(local_read.desc().into_iter().cloned());

        let local_dirtied = IntGaugeVec::new(
            Opts::new(
                "local_buffers_dirtied_total",
                "Total number of blocks have been dirtied in local buffers by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(local_dirtied.desc().into_iter().cloned());

        let local_written = IntGaugeVec::new(
            Opts::new(
                "local_buffers_written_bytes_total",
                "Total number of bytes written from local buffers to disk by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(local_written.desc().into_iter().cloned());

        let temp_read = IntGaugeVec::new(
            Opts::new(
                "temp_read_bytes_total",
                "Total number of bytes read from temporary files by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(temp_read.desc().into_iter().cloned());

        let temp_written = IntGaugeVec::new(
            Opts::new(
                "temp_written_bytes_total",
                "Total number of bytes written to temporary files by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(temp_written.desc().into_iter().cloned());

        let wal_records = IntGaugeVec::new(
            Opts::new(
                "wal_records_total",
                "Total number of WAL records generated by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(wal_records.desc().into_iter().cloned());

        let wal_buffers = IntGaugeVec::new(
            Opts::new(
                "wal_buffers_full",
                "Total number of times the WAL buffers became full generated by the statement.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(wal_buffers.desc().into_iter().cloned());

        let wal_all_bytes = IntGaugeVec::new(
            Opts::new(
                "wal_bytes_all_total",
                "Total number of WAL generated by the statement, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("statements")
            .const_labels(dbi.labels.clone()),
            &["user", "database", "queryid"],
        )?;
        descs.extend(wal_all_bytes.desc().into_iter().cloned());

        Ok(Self {
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
            local_hit,
            local_read,
            local_dirtied,
            local_written,
            temp_read,
            temp_written,
            wal_records,
            wal_buffers,
            wal_all_bytes,
        })
    }

    fn select_query(&self, cfg: &instance::PGConfig) -> String {
        let query_column = if cfg.notrack { "null" } else { "p.query" };

        if cfg.pg_version < POSTGRES_V13 {
            if cfg.pg_collect_topq > 0 {
                format!(
                    statements_query12_topk!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query12!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            }
        } else if cfg.pg_version > POSTGRES_V12 && cfg.pg_version < POSTGRES_V17 {
            if cfg.pg_collect_topq > 0 {
                format!(
                    statements_query16_topk!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query16!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            }
        } else if cfg.pg_version > POSTGRES_V16 && cfg.pg_version < POSTGRES_V18 {
            if cfg.pg_collect_topq > 0 {
                format!(
                    statements_query17_topk!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            } else {
                format!(
                    statements_query17!(),
                    query_column, cfg.pg_stat_statements_schema
                )
            }
        } else if cfg.pg_collect_topq > 0 {
            format!(
                statements_query_latest_topk!(),
                query_column, cfg.pg_stat_statements_schema
            )
        } else {
            format!(
                statements_query_latest!(),
                query_column, cfg.pg_stat_statements_schema
            )
        }
    }
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStatementsCollector> {
    // Collecting since Postgres 12.
    if dbi
        .current_cfg()
        .map(|c| c.pg_version >= POSTGRES_V12 && c.pg_stat_statements)
        .unwrap_or(true)
    {
        match PGStatementsCollector::new(dbi) {
            Ok(result) => Some(result),
            Err(e) => {
                error!("error when create pg statements collector: {}", e);
                None
            }
        }
    } else {
        None
    }
}

impl Collector for PGStatementsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let cfg = match self.dbi.current_cfg() {
            Some(c) => c,
            None => return Vec::new(),
        };
        if !cfg.pg_stat_statements {
            return Vec::new();
        }
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg statements collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        for row in data_lock.iter() {
            let q = match row.query.as_ref() {
                Some(q) => q,
                None => {
                    error!(
                        "pg statements collect: get query: {:?}",
                        anyhow!("query is empty")
                    );
                    // return empty mfs
                    return mfs;
                }
            }
            .as_str();

            let query = if cfg.notrack {
                "/* query text hidden, no-track mode enabled */"
            } else {
                q
            };

            let user = match row.user.as_ref() {
                Some(q) => q,
                None => {
                    error!(
                        "pg statements collect: get user: {:?}",
                        anyhow!("user is empty")
                            .context(format!("pg_ver: {}", cfg.pg_version))
                    );
                    // return empty mfs
                    return mfs;
                }
            }
            .as_str();

            let database = match row.database.as_ref() {
                Some(d) => d,
                None => {
                    error!(
                        "pg statements collect: get database: {:?}",
                        anyhow!("database is empty")
                            .context(format!("pg_ver: {}", cfg.pg_version))
                    );
                    // return empty mfs
                    return mfs;
                }
            }
            .as_str();

            let query_id = row.queryid.unwrap_or_default().to_string();

            self.query
                .with_label_values(&[user, database, query_id.as_str(), query])
                .set(1);

            self.calls
                .with_label_values(&[user, database, query_id.as_str()])
                .set(row.calls.unwrap_or_default().to_i64().unwrap_or_default());

            self.rows
                .with_label_values(&[user, database, query_id.as_str()])
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
                .with_label_values(&[user, database, query_id.as_str()])
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
                .with_label_values(&[user, database, query_id.as_str(), "planning"])
                .set(total_plan_time);

            // execution time = execution - io times.
            self.times
                .with_label_values(&[user, database, query_id.as_str(), "executing"])
                .set(total_exec_time - (blk_read_time + blk_write_time));

            // avoid metrics spamming and send metrics only if they greater than zero.
            if blk_read_time > 0 {
                self.times
                    .with_label_values(&[user, database, query_id.as_str(), "ioread"])
                    .set(blk_read_time);
            }

            if blk_write_time > 0 {
                self.times
                    .with_label_values(&[user, database, query_id.as_str(), "iowrite"])
                    .set(blk_write_time);
            }

            let shared_blks_hit = row
                .shared_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_hit > 0 {
                self.shared_hit
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(shared_blks_hit);
            }

            let block_size = cfg.pg_block_size;

            let shared_blks_read = row
                .shared_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_read > 0 {
                self.shared_read
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(shared_blks_read * block_size);
            }

            let shared_blks_dirtied = row
                .shared_blks_dirtied
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_dirtied > 0 {
                self.shared_dirtied
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(shared_blks_dirtied);
            }

            let shared_blks_written = row
                .shared_blks_written
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if shared_blks_written > 0 {
                self.shared_written
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(shared_blks_written * block_size);
            }

            let local_blks_hit = row
                .local_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if local_blks_hit > 0 {
                self.local_hit
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(local_blks_hit);
            }

            let local_blks_read = row
                .local_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if local_blks_read > 0 {
                self.local_read
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(local_blks_read * block_size);
            }

            let local_blks_dirtied = row
                .local_blks_dirtied
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if local_blks_dirtied > 0 {
                self.local_dirtied
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(local_blks_dirtied);
            }

            let local_blks_written = row
                .local_blks_written
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if local_blks_written > 0 {
                self.local_written
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(local_blks_written * block_size);
            }

            let temp_blks_read = row
                .temp_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if temp_blks_read > 0 {
                self.temp_read
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(temp_blks_read * block_size);
            }

            let temp_blks_written = row
                .temp_blks_written
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if temp_blks_written > 0 {
                self.temp_written
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(temp_blks_written * block_size);
            }

            let wal_records = row
                .wal_records
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if wal_records > 0 {
                // WAL records
                self.wal_records
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set(wal_records);
                // WAL total bytes
                let wal_fpi = row.wal_fpi.unwrap_or_default().to_i64().unwrap_or_default();

                let wal_bytes = row
                    .wal_bytes
                    .unwrap_or_default()
                    .to_i64()
                    .unwrap_or_default();

                self.wal_all_bytes
                    .with_label_values(&[user, database, query_id.as_str()])
                    .set((wal_fpi * block_size) + wal_bytes);

                if cfg.pg_version >= POSTGRES_V18 {
                    // WAL buffers
                    let wal_buffers = row
                        .wal_buffers
                        .unwrap_or_default()
                        .to_i64()
                        .unwrap_or_default();

                    self.wal_buffers
                        .with_label_values(&[user, database, query_id.as_str()])
                        .set(wal_buffers);
                }
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
        mfs.extend(self.local_hit.collect());
        mfs.extend(self.local_read.collect());
        mfs.extend(self.local_dirtied.collect());
        mfs.extend(self.local_written.collect());
        mfs.extend(self.temp_read.collect());
        mfs.extend(self.temp_written.collect());
        mfs.extend(self.wal_buffers.collect());
        mfs.extend(self.wal_records.collect());

        mfs
    }
}
#[async_trait]
impl PG for PGStatementsCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let cfg = self.dbi.ensure_ready().await?;
        if !cfg.pg_stat_statements {
            return Ok(());
        }

        let query = self.select_query(&cfg);

        let mut pg_statemnts_rows = sqlx::query_as::<_, PGStatementsStat>(&query)
            .bind(cfg.pg_collect_topq)
            .fetch_all(&self.dbi.db)
            .await?;

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg statements collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();

        data_lock.append(&mut pg_statemnts_rows);

        Ok(())
    }
}

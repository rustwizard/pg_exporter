use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::IntCounterVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;

use crate::collectors::{PG, POSTGRES_V16};
use crate::instance;

const USER_INDEXES_QUERY: &str = "SELECT current_database() AS database, schemaname AS schema, relname AS table, 
        indexrelname AS index, (i.indisprimary OR i.indisunique) AS key,
		i.indisvalid AS isvalid, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit, pg_relation_size(s1.indexrelid) AS size_bytes 
		FROM pg_stat_user_indexes s1 
		JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) 
		JOIN pg_index i ON (s1.indexrelid = i.indexrelid) 
		WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)";

const USER_INDEXES_QUERY_TOPK: &str = "WITH stat AS (SELECT schemaname AS schema, relname AS table, indexrelname AS index, 
        (i.indisprimary OR i.indisunique) AS key, 
		i.indisvalid AS isvalid, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit, pg_relation_size(s1.indexrelid) AS size_bytes, 
		NOT i.indisvalid OR /* unused and size > 50mb */ (idx_scan = 0 AND pg_relation_size(s1.indexrelid) > 50*1024*1024) OR 
		(row_number() OVER (ORDER BY idx_scan DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_tup_read DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY idx_tup_fetch DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_blks_read DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY idx_blks_hit DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY pg_relation_size(s1.indexrelid) DESC NULLS LAST) < $1) AS visible 
		FROM pg_stat_user_indexes s1 
		JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) 
		JOIN pg_index i ON (s1.indexrelid = i.indexrelid) 
		WHERE NOT EXISTS ( SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)) 
		SELECT current_database() AS database, \"schema\", \"table\", \"index\", \"key\", isvalid, idx_scan, idx_tup_read, idx_tup_fetch, 
		idx_blks_read, idx_blks_hit, size_bytes FROM stat WHERE visible 
		UNION ALL SELECT current_database() AS database, 'all_shemas', 'all_other_tables', 'all_other_indexes', true, null, 
		NULLIF(SUM(COALESCE(idx_scan,0)),0), NULLIF(SUM(COALESCE(idx_tup_fetch,0)),0), NULLIF(SUM(COALESCE(idx_tup_read,0)),0), 
		NULLIF(SUM(COALESCE(idx_blks_read,0)),0), NULLIF(SUM(COALESCE(idx_blks_hit,0)),0), 
		NULLIF(SUM(COALESCE(size_bytes,0)),0) FROM stat WHERE NOT visible HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)";

#[derive(sqlx::FromRow, Debug)]
pub struct PGIndexesStats {
    database: String,
    schema: String,
    table: String,
    index: String,
    key: bool,
    isvalid: bool,
    idx_scan: i64,
    idx_tup_read: i64,
    idx_tup_fetch: i64,
    idx_blks_read: i64,
    idx_blks_hit: i64,
    size_bytes: i64,
}

impl PGIndexesStats {
    fn new() -> Self {
        Self {
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            index: String::new(),
            key: false,
            isvalid: false,
            idx_scan: (0),
            idx_tup_read: (0),
            idx_tup_fetch: (0),
            idx_blks_read: (0),
            idx_blks_hit: (0),
            size_bytes: (0),
        }
    }
}

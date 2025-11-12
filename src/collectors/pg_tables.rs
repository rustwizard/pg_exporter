use rust_decimal::Decimal;

const POSTGRES_USERS_TABLE: &str = "SELECT current_database() AS database, s1.schemaname AS schema, s1.relname AS table, 
		seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, 
		n_live_tup, n_dead_tup, n_mod_since_analyze, 
		EXTRACT(EPOCH FROM AGE(now(), GREATEST(last_vacuum, last_autovacuum))) AS last_vacuum_seconds, 
		EXTRACT(EPOCH FROM AGE(now(), GREATEST(last_analyze, last_autoanalyze))) AS last_analyze_seconds, 
		EXTRACT(EPOCH FROM GREATEST(last_vacuum, last_autovacuum)) AS last_vacuum_time, 
		EXTRACT(EPOCH FROM GREATEST(last_analyze, last_autoanalyze)) AS last_analyze_time, 
		vacuum_count, autovacuum_count,  analyze_count, autoanalyze_count, heap_blks_read, heap_blks_hit, idx_blks_read, 
		idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit, 
		pg_table_size(s1.relid) AS size_bytes, reltuples 
		FROM pg_stat_user_tables s1 JOIN pg_statio_user_tables s2 USING (schemaname, relname) JOIN pg_class c ON s1.relid = c.oid 
		WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.relid AND mode = 'AccessExclusiveLock' AND granted)";

const POSTGRES_USERS_TABLE_TOPK: &str = "WITH stat AS ( SELECT s1.schemaname AS schema, s1.relname AS table, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, 
		n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, 
		EXTRACT(EPOCH FROM AGE(now(), GREATEST(last_vacuum, last_autovacuum))) AS last_vacuum_seconds, 
		EXTRACT(EPOCH FROM AGE(now(), GREATEST(last_analyze, last_autoanalyze))) AS last_analyze_seconds, 
		EXTRACT(EPOCH FROM GREATEST(last_vacuum, last_autovacuum)) AS last_vacuum_time, 
		EXTRACT(EPOCH FROM GREATEST(last_analyze, last_autoanalyze)) AS last_analyze_time, 
		vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, heap_blks_read, heap_blks_hit, idx_blks_read, 
		idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit, pg_table_size(s1.relid) AS size_bytes, 
		reltuples, (row_number() OVER (ORDER BY seq_scan DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY seq_tup_read DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY idx_scan DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_tup_fetch DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY n_tup_ins DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY n_tup_upd DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY n_tup_del DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY n_tup_hot_upd DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY n_live_tup DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY n_dead_tup DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY n_mod_since_analyze DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY vacuum_count DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY autovacuum_count DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY analyze_count DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY heap_blks_read DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_blks_hit DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY toast_blks_read DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY toast_blks_hit DESC NULLS LAST) < $1) OR 
		(row_number() OVER (ORDER BY pg_table_size(s1.relid) DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY reltuples DESC NULLS LAST) < $1) AS visible 
		FROM pg_stat_user_tables s1 JOIN pg_statio_user_tables s2 USING (schemaname, relname) 
		JOIN pg_class c ON s1.relid = c.oid WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.relid AND mode = 'AccessExclusiveLock' AND granted)) 
		SELECT current_database() AS database, schema, \"table\", seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, 
		n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, last_vacuum_seconds, last_analyze_seconds, last_vacuum_time, last_analyze_time, 
		vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read, 
		toast_blks_hit, tidx_blks_read, tidx_blks_hit, size_bytes, reltuples FROM stat WHERE visible UNION ALL (SELECT current_database() AS database, 
		'all_shemas', 'all_other_tables', NULLIF(SUM(COALESCE(seq_scan,0)),0), NULLIF(SUM(COALESCE(seq_tup_read,0)),0), NULLIF(SUM(COALESCE(idx_scan,0)),0), 
		NULLIF(SUM(COALESCE(idx_tup_fetch,0)),0), NULLIF(SUM(COALESCE(n_tup_ins,0)),0), NULLIF(SUM(COALESCE(n_tup_upd,0)),0), 
		NULLIF(SUM(COALESCE(n_tup_del,0)),0), NULLIF(SUM(COALESCE(n_tup_hot_upd,0)),0), NULLIF(SUM(COALESCE(n_live_tup,0)),0), 
		NULLIF(SUM(COALESCE(n_dead_tup,0)),0), NULLIF(SUM(COALESCE(n_mod_since_analyze,0)),0), NULL, NULL, NULL, NULL, 
		NULLIF(SUM(COALESCE(vacuum_count,0)),0), NULLIF(SUM(COALESCE(autovacuum_count,0)),0), NULLIF(SUM(COALESCE(analyze_count,0)),0), 
		NULLIF(SUM(COALESCE(autoanalyze_count,0)),0), NULLIF(SUM(COALESCE(heap_blks_read,0)),0), NULLIF(SUM(COALESCE(heap_blks_hit,0)),0), 
		NULLIF(SUM(COALESCE(idx_blks_read,0)),0), NULLIF(SUM(COALESCE(idx_blks_hit,0)),0), NULLIF(SUM(COALESCE(toast_blks_read,0)),0), 
		NULLIF(SUM(COALESCE(toast_blks_hit,0)),0), NULLIF(SUM(COALESCE(tidx_blks_read,0)),0), NULLIF(SUM(COALESCE(tidx_blks_hit, 0)),0), 
		NULLIF(SUM(COALESCE(size_bytes,0)),0), NULLIF(SUM(COALESCE(reltuples,0)),0) FROM stat 
		WHERE NOT visible HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible))";

#[derive(sqlx::FromRow, Debug)]
pub struct PGTablesStats {
    database: String,
    schema: String,
    table: String,
    seq_scan: i64,
    seq_tup_read: i64,
    idx_scan: i64,
    idx_tup_fetch: i64,
    n_tup_ins: i64,
    n_tup_upd: i64,
    n_tup_del: i64,
    n_tup_hot_upd: i64,
    n_live_tup: i64,
    n_dead_tup: i64,
    n_mod_since_analyze: i64,
    last_vacuum_seconds: Decimal,
    last_analyze_seconds: Decimal,
    last_vacuum_time: Decimal,
    last_analyze_time: Decimal,
    vacuum_count: i64,
    autovacuum_count: i64,
    analyze_count: i64,
    autoanalyze_count: i64,
    heap_blks_read: i64,
    heap_blks_hit: i64,
    idx_blks_read: i64,
    idx_blks_hit: i64,
    toast_blks_read: i64,
    toast_blks_hit: i64,
    tidx_blks_read: i64,
    tidx_blks_hit: i64,
    size_bytes: i64,
    reltuples: f64,
}

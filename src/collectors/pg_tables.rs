use rust_decimal::Decimal;

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
use tracing::error;

use crate::collectors::PG;
use crate::instance;

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
		'all_shemas', 'all_other_tables', NULLIF(SUM(COALESCE(seq_scan,0)),0)::INT8, NULLIF(SUM(COALESCE(seq_tup_read,0)),0)::INT8, NULLIF(SUM(COALESCE(idx_scan,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(idx_tup_fetch,0)),0)::INT8, NULLIF(SUM(COALESCE(n_tup_ins,0)),0)::INT8, NULLIF(SUM(COALESCE(n_tup_upd,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(n_tup_del,0)),0)::INT8, NULLIF(SUM(COALESCE(n_tup_hot_upd,0)),0)::INT8, NULLIF(SUM(COALESCE(n_live_tup,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(n_dead_tup,0)),0)::INT8, NULLIF(SUM(COALESCE(n_mod_since_analyze,0)),0)::INT8, NULL, NULL, NULL, NULL, 
		NULLIF(SUM(COALESCE(vacuum_count,0)),0)::INT8, NULLIF(SUM(COALESCE(autovacuum_count,0)),0)::INT8, NULLIF(SUM(COALESCE(analyze_count,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(autoanalyze_count,0)),0)::INT8, NULLIF(SUM(COALESCE(heap_blks_read,0)),0)::INT8, NULLIF(SUM(COALESCE(heap_blks_hit,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(idx_blks_read,0)),0)::INT8, NULLIF(SUM(COALESCE(idx_blks_hit,0)),0)::INT8, NULLIF(SUM(COALESCE(toast_blks_read,0)),0)::INT8, 
		NULLIF(SUM(COALESCE(toast_blks_hit,0)),0)::INT8, NULLIF(SUM(COALESCE(tidx_blks_read,0)),0)::INT8, NULLIF(SUM(COALESCE(tidx_blks_hit, 0)),0)::INT8, 
		NULLIF(SUM(COALESCE(size_bytes,0)),0)::INT8, NULLIF(SUM(COALESCE(reltuples,0)),0)::INT8 FROM stat 
		WHERE NOT visible HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible))";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGTablesStats {
    database: String,
    schema: String,
    table: String,
    seq_scan: Option<i64>,
    seq_tup_read: Option<i64>,
    idx_scan: Option<i64>,
    idx_tup_fetch: Option<i64>,
    n_tup_ins: Option<i64>,
    n_tup_upd: Option<i64>,
    n_tup_del: Option<i64>,
    n_tup_hot_upd: Option<i64>,
    n_live_tup: Option<i64>,
    n_dead_tup: Option<i64>,
    n_mod_since_analyze: Option<i64>,
    last_vacuum_seconds: Option<Decimal>,
    last_analyze_seconds: Option<Decimal>,
    last_vacuum_time: Option<Decimal>,
    last_analyze_time: Option<Decimal>,
    vacuum_count: Option<i64>,
    autovacuum_count: Option<i64>,
    analyze_count: Option<i64>,
    autoanalyze_count: Option<i64>,
    heap_blks_read: Option<i64>,
    heap_blks_hit: Option<i64>,
    idx_blks_read: Option<i64>,
    idx_blks_hit: Option<i64>,
    toast_blks_read: Option<i64>,
    toast_blks_hit: Option<i64>,
    tidx_blks_read: Option<i64>,
    tidx_blks_hit: Option<i64>,
    size_bytes: Option<i64>,
    reltuples: Option<f32>,
}

impl PGTablesStats {
    fn new() -> Self {
        Self::default()
    }
}

// PGTableCollector returns a new Collector exposing postgres tables stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
#[derive(Debug, Clone)]
pub struct PGTableCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGTablesStats>>>,
    descs: Vec<Desc>,
    seqscan: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGTableCollector> {
    match PGTableCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            error!("error when create pg tables collector: {}", e);
            None
        }
    }
}

impl PGTableCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGTablesStats::new()]));

        let seqscan = IntGaugeVec::new(
            Opts::new(
                "seq_scan_total",
                "The total number of sequential scans have been done.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(seqscan.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            seqscan,
        })
    }
}

impl Collector for PGTableCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg tables collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        for row in data_lock.iter() {
            self.seqscan
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.seq_scan.unwrap_or_default());
        }

        mfs.extend(self.seqscan.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGTableCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let mut pg_tables_stat_rows = if self.dbi.cfg.pg_collect_top_table > 0 {
            sqlx::query_as::<_, PGTablesStats>(POSTGRES_USERS_TABLE_TOPK)
                .bind(self.dbi.cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGTablesStats>(POSTGRES_USERS_TABLE)
                .fetch_all(&self.dbi.db)
                .await?
        };

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg tables collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();
        data_lock.append(&mut pg_tables_stat_rows);

        Ok(())
    }
}

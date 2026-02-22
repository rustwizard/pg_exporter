use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntGaugeVec, proto};
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
		pg_table_size(s1.relid) AS size_bytes, reltuples::FLOAT8
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
		toast_blks_hit, tidx_blks_read, tidx_blks_hit, size_bytes, reltuples::FLOAT8 FROM stat WHERE visible UNION ALL (SELECT current_database() AS database,
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
    reltuples: Option<f64>,
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
    seqtupread: IntGaugeVec,
    idxscan: IntGaugeVec,
    idxtupfetch: IntGaugeVec,
    tup_inserted: IntGaugeVec,
    tup_updated: IntGaugeVec,
    tup_hot_updated: IntGaugeVec,
    tup_deleted: IntGaugeVec,
    tup_live: IntGaugeVec,
    tup_dead: IntGaugeVec,
    tup_modified: IntGaugeVec,
    maint_last_vacuum_age: IntGaugeVec,
    maint_last_analyze_age: IntGaugeVec,
    maint_last_vacuum_time: IntGaugeVec,
    maint_last_analyze_time: IntGaugeVec,
    maintenance: IntGaugeVec,
    io: IntGaugeVec,
    sizes: IntGaugeVec,
    reltuples: GaugeVec,
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

        let seqtupread = IntGaugeVec::new(
            Opts::new(
                "seq_tup_read_total",
                "The total number of tuples have been read by sequential scans.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(seqtupread.desc().into_iter().cloned());

        let idxscan = IntGaugeVec::new(
            Opts::new(
                "idx_scan_total",
                "Total number of index scans initiated on this table.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(idxscan.desc().into_iter().cloned());

        let idxtupfetch = IntGaugeVec::new(
            Opts::new(
                "idx_tup_fetch_total",
                "Total number of live rows fetched by index scans.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(idxtupfetch.desc().into_iter().cloned());

        let tup_inserted = IntGaugeVec::new(
            Opts::new(
                "tuples_inserted_total",
                "Total number of tuples (rows) have been inserted in the table.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_inserted.desc().into_iter().cloned());

        let tup_updated = IntGaugeVec::new(
            Opts::new(
                "tuples_updated_total",
                "Total number of tuples (rows) have been updated in the table (including HOT).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_updated.desc().into_iter().cloned());

        let tup_hot_updated = IntGaugeVec::new(
            Opts::new(
                "tuples_hot_updated_total",
                "Total number of tuples (rows) have been updated in the table (HOT only).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_hot_updated.desc().into_iter().cloned());

        let tup_deleted = IntGaugeVec::new(
            Opts::new(
                "tuples_deleted_total",
                "Total number of tuples (rows) have been deleted in the table.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_deleted.desc().into_iter().cloned());

        let tup_live = IntGaugeVec::new(
            Opts::new(
                "tuples_live_total",
                "Estimated total number of live tuples in the table.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_live.desc().into_iter().cloned());

        let tup_dead = IntGaugeVec::new(
            Opts::new(
                "tuples_dead_total",
                "Estimated total number of dead tuples in the table.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_dead.desc().into_iter().cloned());

        let tup_modified = IntGaugeVec::new(
            Opts::new(
                "tuples_modified_total",
                "Estimated total number of modified tuples in the table since last vacuum.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(tup_modified.desc().into_iter().cloned());

        let maint_last_vacuum_age = IntGaugeVec::new(
            Opts::new(
                "since_last_vacuum_seconds_total",
                "Total time since table was vacuumed manually or automatically (not counting VACUUM FULL), in seconds. DEPRECATED.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(maint_last_vacuum_age.desc().into_iter().cloned());

        let maint_last_analyze_age = IntGaugeVec::new(
            Opts::new(
                "since_last_analyze_seconds_total",
                "Total time since table was analyzed manually or automatically, in seconds. DEPRECATED.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(maint_last_analyze_age.desc().into_iter().cloned());

        let maint_last_vacuum_time = IntGaugeVec::new(
            Opts::new(
                "last_vacuum_time",
                "Time of last vacuum or autovacuum has been done (not counting VACUUM FULL), in unixtime.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(maint_last_vacuum_time.desc().into_iter().cloned());

        let maint_last_analyze_time = IntGaugeVec::new(
            Opts::new(
                "last_analyze_time",
                "Time of last analyze or autoanalyze has been done, in unixtime.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(maint_last_analyze_time.desc().into_iter().cloned());

        let maintenance = IntGaugeVec::new(
            Opts::new(
                "maintenance_total",
                "Total number of times this table has been maintained by each type of maintenance operation.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "type"],
        )?;
        descs.extend(maintenance.desc().into_iter().cloned());

        let io = IntGaugeVec::new(
            Opts::new("blocks_total", "Total number of table's blocks processed.")
                .namespace(super::NAMESPACE)
                .subsystem("table")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "type", "access"],
        )?;
        descs.extend(io.desc().into_iter().cloned());

        let sizes = IntGaugeVec::new(
            Opts::new(
                "size_bytes",
                "Total size of the table (including all forks and TOASTed data), in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(sizes.desc().into_iter().cloned());

        let reltuples = GaugeVec::new(
            Opts::new(
                "tuples_total",
                "Number of rows in the table based on pg_class.reltuples value.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("table")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table"],
        )?;
        descs.extend(reltuples.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            seqscan,
            seqtupread,
            idxscan,
            idxtupfetch,
            tup_inserted,
            tup_updated,
            tup_hot_updated,
            tup_deleted,
            tup_live,
            tup_dead,
            tup_modified,
            maint_last_vacuum_age,
            maint_last_analyze_age,
            maint_last_vacuum_time,
            maint_last_analyze_time,
            maintenance,
            io,
            sizes,
            reltuples,
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
            // scan stats
            self.seqscan
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.seq_scan.unwrap_or_default());

            self.seqtupread
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.seq_tup_read.unwrap_or_default());

            self.idxscan
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.idx_scan.unwrap_or_default());

            self.idxtupfetch
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.idx_tup_fetch.unwrap_or_default());

            // tuples stats
            self.tup_inserted
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_tup_ins.unwrap_or_default());

            self.tup_updated
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_tup_upd.unwrap_or_default());

            self.tup_hot_updated
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_tup_hot_upd.unwrap_or_default());

            self.tup_deleted
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_tup_del.unwrap_or_default());

            // tuples total stats
            self.tup_live
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_live_tup.unwrap_or_default());

            self.tup_dead
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_dead_tup.unwrap_or_default());

            self.tup_modified
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.n_mod_since_analyze.unwrap_or_default());

            // maintenance stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
            let last_vacuum_seconds = row
                .last_vacuum_seconds
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if last_vacuum_seconds > 0 {
                self.maint_last_vacuum_age
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                    ])
                    .set(last_vacuum_seconds);
            }

            let last_analyze_seconds = row
                .last_analyze_seconds
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if last_analyze_seconds > 0 {
                self.maint_last_analyze_age
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                    ])
                    .set(last_analyze_seconds);
            }

            let last_vacuum_time = row
                .last_vacuum_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if last_vacuum_time > 0 {
                self.maint_last_vacuum_time
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                    ])
                    .set(last_vacuum_time);
            }

            let last_analyze_time = row
                .last_analyze_time
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if last_analyze_time > 0 {
                self.maint_last_analyze_time
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                    ])
                    .set(last_analyze_time);
            }

            let vacuum = row
                .vacuum_count
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if vacuum > 0 {
                self.maintenance
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "vacuum",
                    ])
                    .set(vacuum);
            }

            let autovacuum = row
                .autovacuum_count
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if autovacuum > 0 {
                self.maintenance
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "autovacuum",
                    ])
                    .set(autovacuum);
            }

            let analyze = row
                .analyze_count
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if analyze > 0 {
                self.maintenance
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "analyze",
                    ])
                    .set(analyze);
            }

            let autoanalyze = row
                .autoanalyze_count
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if autoanalyze > 0 {
                self.maintenance
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "autoanalyze",
                    ])
                    .set(autoanalyze);
            }

            // io stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
            let heapread = row
                .heap_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if heapread > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "heap",
                        "read",
                    ])
                    .set(heapread);
            }

            let heaphit = row
                .heap_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if heapread > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "heap",
                        "hit",
                    ])
                    .set(heaphit);
            }

            let idxread = row
                .idx_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if idxread > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "idx",
                        "read",
                    ])
                    .set(idxread);
            }

            let idxhit = row
                .idx_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if idxhit > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "idx",
                        "hit",
                    ])
                    .set(idxhit);
            }

            let toastread = row
                .toast_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if toastread > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "toast",
                        "read",
                    ])
                    .set(toastread);
            }

            let toasthit = row
                .toast_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if toasthit > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "toast",
                        "hit",
                    ])
                    .set(toasthit);
            }

            let tidxread = row
                .tidx_blks_read
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if tidxread > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "tidx",
                        "read",
                    ])
                    .set(tidxread);
            }

            let tidxhit = row
                .tidx_blks_hit
                .unwrap_or_default()
                .to_i64()
                .unwrap_or_default();

            if tidxhit > 0 {
                self.io
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        "tidx",
                        "hit",
                    ])
                    .set(tidxhit);
            }

            self.sizes
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.size_bytes.unwrap_or_default());

            self.reltuples
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                ])
                .set(row.reltuples.unwrap_or_default());
        }

        mfs.extend(self.seqscan.collect());
        mfs.extend(self.seqtupread.collect());
        mfs.extend(self.idxscan.collect());
        mfs.extend(self.idxtupfetch.collect());
        mfs.extend(self.tup_inserted.collect());
        mfs.extend(self.tup_updated.collect());
        mfs.extend(self.tup_hot_updated.collect());
        mfs.extend(self.tup_deleted.collect());
        mfs.extend(self.tup_live.collect());
        mfs.extend(self.tup_dead.collect());
        mfs.extend(self.tup_modified.collect());
        mfs.extend(self.maint_last_vacuum_age.collect());
        mfs.extend(self.maint_last_analyze_age.collect());
        mfs.extend(self.maint_last_vacuum_time.collect());
        mfs.extend(self.maint_last_analyze_age.collect());
        mfs.extend(self.maintenance.collect());
        mfs.extend(self.io.collect());
        mfs.extend(self.sizes.collect());
        mfs.extend(self.reltuples.collect());

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

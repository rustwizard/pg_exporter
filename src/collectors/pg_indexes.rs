use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntCounterVec};
use prometheus::{IntGaugeVec, proto};
use tracing::error;

use crate::collectors::PG;
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
		SELECT current_database() AS database, \"schema\", \"table\", \"index\", \"key\", isvalid, idx_scan::INT8, idx_tup_read::INT8, idx_tup_fetch::INT8,
		idx_blks_read::INT8, idx_blks_hit::INT8, size_bytes::INT8 FROM stat WHERE visible
		UNION ALL SELECT current_database() AS database, 'all_shemas', 'all_other_tables', 'all_other_indexes', true, null,
		NULLIF(SUM(COALESCE(idx_scan,0)),0)::INT8, NULLIF(SUM(COALESCE(idx_tup_fetch,0)),0)::INT8, NULLIF(SUM(COALESCE(idx_tup_read,0)),0)::INT8,
		NULLIF(SUM(COALESCE(idx_blks_read,0)),0)::INT8, NULLIF(SUM(COALESCE(idx_blks_hit,0)),0)::INT8,
		NULLIF(SUM(COALESCE(size_bytes,0)),0)::INT8 FROM stat WHERE NOT visible HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)";

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

// PGIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
#[derive(Debug, Clone)]
pub struct PGIndexesCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGIndexesStats>>>,
    descs: Vec<Desc>,
    indexes: IntGaugeVec,
    tuples: IntGaugeVec,
    io: IntCounterVec,
    sizes: GaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGIndexesCollector> {
    match PGIndexesCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            error!("error when create pg indexes collector: {}", e);
            None
        }
    }
}

impl PGIndexesCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGIndexesCollector> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGIndexesStats::new()]));

        let indexes = IntGaugeVec::new(
            Opts::new("scans_total", "Total number of index scans initiated.")
                .namespace(super::NAMESPACE)
                .subsystem("index")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "key", "isvalid"],
        )?;
        descs.extend(indexes.desc().into_iter().cloned());

        let tuples = IntGaugeVec::new(
            Opts::new(
                "tuples_total",
                "Total number of index entries processed by scans.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("index")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "tuples"],
        )?;
        descs.extend(tuples.desc().into_iter().cloned());

        let io = IntCounterVec::new(
            Opts::new("blocks_total", "Total number of indexes blocks processed.")
                .namespace(super::NAMESPACE)
                .subsystem("index_io")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "access"],
        )?;
        descs.extend(io.desc().into_iter().cloned());

        let sizes = GaugeVec::new(
            Opts::new("size_bytes", "Total size of the index, in bytes.")
                .namespace(super::NAMESPACE)
                .subsystem("index")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index"],
        )?;
        descs.extend(sizes.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            indexes,
            tuples,
            io,
            sizes,
        })
    }
}

impl Collector for PGIndexesCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg indexes collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        for row in data_lock.iter() {
            // always send idx scan metrics and indexes size
            self.indexes
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                    row.index.as_str(),
                    row.key.to_string().as_str(),
                    row.isvalid.to_string().as_str(),
                ])
                .set(row.idx_scan);

            self.sizes
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                    row.index.as_str(),
                ])
                .set(row.size_bytes as f64);

            // avoid metrics spamming and send metrics only if they greater than zero.
            if row.idx_tup_read > 0 {
                self.tuples
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        row.index.as_str(),
                        "read",
                    ])
                    .set(row.idx_tup_read);
            }
            if row.idx_tup_fetch > 0 {
                self.tuples
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        row.index.as_str(),
                        "fetched",
                    ])
                    .set(row.idx_tup_fetch);
            }
            if row.idx_blks_read > 0 {
                self.tuples
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        row.index.as_str(),
                        "read",
                    ])
                    .set(row.idx_blks_read);
            }
            if row.idx_blks_hit > 0 {
                self.tuples
                    .with_label_values(&[
                        row.database.as_str(),
                        row.schema.as_str(),
                        row.table.as_str(),
                        row.index.as_str(),
                        "hit",
                    ])
                    .set(row.idx_blks_hit);
            }
        }

        mfs.extend(self.indexes.collect());
        mfs.extend(self.sizes.collect());
        mfs.extend(self.io.collect());
        mfs.extend(self.tuples.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGIndexesCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let cfg = self.dbi.ensure_ready().await?;
        let mut pg_idx_stats_rows = if cfg.pg_collect_topidx > 0 {
            sqlx::query_as::<_, PGIndexesStats>(USER_INDEXES_QUERY_TOPK)
                .bind(cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGIndexesStats>(USER_INDEXES_QUERY)
                .fetch_all(&self.dbi.db)
                .await?
        };

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg indexes collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();

        data_lock.append(&mut pg_idx_stats_rows);

        Ok(())
    }
}

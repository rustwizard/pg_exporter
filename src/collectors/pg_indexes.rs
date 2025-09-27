use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;
use prometheus::{GaugeVec, IntCounterVec};

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

// NewPostgresIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
#[derive(Debug, Clone)]
pub struct PGIndexesCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGIndexesStats>>>,
    descs: Vec<Desc>,
    indexes: IntCounterVec,
    tuples: IntCounterVec,
    io: IntCounterVec,
    sizes: GaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGIndexesCollector> {
    Some(PGIndexesCollector::new(dbi))
}

impl PGIndexesCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> PGIndexesCollector {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGIndexesStats::new()]));

        let indexes = IntCounterVec::new(
            Opts::new("scans_total", "Total number of index scans initiated.")
                .namespace(super::NAMESPACE)
                .subsystem("index")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "key", "isvalid"],
        )
        .unwrap();
        descs.extend(indexes.desc().into_iter().cloned());

        let tuples = IntCounterVec::new(
            Opts::new(
                "tuples_total",
                "Total number of index entries processed by scans.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("index")
            .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "tuples"],
        )
        .unwrap();
        descs.extend(tuples.desc().into_iter().cloned());

        let io = IntCounterVec::new(
            Opts::new("blocks_total", "Total number of indexes blocks processed.")
                .namespace(super::NAMESPACE)
                .subsystem("index_io")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index", "access"],
        )
        .unwrap();
        descs.extend(io.desc().into_iter().cloned());

        let sizes = GaugeVec::new(
            Opts::new("size_bytes", "Total size of the index, in bytes.")
                .namespace(super::NAMESPACE)
                .subsystem("index")
                .const_labels(dbi.labels.clone()),
            &["database", "schema", "table", "index"],
        )
        .unwrap();
        descs.extend(sizes.desc().into_iter().cloned());

        Self {
            dbi,
            data,
            descs,
            indexes,
            tuples,
            io,
            sizes,
        }
    }
}

impl Collector for PGIndexesCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = self.data.read().expect("can't acuire lock");

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
                .inc_by(row.idx_scan as u64);

            self.sizes
                .with_label_values(&[
                    row.database.as_str(),
                    row.schema.as_str(),
                    row.table.as_str(),
                    row.index.as_str(),
                ])
                .set(row.size_bytes as f64);
        }

        mfs
    }
}

#[async_trait]
impl PG for PGIndexesCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

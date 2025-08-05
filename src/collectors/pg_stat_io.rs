use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Counter, CounterVec, IntCounter, IntGauge, IntGaugeVec};

use crate::collectors::{PG, POSTGRES_V16, POSTGRES_V18};
use crate::instance;

const POSTGRES_STAT_IO_QUERY17: &str = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time,
		COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, 
		COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, 
		COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, 
		COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, 
		COALESCE(reads, 0) * COALESCE(op_bytes, 0) AS read_bytes, 
		COALESCE(writes, 0) * COALESCE(op_bytes, 0) AS write_bytes, 
		COALESCE(extends, 0) * COALESCE(op_bytes, 0) AS extend_bytes 
		FROM pg_stat_io";

const POSTGRES_STAT_IO_LATEST: &str = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time, 
		COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, 
		COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, 
		COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, 
		COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, 
		COALESCE(read_bytes, 0) AS read_bytes, COALESCE(write_bytes, 0) AS write_bytes, COALESCE(extend_bytes, 0) AS extend_bytes 
		FROM pg_stat_io";

#[derive(sqlx::FromRow, Debug)]
pub struct PGStatIOStats {
    backend_type: String, // a backend type like "autovacuum worker"
    #[sqlx(rename = "object")]
    io_object: String,    // "relation" or "temp relation"
    #[sqlx(rename = "context")]
    io_context: String,   // "normal", "vacuum", "bulkread" or "bulkwrite"
    reads: i64,
    read_time: f64,
    writes: i64,
    write_time: f64,
    #[sqlx(rename = "writebacks")]
    write_backs: i64,
    writeback_time: f64,
    extends: i64,
    extend_time: f64,
    hits: i64,
    evictions: i64,
    reuses: i64,
    fsyncs: i64,
    fsync_time: f64,
    read_bytes: i64,
    write_bytes: i64,
    extend_bytes: i64,
}

impl PGStatIOStats {
    fn new() -> Self {
        PGStatIOStats {
            backend_type: String::new(),
            io_object: String::new(),
            io_context: String::new(),
            reads: 0,
            read_time: 0.0,
            writes: 0,
            write_time: 0.0,
            write_backs: 0,
            writeback_time: 0.0,
            extends: 0,
            extend_time: 0.0,
            hits: 0,
            evictions: 0,
            reuses: 0,
            fsyncs: 0,
            fsync_time: 0.0,
            read_bytes: 0,
            write_bytes: 0,
            extend_bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGStatIOCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGStatIOStats>>,
    descs: Vec<Desc>,
    reads: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStatIOCollector> {
    if dbi.cfg.pg_version >= POSTGRES_V16 {
        Some(PGStatIOCollector::new(dbi))
    } else {
        None
    }
    
}

impl PGStatIOCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> PGStatIOCollector {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(PGStatIOStats::new()));

        let var_labels = vec!["backend_type", "object", "context"];

        let reads = IntGaugeVec::new(
            Opts::new(
                "reads",
                "Number of read operations, each of the size specified in op_bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(reads.desc().into_iter().cloned());

        PGStatIOCollector {
            dbi,
            data,
            descs,
            reads,
        }
    }
}

impl Collector for PGStatIOCollector {
    fn desc(&self) -> std::vec::Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> std::vec::Vec<MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(16);

        let data_lock = self.data.read().expect("can't acuire lock");

        let vals = vec![
            data_lock.backend_type.as_str(),
            data_lock.io_object.as_str(),
            data_lock.io_context.as_str(),
        ];

        self.reads
            .with_label_values(vals.as_slice())
            .set(data_lock.reads);

        mfs
    }
}

#[async_trait]
impl PG for PGStatIOCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_pg_statio_stats = if self.dbi.cfg.pg_version < POSTGRES_V18 {
            sqlx::query_as::<_, PGStatIOStats>(POSTGRES_STAT_IO_QUERY17)
                .fetch_optional(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGStatIOStats>(POSTGRES_STAT_IO_LATEST)
                .fetch_optional(&self.dbi.db)
                .await?
        };

        if let Some(pg_statio_stats) = maybe_pg_statio_stats {
            let mut data_lock = match self.data.write() {
                Ok(data_lock) => data_lock,
                Err(e) => bail!("can't unwrap lock. {}", e)
            };

            data_lock.reads = pg_statio_stats.reads;
            
        }

        Ok(())
    }
}

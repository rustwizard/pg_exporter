use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, IntGaugeVec};

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
    io_object: String, // "relation" or "temp relation"
    #[sqlx(rename = "context")]
    io_context: String, // "normal", "vacuum", "bulkread" or "bulkwrite"
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
    data: Arc<RwLock<Vec<PGStatIOStats>>>,
    descs: Vec<Desc>,
    reads: IntGaugeVec,
    read_time: GaugeVec,
    writes: IntGaugeVec,
    write_time: GaugeVec,
    write_backs: IntGaugeVec,
    writeback_time: GaugeVec,
    extends: IntGaugeVec,
    extend_time: GaugeVec,
    hits: IntGaugeVec
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStatIOCollector> {
    // Collecting pg_stat_io since Postgres 16.
    if dbi.cfg.pg_version >= POSTGRES_V16 {
        Some(PGStatIOCollector::new(dbi))
    } else {
        None
    }
}

impl PGStatIOCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> PGStatIOCollector {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGStatIOStats::new()]));

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

        let read_time = GaugeVec::new(
            Opts::new(
                "read_time",
                "Time spent in read operations in milliseconds (if track_io_timing is enabled, otherwise zero)",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(read_time.desc().into_iter().cloned());

        let writes = IntGaugeVec::new(
            Opts::new(
                "writes",
                "Number of write operations, each of the size specified in op_bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(writes.desc().into_iter().cloned());

        let write_time = GaugeVec::new(
            Opts::new(
                "write_time",
                "Time spent in write operations in milliseconds (if track_io_timing is enabled, otherwise zero).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(write_time.desc().into_iter().cloned());

        let write_backs = IntGaugeVec::new(
            Opts::new(
                "writebacks",
                "Number of units of size op_bytes which the process requested the kernel write out to permanent storage.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(write_backs.desc().into_iter().cloned());

        let writeback_time = GaugeVec::new(
            Opts::new(
                "writeback_time",
                "Time spent in writeback operations in milliseconds (if track_io_timing is enabled, otherwise zero).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(writeback_time.desc().into_iter().cloned());

        let extends = IntGaugeVec::new(
            Opts::new(
                "extends",
                "Number of relation extend operations, each of the size specified in op_bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(extends.desc().into_iter().cloned());

        let extend_time = GaugeVec::new(
            Opts::new(
                "extend_time",
                "Time spent in extend operations in milliseconds (if track_io_timing is enabled, otherwise zero)",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(extend_time.desc().into_iter().cloned());

        let hits = IntGaugeVec::new(
            Opts::new(
                "hits",
                "The number of times a desired block was found in a shared buffer.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_io")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )
        .unwrap();
        descs.extend(hits.desc().into_iter().cloned());

        PGStatIOCollector {
            dbi,
            data,
            descs,
            reads,
            read_time,
            writes,
            write_time,
            write_backs,
            writeback_time,
            extends,
            extend_time,
            hits
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

        for row in data_lock.iter() {
            let vals = vec![
                row.backend_type.as_str(),
                row.io_object.as_str(),
                row.io_context.as_str(),
            ];

            self.reads.with_label_values(vals.as_slice()).set(row.reads);
            self.read_time.with_label_values(vals.as_slice()).set(row.read_time);
            self.writes.with_label_values(vals.as_slice()).set(row.writes);
            self.write_time.with_label_values(vals.as_slice()).set(row.write_time);
            self.write_backs.with_label_values(vals.as_slice()).set(row.write_backs);
            self.writeback_time.with_label_values(vals.as_slice()).set(row.writeback_time);
            self.extends.with_label_values(vals.as_slice()).set(row.extends);
            self.extend_time.with_label_values(vals.as_slice()).set(row.extend_time);
            self.hits.with_label_values(vals.as_slice()).set(row.hits);

        }

        mfs.extend(self.reads.collect());
        mfs.extend(self.read_time.collect());
        mfs.extend(self.writes.collect());
        mfs.extend(self.write_time.collect());
        mfs.extend(self.write_backs.collect());
        mfs.extend(self.writeback_time.collect());
        mfs.extend(self.extends.collect());
        mfs.extend(self.extend_time.collect());
        mfs.extend(self.hits.collect());


        mfs
    }
}

#[async_trait]
impl PG for PGStatIOCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let mut pg_statio_stats_rows = if self.dbi.cfg.pg_version < POSTGRES_V18 {
            sqlx::query_as::<_, PGStatIOStats>(POSTGRES_STAT_IO_QUERY17)
                .fetch_all(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGStatIOStats>(POSTGRES_STAT_IO_LATEST)
                .fetch_all(&self.dbi.db)
                .await?
        };

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("can't unwrap lock. {}", e),
        };

        data_lock.clear();

        data_lock.append(&mut pg_statio_stats_rows);

        Ok(())
    }
}

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;
use bigdecimal::{BigDecimal, FromPrimitive};
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Counter, IntCounter, IntGauge};

use crate::collectors::{PG, POSTGRES_V10, POSTGRES_V14};
use crate::instance;

const POSTGRES_WAL_QUERY96: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_xlog_receive_location() ELSE pg_current_xlog_location() END) - '0/00000000' AS wal_written";

const POSTGRES_WAL_QUERY13: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END) - '0/00000000' AS wal_written";

const POSTGRES_WAL_QUERY_LATEST: &str =
    "SELECT pg_is_in_recovery()::int AS recovery, wal_records, wal_fpi, 
		(CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_receive_lsn() - '0/00000000' ELSE pg_current_wal_lsn() - '0/00000000' END) AS wal_written, 
		wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time, wal_sync_time, extract('epoch' from stats_reset) as reset_time 
		FROM pg_stat_wal";

#[derive(sqlx::FromRow, Debug)]
pub struct PGWALStats {
    recovery: i64,
    wal_records: i64,
    wal_fpi: i64,
    wal_written: BigDecimal,
    wal_bytes: BigDecimal,
    wal_buffers_full: i64,
    wal_write: i64,
    wal_sync: i64,
    wal_write_time: f64,
    reset_time: BigDecimal,
}

impl PGWALStats {
    fn new() -> Self {
        PGWALStats {
            recovery: (0),
            wal_records: (0),
            wal_fpi: (0),
            wal_written: BigDecimal::from_i64(0).unwrap(),
            wal_bytes: BigDecimal::from_i64(0).unwrap(),
            wal_buffers_full: (0),
            wal_write: (0),
            wal_sync: (0),
            wal_write_time: (0.0),
            reset_time: BigDecimal::from_i64(0).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGWALCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGWALStats>>,
    descs: Vec<Desc>,
    recovery_info: IntGauge,
    records_total: IntCounter,
    fpi_total: IntCounter,
    bytes_total: Counter,
    written_bytes_total: Counter,
    buffers_full_total: IntCounter,
    write_total: IntCounter,
    sync_total: IntCounter,
    seconds_all_total: Counter,
    seconds_total: Counter,
    stats_reset_time: IntGauge,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> PGWALCollector {
    PGWALCollector::new(dbi)
}

impl PGWALCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> PGWALCollector {
        let mut descs = Vec::new();

        let data = Arc::new(RwLock::new(PGWALStats::new()));

        let recovery_info = IntGauge::with_opts(
            Opts::new(
                "info",
                "Current recovery state, 0 - not in recovery; 1 - in recovery.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("recovery")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(recovery_info.desc().into_iter().cloned());

        let records_total = IntCounter::with_opts(
            Opts::new(
                "records_total",
                "Total number of WAL records generated (zero in case of standby).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(records_total.desc().into_iter().cloned());

        let fpi_total = IntCounter::with_opts(
            Opts::new(
                "fpi_total",
                "Total number of WAL full page images generated (zero in case of standby).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(fpi_total.desc().into_iter().cloned());

        let bytes_total = Counter::with_opts(
            Opts::new(
                "bytes_total",
                "Total amount of WAL generated (zero in case of standby) since last stats reset, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(bytes_total.desc().into_iter().cloned());

        let written_bytes_total = Counter::with_opts(
            Opts::new(
                "written_bytes_total",
                "Total amount of WAL written (or received in case of standby) since cluster init, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(written_bytes_total.desc().into_iter().cloned());

        let buffers_full_total = IntCounter::with_opts(
            Opts::new(
                "buffers_full_total",
                "Total number of times WAL data was written to disk because WAL buffers became full (zero in case of standby).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(buffers_full_total.desc().into_iter().cloned());

        let write_total = IntCounter::with_opts(
            Opts::new(
                "write_total",
                "Total number of times WAL buffers were written out to disk via XLogWrite request (zero in case of standby).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(write_total.desc().into_iter().cloned());

        let sync_total = IntCounter::with_opts(
            Opts::new(
                "sync_total",
                "Total number of times WAL files were synced to disk via issue_xlog_fsync request (zero in case of standby).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(sync_total.desc().into_iter().cloned());

        let seconds_all_total = Counter::with_opts(
            Opts::new(
                "seconds_all_total",
                "Total amount of time spent processing WAL buffers (zero in case of standby), in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(seconds_all_total.desc().into_iter().cloned());

        let seconds_total = Counter::with_opts(
            Opts::new(
                "seconds_total",
                "Total amount of time spent processing WAL buffers (zero in case of standby), in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(seconds_total.desc().into_iter().cloned());

        let seconds_total = Counter::with_opts(
            Opts::new(
                "seconds_total",
                "Total amount of time spent processing WAL buffers by each operation (zero in case of standby), in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(seconds_total.desc().into_iter().cloned());

        let stats_reset_time = IntGauge::with_opts(
            Opts::new(
                "stats_reset_time",
                "Time at which WAL statistics were last reset, in unixtime.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(stats_reset_time.desc().into_iter().cloned());

        PGWALCollector {
            dbi,
            data,
            descs,
            recovery_info,
            records_total,
            fpi_total,
            bytes_total,
            written_bytes_total,
            buffers_full_total,
            write_total,
            sync_total,
            seconds_all_total,
            seconds_total,
            stats_reset_time,
        }
    }
}

impl Collector for PGWALCollector {
    fn desc(&self) -> std::vec::Vec<&Desc> {
        todo!()
    }
    fn collect(&self) -> std::vec::Vec<MetricFamily> {
        todo!()
    }
}

#[async_trait]
impl PG for PGWALCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_pg_wal_stats = if self.dbi.cfg.pg_version < POSTGRES_V10 {
            sqlx::query_as::<_, PGWALStats>(POSTGRES_WAL_QUERY96)
                .fetch_optional(&self.dbi.db)
                .await?
        } else if self.dbi.cfg.pg_version < POSTGRES_V14 {
            sqlx::query_as::<_, PGWALStats>(POSTGRES_WAL_QUERY13)
                .fetch_optional(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGWALStats>(POSTGRES_WAL_QUERY_LATEST)
                .fetch_optional(&self.dbi.db)
                .await?
        };

        if let Some(pg_wal_stats) = maybe_pg_wal_stats {
            let mut data_lock = match self.data.write() {
                Ok(data_lock) => data_lock,
                Err(e) => bail!("can't unwrap lock. {}", e)
            };

            data_lock.recovery = pg_wal_stats.recovery;
            data_lock.reset_time = pg_wal_stats.reset_time;
            data_lock.wal_buffers_full = pg_wal_stats.wal_buffers_full;
            data_lock.wal_bytes = pg_wal_stats.wal_bytes;
            data_lock.wal_fpi = pg_wal_stats.wal_fpi;
            data_lock.wal_records = pg_wal_stats.wal_records;
            data_lock.wal_sync = pg_wal_stats.wal_sync;
            data_lock.wal_write = pg_wal_stats.wal_write;
            data_lock.wal_write_time = pg_wal_stats.wal_write_time;
            data_lock.wal_written = pg_wal_stats.wal_written;
        }

        Ok(())
    }
}

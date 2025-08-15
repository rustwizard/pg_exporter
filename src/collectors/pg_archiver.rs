use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Gauge, IntCounter, IntGauge};

use crate::collectors::{PG, POSTGRES_V12};
use crate::instance;

const POSTGRES_WAL_ARCHIVING_QUERY: &str = "SELECT archived_count, failed_count, 
	EXTRACT(EPOCH FROM now() - last_archived_time)::FLOAT8 AS since_last_archive_seconds, 
	(SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name ~'.ready') AS lag_files 
	FROM pg_stat_archiver WHERE archived_count > 0";

#[derive(sqlx::FromRow, Debug)]
pub struct PGArchiverStats {
    #[sqlx(rename = "archived_count")]
    archived: i64,
    #[sqlx(rename = "failed_count")]
    failed: i64,
    #[sqlx(rename = "since_last_archive_seconds")]
    since_archived_seconds: f64,
    lag_files: i64,
}

impl PGArchiverStats {
    fn new() -> Self {
        Self {
            archived: 0,
            failed: 0,
            since_archived_seconds: 0.0,
            lag_files: 0,
        }
    }
}
#[derive(Debug, Clone)]
pub struct PGArchiverCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGArchiverStats>>>,
    descs: Vec<Desc>,
    archived_total: IntCounter,
    failed_total: IntCounter,
    since_last_archive_seconds: Gauge,
    lag_bytes: IntGauge,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGArchiverCollector> {
    // some system functions are not available, required Postgres 12 or newer
    if dbi.cfg.pg_version > POSTGRES_V12 {
        Some(PGArchiverCollector::new(dbi))
    } else {
        None
    }
}

impl PGArchiverCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> Self {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGArchiverStats::new()]));

        let archived_total = IntCounter::with_opts(
            Opts::new(
                "archived_total",
                "Total number of WAL segments had been successfully archived.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("archiver")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(archived_total.desc().into_iter().cloned());

        let failed_total = IntCounter::with_opts(
            Opts::new(
                "failed_total",
                "Total number of attempts when WAL segments had been failed to archive.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("archiver")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(failed_total.desc().into_iter().cloned());

        let since_last_archive_seconds = Gauge::with_opts(
            Opts::new(
                "since_last_archive_seconds",
                "Number of seconds since last WAL segment had been successfully archived.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("archiver")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(since_last_archive_seconds.desc().into_iter().cloned());

        let lag_bytes = IntGauge::with_opts(
            Opts::new(
                "lag_bytes",
                "Amount of WAL segments ready, but not archived, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("archiver")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(lag_bytes.desc().into_iter().cloned());

        Self {
            dbi,
            data,
            descs,
            archived_total,
            failed_total,
            since_last_archive_seconds,
            lag_bytes,
        }
    }
}

impl Collector for PGArchiverCollector {
    fn desc(&self) -> std::vec::Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> std::vec::Vec<MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = self.data.read().expect("can't acuire lock");
        for row in data_lock.iter() {
            self.archived_total.inc_by(row.archived as u64);
            self.failed_total.inc_by(row.failed as u64);
            self.since_last_archive_seconds
                .set(row.since_archived_seconds);
            self.lag_bytes
                .set(row.lag_files * self.dbi.cfg.pg_wal_segment_size);
        }

        mfs.extend(self.archived_total.collect());
        mfs.extend(self.failed_total.collect());
        mfs.extend(self.since_last_archive_seconds.collect());
        mfs.extend(self.lag_bytes.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGArchiverCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let mut pg_archiver_stats_rows =
            sqlx::query_as::<_, PGArchiverStats>(POSTGRES_WAL_ARCHIVING_QUERY)
                .fetch_all(&self.dbi.db)
                .await?;

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("can't unwrap lock. {}", e),
        };

        data_lock.clear();

        data_lock.append(&mut pg_archiver_stats_rows);

        Ok(())
    }
}

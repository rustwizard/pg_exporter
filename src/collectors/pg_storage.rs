use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
use tracing::{error, info};

use crate::collectors::{PG, POSTGRES_V10, POSTGRES_V12};
use crate::instance;

use sqlx::Row;

const POSTGRES_TEMP_FILES_INFLIGHT: &str = "SELECT ts.spcname AS tablespace, COALESCE(COUNT(size), 0) AS files_total, COALESCE(sum(size), 0) AS bytes_total,
		COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(modification)), 0) AS max_age_seconds
		FROM pg_tablespace ts LEFT JOIN (SELECT spcname,(pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') ls ON ls.spcname = ts.spcname
		WHERE ts.spcname != 'pg_global' GROUP BY ts.spcname";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGStorageStats {
    tablespace: Option<String>,
    files_total: Option<i64>,
    bytes_total: Option<Decimal>,
    max_age_seconds: Option<Decimal>,
}
#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGDirStats {
    waldir_path: Option<String>,
    waldir_size_bytes: Option<Decimal>,
    waldir_files_count: Option<i64>,
    tmpfiles_size_bytes: Option<Decimal>,
    tmpfiles_count: Option<i64>,
}

// PGStorageCollector exposing various stats related to Postgres storage layer.
// This stats observed using different stats sources.
#[derive(Debug, Clone)]
pub struct PGStorageCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGStorageStats>>>,
    data_dirstat: Arc<RwLock<PGDirStats>>,
    descs: Vec<Desc>,
    temp_files: IntGaugeVec,
    temp_bytes: IntGaugeVec,
    temp_files_max_age: IntGaugeVec,
    wal_dir_bytes: IntGaugeVec,
    wal_dir_files: IntGaugeVec,
    tmp_files_bytes: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStorageCollector> {
    // Collecting pg_storage since Postgres 10.
    if dbi.current_cfg().map(|c| c.pg_version).unwrap_or(POSTGRES_V10) >= POSTGRES_V10 {
        match PGStorageCollector::new(dbi) {
            Ok(result) => Some(result),
            Err(e) => {
                error!("error when create pg storage collector: {}", e);
                None
            }
        }
    } else {
        info!("some server-side functions are not available, required Postgres 10 or newer");
        None
    }
}

impl PGStorageCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGStorageStats::default()]));

        let data_dirstat = Arc::new(RwLock::new(PGDirStats::default()));

        let temp_files = IntGaugeVec::new(
            Opts::new(
                "in_flight",
                "Number of temporary files processed in flight.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("temp_files")
            .const_labels(dbi.labels.clone()),
            &["tablespace"],
        )?;
        descs.extend(temp_files.desc().into_iter().cloned());

        let temp_bytes = IntGaugeVec::new(
            Opts::new(
                "in_flight",
                "Number of bytes occupied by temporary files processed in flight.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("temp_bytes")
            .const_labels(dbi.labels.clone()),
            &["tablespace"],
        )?;
        descs.extend(temp_bytes.desc().into_iter().cloned());

        let temp_files_max_age = IntGaugeVec::new(
            Opts::new(
                "max_age_seconds",
                "The age of the oldest temporary file, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("temp_bytes")
            .const_labels(dbi.labels.clone()),
            &["tablespace"],
        )?;
        descs.extend(temp_files_max_age.desc().into_iter().cloned());

        let wal_dir_bytes = IntGaugeVec::new(
            Opts::new(
                "bytes",
                "The size of Postgres server WAL directory, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal_directory")
            .const_labels(dbi.labels.clone()),
            &["device", "mountpoint", "path"],
        )?;
        descs.extend(wal_dir_bytes.desc().into_iter().cloned());

        let wal_dir_files = IntGaugeVec::new(
            Opts::new(
                "files",
                "The number of files in Postgres server WAL directory.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("wal_directory")
            .const_labels(dbi.labels.clone()),
            &["device", "mountpoint", "path"],
        )?;
        descs.extend(wal_dir_files.desc().into_iter().cloned());

        let tmp_files_bytes = IntGaugeVec::new(
            Opts::new(
                "bytes",
                "The size of all Postgres temp directories, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("temp_files_all")
            .const_labels(dbi.labels.clone()),
            &["device", "mountpoint", "path"],
        )?;
        descs.extend(tmp_files_bytes.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            data_dirstat,
            descs,
            temp_files,
            temp_bytes,
            temp_files_max_age,
            wal_dir_bytes,
            wal_dir_files,
            tmp_files_bytes,
        })
    }
}

impl Collector for PGStorageCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg storage collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        let dirstat_lock = match self.data_dirstat.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg storage collect: can't acquire dirstat read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        let pg_version = self.dbi.current_cfg().map(|c| c.pg_version).unwrap_or(0);

        for row in data_lock.iter() {
            let tablespace = row.tablespace.clone().unwrap_or_default();

            // Collecting in-flight temp only since Postgres 12.
            if pg_version >= POSTGRES_V12 {
                self.temp_files.with_label_values(&[&tablespace]).set(
                    row.files_total
                        .unwrap_or_default()
                        .to_i64()
                        .unwrap_or_default(),
                );

                self.temp_bytes.with_label_values(&[&tablespace]).set(
                    row.bytes_total
                        .unwrap_or_default()
                        .to_i64()
                        .unwrap_or_default(),
                );

                self.temp_files_max_age
                    .with_label_values(&[&tablespace])
                    .set(
                        row.max_age_seconds
                            .unwrap_or_default()
                            .to_i64()
                            .unwrap_or_default(),
                    );
            }
        }

        let waldir_path = dirstat_lock.waldir_path.clone().unwrap_or_default();
        self.wal_dir_bytes
            .with_label_values(&["unknown", "unknown", &waldir_path])
            .set(
                dirstat_lock
                    .waldir_size_bytes
                    .unwrap_or_default()
                    .to_i64()
                    .unwrap_or_default(),
            );

        let waldir_files_count = dirstat_lock.waldir_files_count.unwrap_or_default();
        self.wal_dir_files
            .with_label_values(&["unknown", "unknown", &waldir_path])
            .set(waldir_files_count);

        let tmp_files_bytes = dirstat_lock
            .tmpfiles_size_bytes
            .unwrap_or_default()
            .to_i64()
            .unwrap_or_default();
        self.tmp_files_bytes
            .with_label_values(&["temp", "temp", "temp"])
            .set(tmp_files_bytes);

        mfs.extend(self.temp_files.collect());
        mfs.extend(self.temp_bytes.collect());
        mfs.extend(self.temp_files_max_age.collect());
        mfs.extend(self.wal_dir_bytes.collect());
        mfs.extend(self.wal_dir_files.collect());
        mfs.extend(self.tmp_files_bytes.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGStorageCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let cfg = self.dbi.ensure_ready().await?;
        let mut pg_storage_stat_rows =
            sqlx::query_as::<_, PGStorageStats>(POSTGRES_TEMP_FILES_INFLIGHT)
                .bind(cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?;

        let waldir_row = sqlx::query("SELECT current_setting('data_directory')||'/pg_wal' AS path, COALESCE(sum(size), 0) AS bytes, COALESCE(count(name), 0) AS count FROM pg_ls_waldir()")
            .fetch_one(&self.dbi.db)
            .await?;

        let wal_path: String = waldir_row.try_get("path")?;
        let wal_bytes: Decimal = waldir_row.try_get("bytes")?;
        let wal_count: i64 = waldir_row.try_get("count")?;

        let tmpdir_row = sqlx::query("SELECT coalesce(sum(size), 0) AS bytes, coalesce(count(name), 0) AS count FROM (SELECT (pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') tablespaces")
        .fetch_one(&self.dbi.db)
        .await?;

        let tmpdir_bytes: Decimal = tmpdir_row.try_get("bytes")?;
        let tmpdir_count: i64 = tmpdir_row.try_get("count")?;

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg storage collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();
        data_lock.append(&mut pg_storage_stat_rows);

        let mut dirstat_lock = match self.data_dirstat.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!(
                "pg storage collector: can't acquire dirstat write lock. {}",
                e
            ),
        };

        dirstat_lock.tmpfiles_count = Some(tmpdir_count);
        dirstat_lock.tmpfiles_size_bytes = Some(tmpdir_bytes);
        dirstat_lock.waldir_files_count = Some(wal_count);
        dirstat_lock.waldir_path = Some(wal_path);
        dirstat_lock.waldir_size_bytes = Some(wal_bytes);

        Ok(())
    }
}

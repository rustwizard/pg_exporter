use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use prometheus::IntGaugeVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;

use crate::collectors::{PG, POSTGRES_V13, RwLockExt};
use crate::instance;

const POSTGRES_STAT_SLRU_QUERY: &str = "SELECT name, COALESCE(blks_zeroed, 0) AS blks_zeroed, COALESCE(blks_hit, 0) AS blks_hit, \
     COALESCE(blks_read, 0) AS blks_read, COALESCE(blks_written, 0) AS blks_written, \
     COALESCE(blks_exists, 0) AS blks_exists, COALESCE(flushes, 0) AS flushes, \
     COALESCE(truncates, 0) AS truncates FROM pg_stat_slru";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGStatSlruStats {
    name: String,
    blks_zeroed: i64,
    blks_hit: i64,
    blks_read: i64,
    blks_written: i64,
    blks_exists: i64,
    flushes: i64,
    truncates: i64,
}

#[derive(Debug, Clone)]
pub struct PGStatSlruCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGStatSlruStats>>>,
    descs: Vec<Desc>,
    blks_zeroed: IntGaugeVec,
    blks_hit: IntGaugeVec,
    blks_read: IntGaugeVec,
    blks_written: IntGaugeVec,
    blks_exists: IntGaugeVec,
    flushes: IntGaugeVec,
    truncates: IntGaugeVec,
}

crate::collector_new!(dbi, "pg stat_slru", PGStatSlruCollector, {
    dbi.current_cfg()
        .map(|c| c.pg_version)
        .unwrap_or(POSTGRES_V13)
        >= POSTGRES_V13
});

impl PGStatSlruCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGStatSlruCollector> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(Vec::<PGStatSlruStats>::new()));

        let var_labels = vec!["name"];

        let blks_zeroed = IntGaugeVec::new(
            Opts::new(
                "blks_zeroed",
                "Number of blocks zeroed during initializations.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_slru")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(blks_zeroed.desc().into_iter().cloned());

        let blks_hit = IntGaugeVec::new(
            Opts::new(
                "blks_hit",
                "Number of times disk blocks were found already in the SLRU, so that a read was not necessary \
                 (this only includes hits in the SLRU, not the operating system's file system cache).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_slru")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(blks_hit.desc().into_iter().cloned());

        let blks_read = IntGaugeVec::new(
            Opts::new("blks_read", "Number of disk blocks read for this SLRU.")
                .namespace(super::NAMESPACE)
                .subsystem("stat_slru")
                .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(blks_read.desc().into_iter().cloned());

        let blks_written = IntGaugeVec::new(
            Opts::new(
                "blks_written",
                "Number of disk blocks written for this SLRU.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_slru")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(blks_written.desc().into_iter().cloned());

        let blks_exists = IntGaugeVec::new(
            Opts::new(
                "blks_exists",
                "Number of blocks checked for existence for this SLRU.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("stat_slru")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(blks_exists.desc().into_iter().cloned());

        let flushes = IntGaugeVec::new(
            Opts::new("flushes", "Number of flushes of dirty data for this SLRU.")
                .namespace(super::NAMESPACE)
                .subsystem("stat_slru")
                .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(flushes.desc().into_iter().cloned());

        let truncates = IntGaugeVec::new(
            Opts::new("truncates", "Number of truncates for this SLRU.")
                .namespace(super::NAMESPACE)
                .subsystem("stat_slru")
                .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(truncates.desc().into_iter().cloned());

        Ok(PGStatSlruCollector {
            dbi,
            data,
            descs,
            blks_zeroed,
            blks_hit,
            blks_read,
            blks_written,
            blks_exists,
            flushes,
            truncates,
        })
    }
}

impl Collector for PGStatSlruCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut mfs = Vec::with_capacity(7);

        let Some(data_lock) = self.data.read_or_log("pg stat_slru collect") else {
            return mfs;
        };

        for row in data_lock.iter() {
            let vals = vec![row.name.as_str()];

            self.blks_zeroed
                .with_label_values(&vals)
                .set(row.blks_zeroed);
            self.blks_hit.with_label_values(&vals).set(row.blks_hit);
            self.blks_read.with_label_values(&vals).set(row.blks_read);
            self.blks_written
                .with_label_values(&vals)
                .set(row.blks_written);
            self.blks_exists
                .with_label_values(&vals)
                .set(row.blks_exists);
            self.flushes.with_label_values(&vals).set(row.flushes);
            self.truncates.with_label_values(&vals).set(row.truncates);
        }

        mfs.extend(self.blks_zeroed.collect());
        mfs.extend(self.blks_hit.collect());
        mfs.extend(self.blks_read.collect());
        mfs.extend(self.blks_written.collect());
        mfs.extend(self.blks_exists.collect());
        mfs.extend(self.flushes.collect());
        mfs.extend(self.truncates.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGStatSlruCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        self.dbi.ensure_ready().await?;
        let rows = sqlx::query_as::<_, PGStatSlruStats>(POSTGRES_STAT_SLRU_QUERY)
            .fetch_all(&self.dbi.db)
            .await?;

        let mut data_lock = self.data.write_or_bail("pg stat_slru collector")?;

        *data_lock = rows;

        Ok(())
    }
}

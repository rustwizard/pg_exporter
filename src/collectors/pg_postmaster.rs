use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;
use prometheus::Gauge;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;

use crate::instance;

use super::PG;

const POSTMASTER_QUERY: &str = "SELECT extract(epoch from pg_postmaster_start_time)::FLOAT8 as start_time_seconds from pg_postmaster_start_time()";
const POSTMASTER_SUBSYSTEM: &str = "postmaster";

#[derive(sqlx::FromRow, Debug)]
pub struct PGPostmasterStats {
    start_time_seconds: f64,
}

impl PGPostmasterStats {
    pub fn new() -> PGPostmasterStats {
        PGPostmasterStats {
            start_time_seconds: (0.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGPostmasterCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGPostmasterStats>>,
    descs: Vec<Desc>,
    start_time_seconds: Gauge,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGPostmasterCollector> {
    match PGPostmasterCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            eprintln!("error when create pg postmaster collector: {}", e);
            None
        }
    }
}

impl PGPostmasterCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGPostmasterCollector> {
        let start_time_seconds = Gauge::with_opts(
            Opts::new("start_time_seconds", "Time at which postmaster started")
                .namespace(super::NAMESPACE)
                .subsystem(POSTMASTER_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
        )?;

        let mut descs = Vec::new();
        descs.extend(start_time_seconds.desc().into_iter().cloned());

        Ok(PGPostmasterCollector {
            dbi,
            data: Arc::new(RwLock::new(PGPostmasterStats::new())),
            descs,
            start_time_seconds,
        })
    }
}

impl Collector for PGPostmasterCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(1);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                eprintln!("pg postmaster collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        self.start_time_seconds.set(data_lock.start_time_seconds);

        mfs.extend(self.start_time_seconds.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGPostmasterCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_stats = sqlx::query_as::<_, PGPostmasterStats>(POSTMASTER_QUERY)
            .fetch_optional(&self.dbi.db)
            .await?;

        if let Some(stats) = maybe_stats {
            let mut data_lock = match self.data.write() {
                Ok(data_lock) => data_lock,
                Err(e) => bail!("pg postmaster collector: can't acquire write lock. {}", e),
            };

            data_lock.start_time_seconds = stats.start_time_seconds;
        }

        Ok(())
    }
}

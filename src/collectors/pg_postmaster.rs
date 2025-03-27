use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use prometheus::core::{Desc, Opts, Collector};
use prometheus::Gauge;
use prometheus::proto;

use crate::instance;

use super::PG;



const POSTMASTER_QUERY: &str = "SELECT extract(epoch from pg_postmaster_start_time)::FLOAT8 as start_time_seconds from pg_postmaster_start_time()";
const POSTMASTER_SUBSYSTEM: &str = "postmaster";

#[derive(sqlx::FromRow, Debug)]
pub struct PGPostmasterStats {
    start_time_seconds: f64
}

impl PGPostmasterStats {
    pub fn new() -> PGPostmasterStats {
        PGPostmasterStats{
            start_time_seconds: (0.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGPostmasterCollector {
    dbi: instance::PostgresDB,
    data: Arc<RwLock<PGPostmasterStats>>,
    descs: Vec<Desc>,
    start_time_seconds: Gauge,
}


pub fn new(dbi: instance::PostgresDB) -> PGPostmasterCollector {
    PGPostmasterCollector::new(dbi)
}


impl PGPostmasterCollector {
    pub fn new(dbi: instance::PostgresDB) -> PGPostmasterCollector {
        let start_time_seconds = Gauge::with_opts(
            Opts::new(
                "start_time_seconds",
                "Time at which postmaster started",
            )
            .namespace(super::NAMESPACE)
            .subsystem(POSTMASTER_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(start_time_seconds.desc().into_iter().cloned());

        PGPostmasterCollector{
            dbi: dbi,
            data: Arc::new(RwLock::new(PGPostmasterStats::new())),
            descs: descs,
            start_time_seconds: start_time_seconds,
        }
    }
}



impl Collector for PGPostmasterCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(1);
        
        let data_lock = self.data.read().unwrap();
        self.start_time_seconds.set(data_lock.start_time_seconds);

        mfs.extend(self.start_time_seconds.collect());
        mfs

    }
}

#[async_trait]
impl PG for PGPostmasterCollector {
     async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_stats = sqlx::query_as::<_, PGPostmasterStats>(POSTMASTER_QUERY).fetch_optional(&self.dbi.db).await?;
       
        if let Some(stats) = maybe_stats {
            let mut data_lock = self.data.write().unwrap();
            data_lock.start_time_seconds         = stats.start_time_seconds;
        }
    
        Ok(())
    }
}
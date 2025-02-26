use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prometheus::core::{Desc, Opts, Collector};
use prometheus::Gauge;
use prometheus::proto;
use sqlx::PgPool;

use super::PG;



const POSTMASTER_QUERY: &str = "SELECT extract(epoch from pg_postmaster_start_time)::FLOAT8 as start_time_seconds from pg_postmaster_start_time()";

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
    db: PgPool,
    data: Arc<Mutex<PGPostmasterStats>>,
    descs: Vec<Desc>,
    start_time_seconds: Gauge,
}


pub fn new<S: Into<String>>(namespace: S, db: PgPool) -> PGPostmasterCollector {
    PGPostmasterCollector::new(namespace, db)
}


impl PGPostmasterCollector {
    pub fn new<S: Into<String>>(namespace: S, db: PgPool) -> PGPostmasterCollector {
        let namespace = namespace.into();

        let start_time_seconds = Gauge::with_opts(
            Opts::new(
                "start_time_seconds",
                "Time at which postmaster started",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(start_time_seconds.desc().into_iter().cloned());

        PGPostmasterCollector{
            db: db,
            data: Arc::new(Mutex::new(PGPostmasterStats::new())),
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
        // collect MetricFamilys.
        let mut mfs = Vec::with_capacity(1);
        
        let data_lock = self.data.lock().unwrap();
        self.start_time_seconds.set(data_lock.start_time_seconds);

        mfs.extend(self.start_time_seconds.collect());
        mfs

    }
}

#[async_trait]
impl PG for PGPostmasterCollector {
     async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_stats = sqlx::query_as::<_, PGPostmasterStats>(POSTMASTER_QUERY).fetch_optional(&self.db).await?;
       
        if let Some(stats) = maybe_stats {
            let mut data_lock = self.data.lock().unwrap();
            data_lock.start_time_seconds         = stats.start_time_seconds;
        }
    
        Ok(())
    }
}
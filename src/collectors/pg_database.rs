use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prometheus::core::{Desc, Opts, Collector};
use prometheus::IntGauge;
use prometheus::proto;
use sqlx::PgPool;

use super::PG;

const PG_DATABASE_QUERY: &str     = "SELECT pg_database.datname FROM pg_database;";
const PG_DATABASE_SIZE_QUERY: &str = "SELECT pg_database_size($1)";
const DATABASE_SUBSYSTEM: &str = "database";

#[derive(sqlx::FromRow, Debug)]
pub struct PGDatabaseStats {
    size_bytes: i64
}

#[derive(sqlx::FromRow, Debug)]
pub struct PGDatabaseName {
    name: String
}

impl PGDatabaseStats {
    pub fn new() -> PGDatabaseStats {
        PGDatabaseStats{
            size_bytes: (0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGDatabaseCollector {
    db: PgPool,
    data: Arc<Mutex<PGDatabaseStats>>,
    descs: Vec<Desc>,
    size_bytes: IntGauge,
}

pub fn new<S: Into<String>>(namespace: S, db: PgPool) -> PGDatabaseCollector {
    PGDatabaseCollector::new(namespace, db)
}

impl PGDatabaseCollector {
    pub fn new<S: Into<String>>(namespace: S, db: PgPool) -> PGDatabaseCollector {
        let namespace = namespace.into();

        let size_bytes = IntGauge::with_opts(
            Opts::new(
                "size_bytes",
                "Disk space used by the database",
            )
            .namespace(namespace.clone()).subsystem(DATABASE_SUBSYSTEM),
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(size_bytes.desc().into_iter().cloned());

        PGDatabaseCollector{
            db: db,
            data: Arc::new(Mutex::new(PGDatabaseStats::new())),
            descs: descs,
            size_bytes: size_bytes,
        }
    }
}

impl Collector for PGDatabaseCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilys.
        let mut mfs = Vec::with_capacity(1);
        
        let data_lock = self.data.lock().unwrap();
        self.size_bytes.set(data_lock.size_bytes);

        mfs.extend(self.size_bytes.collect());
        mfs

    }
}

#[async_trait]
impl PG for PGDatabaseCollector {
     async fn update(&self) -> Result<(), anyhow::Error> {
        let datnames = sqlx::query_as::<_, PGDatabaseName>(PG_DATABASE_QUERY).fetch_all(&self.db).await?;
       
        for dbname in datnames {
            if dbname.name.len() > 0 {
                let maybe_size =  sqlx::query_as::<_, PGDatabaseStats>(PG_DATABASE_SIZE_QUERY).bind(dbname.name).fetch_optional(&self.db).await?;
                if let Some(dbsize) = maybe_size {
                let mut data_lock = self.data.lock().unwrap();
                    data_lock.size_bytes         = dbsize.size_bytes;
                }
            }
        }
    
        Ok(())
    }
}
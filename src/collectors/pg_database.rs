use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;
use prometheus::IntGaugeVec;
use sqlx::PgPool;

use super::{NAMESPACE, PG};

const PG_DATABASE_QUERY: &str = "SELECT pg_database.datname as name FROM pg_database;";
const PG_DATABASE_SIZE_QUERY: &str = "SELECT pg_database_size($1)";
const DATABASE_SUBSYSTEM: &str = "database";

#[derive(sqlx::FromRow, Debug)]
pub struct PGDatabaseStats {
    size_bytes: HashMap<String, i64>,
}

#[derive(sqlx::FromRow, Debug)]
pub struct PGDatabaseName {
    name: String,
}

impl PGDatabaseStats {
    pub fn new() -> PGDatabaseStats {
        PGDatabaseStats {
            size_bytes: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGDatabaseCollector {
    db: PgPool,
    data: Arc<Mutex<PGDatabaseStats>>,
    descs: Vec<Desc>,
    size_bytes: IntGaugeVec,
}

pub fn new(db: PgPool) -> PGDatabaseCollector {
    PGDatabaseCollector::new(db)
}

impl PGDatabaseCollector {
    pub fn new(db: PgPool) -> PGDatabaseCollector {
        let size_bytes = IntGaugeVec::new(
            Opts::new("size_bytes", "Disk space used by the database")
                .namespace(NAMESPACE)
                .subsystem(DATABASE_SUBSYSTEM),
            &["datname"],
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(size_bytes.desc().into_iter().cloned());

        PGDatabaseCollector {
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
        data_lock
            .size_bytes
            .iter()
            .map(|(dbname, &size)| self.size_bytes.with_label_values(&[&dbname[..]]).set(size))
            .count();

        mfs.extend(self.size_bytes.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGDatabaseCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let datnames = sqlx::query_as::<_, PGDatabaseName>(PG_DATABASE_QUERY)
            .fetch_all(&self.db)
            .await?;

        //TODO: amortize this with one query with select  
        for dbname in datnames {
            if dbname.name.len() > 0 {
                let db_size: (i64,) = sqlx::query_as(PG_DATABASE_SIZE_QUERY)
                    .bind(&dbname.name)
                    .fetch_one(&self.db)
                    .await?;

                    let mut data_lock = self.data.lock().unwrap();
                    data_lock.size_bytes.insert(dbname.name, db_size.0);

                
            }
        }

        Ok(())
    }
}

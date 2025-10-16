use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use prometheus::IntGaugeVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;

use crate::instance;

use super::PG;

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
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGDatabaseStats>>,
    descs: Vec<Desc>,
    size_bytes: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> PGDatabaseCollector {
    PGDatabaseCollector::new(dbi)
}

impl PGDatabaseCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> PGDatabaseCollector {
        let size_bytes = IntGaugeVec::new(
            Opts::new("size_bytes", "Disk space used by the database")
                .namespace(super::NAMESPACE)
                .subsystem(DATABASE_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
            &["datname"],
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(size_bytes.desc().into_iter().cloned());

        PGDatabaseCollector {
            dbi,
            data: Arc::new(RwLock::new(PGDatabaseStats::new())),
            descs,
            size_bytes,
        }
    }
}

impl Collector for PGDatabaseCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(1);

        let data_lock = self.data.read().unwrap();
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
            .fetch_all(&self.dbi.db)
            .await?;

        //TODO: amortize this with one query with select
        for dbname in datnames {
            if self.dbi.excluded_db_names.contains(&dbname.name) {
                continue;
            }

            if !dbname.name.is_empty() {
                let db_size: (i64,) = sqlx::query_as(PG_DATABASE_SIZE_QUERY)
                    .bind(&dbname.name)
                    .fetch_one(&self.dbi.db)
                    .await?;

                let mut data_lock = self.data.write().unwrap();
                data_lock.size_bytes.insert(dbname.name, db_size.0);
            }
        }

        Ok(())
    }
}

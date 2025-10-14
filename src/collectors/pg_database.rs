use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use prometheus::IntGaugeVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::{self, MetricFamily};

use crate::instance;

use super::PG;

const PG_DATABASE_QUERY: &str = "SELECT pg_database.datname as name FROM pg_database;";
const PG_DATABASE_SIZE_QUERY: &str = "SELECT pg_database_size($1)";
const DATABASE_SUBSYSTEM: &str = "database";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGDatabaseStats {
    size_bytes: HashMap<String, i64>,
}

#[derive(sqlx::FromRow, Debug)]
pub struct PGDatabaseName {
    name: String,
}

impl PGDatabaseStats {
    pub fn new() -> PGDatabaseStats {
        Self::default()
    }
}

#[derive(Debug, Clone)]
pub struct PGDatabaseCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGDatabaseStats>>,
    descs: Vec<Desc>,
    size_bytes: IntGaugeVec,
    mfs: Vec<MetricFamily>,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGDatabaseCollector> {
    match PGDatabaseCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            eprintln!("error when create pg database collector: {}", e);
            None
        }
    }
}

impl PGDatabaseCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGDatabaseCollector> {
        let size_bytes = IntGaugeVec::new(
            Opts::new("size_bytes", "Disk space used by the database")
                .namespace(super::NAMESPACE)
                .subsystem(DATABASE_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
            &["datname"],
        )?;

        let mut descs = Vec::new();
        descs.extend(size_bytes.desc().into_iter().cloned());

        Ok(PGDatabaseCollector {
            dbi,
            data: Arc::new(RwLock::new(PGDatabaseStats::new())),
            descs,
            size_bytes,
            mfs: Vec::new(),
        })
    }
}

impl Collector for PGDatabaseCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.mfs.clone()
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

                let mut data_lock = match self.data.write() {
                    Ok(data_lock) => data_lock,
                    Err(e) => bail!("pg database collector: can't acquire lock. {}", e),
                };
                data_lock.size_bytes.insert(dbname.name, db_size.0);
            }
        }

        Ok(())
    }

    async fn collect(&mut self) -> Result<(), anyhow::Error> {
        let data_lock = self
            .data
            .read()
            .map_err(|e| anyhow!("pg database collect: RwLock poisoned during read: {}", e))?;

        data_lock
            .size_bytes
            .iter()
            .map(|(dbname, &size)| self.size_bytes.with_label_values(&[&dbname[..]]).set(size))
            .count();

        self.mfs.extend(self.size_bytes.collect());
        Ok(())
    }
}

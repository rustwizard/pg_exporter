use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use prometheus::IntGaugeVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto;

use crate::instance;

use super::{PG, RwLockExt};

const PG_DATABASE_QUERY: &str = "SELECT datname AS name, pg_database_size(datname) AS size_bytes \
     FROM pg_database WHERE datname != ALL($1) AND datname != ''";

const DATABASE_SUBSYSTEM: &str = "database";

#[derive(sqlx::FromRow, Debug)]
struct PGDatabaseRow {
    name: String,
    size_bytes: i64,
}

#[derive(Debug, Default)]
pub struct PGDatabaseStats {
    size_bytes: HashMap<String, i64>,
}

#[derive(Debug, Clone)]
pub struct PGDatabaseCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGDatabaseStats>>,
    size_bytes: IntGaugeVec,
}

crate::collector_new!(dbi, "pg database", PGDatabaseCollector);

impl PGDatabaseCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGDatabaseCollector> {
        let size_bytes = IntGaugeVec::new(
            Opts::new("size_bytes", "Disk space used by the database")
                .namespace(super::NAMESPACE)
                .subsystem(DATABASE_SUBSYSTEM)
                .const_labels(dbi.labels.clone()),
            &["datname"],
        )?;

        Ok(PGDatabaseCollector {
            dbi,
            data: Arc::new(RwLock::new(PGDatabaseStats::default())),
            size_bytes,
        })
    }
}

impl Collector for PGDatabaseCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.size_bytes.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::with_capacity(1);

        let Some(data_lock) = self.data.read_or_log("pg database collect") else {
            return mfs;
        };

        data_lock
            .size_bytes
            .iter()
            .for_each(|(dbname, &size)| self.size_bytes.with_label_values(&[dbname]).set(size));

        mfs.extend(self.size_bytes.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGDatabaseCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let rows = sqlx::query_as::<_, PGDatabaseRow>(PG_DATABASE_QUERY)
            .bind(&self.dbi.excluded_db_names)
            .fetch_all(&self.dbi.db)
            .await?;

        let new_sizes: HashMap<String, i64> =
            rows.into_iter().map(|r| (r.name, r.size_bytes)).collect();

        let mut data_lock = self.data.write_or_bail("pg database collector")?;
        data_lock.size_bytes = new_sizes;

        Ok(())
    }
}

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntGaugeVec, proto};
use tracing::{error, info};

use crate::collectors::{PG, POSTGRES_V10};
use crate::instance;

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

// PGStorageCollector exposing various stats related to Postgres storage layer.
// This stats observed using different stats sources.
#[derive(Debug, Clone)]
pub struct PGStorageCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGStorageStats>>>,
    descs: Vec<Desc>,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGStorageCollector> {
    // Collecting pg_storage since Postgres 10.
    if dbi.cfg.pg_version >= POSTGRES_V10 {
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

        Ok(Self { dbi, data, descs })
    }
}

impl Collector for PGStorageCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        todo!()
    }
}

#[async_trait]
impl PG for PGStorageCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let mut pg_storage_stat_rows =
            sqlx::query_as::<_, PGStorageStats>(POSTGRES_TEMP_FILES_INFLIGHT)
                .bind(self.dbi.cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?;

        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg storage collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();
        data_lock.append(&mut pg_storage_stat_rows);

        Ok(())
    }
}

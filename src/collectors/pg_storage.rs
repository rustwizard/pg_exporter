use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntGaugeVec, proto};
use tracing::error;

use crate::collectors::PG;
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
    match PGStorageCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            error!("error when create pg storage collector: {}", e);
            None
        }
    }
}

impl PGStorageCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
        todo!()
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
        Ok(())
    }
}

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use crate::instance;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
use tracing::{error, info};

use crate::collectors::{PG, POSTGRES_V10, POSTGRES_V96};

// Query for Postgres version 9.6 and older.
const POSTGRES_REPLICATION_QUERY96: &str = "SELECT database, slot_name, slot_type, active, 
		CASE WHEN pg_is_in_recovery() THEN pg_xlog_location_diff(pg_last_xlog_receive_location(), restart_lsn) 
		ELSE pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn) END AS since_restart_bytes 
		FROM pg_replication_slots";

// Query for Postgres versions from 10 and newer.
const POSTGRES_REPLICATION_QUERY_LATEST: &str = "SELECT database, slot_name, slot_type, active, 
    CASE WHEN pg_is_in_recovery() THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), restart_lsn) 
    ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) END AS since_restart_bytes 
    FROM pg_replication_slots";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGReplicationSlotsStats {
    database: Option<String>,
    slot_name: Option<String>,
    slot_type: Option<String>,
    active: Option<bool>,
    since_restart_bytes: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct PGReplicationSlotsCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGReplicationSlotsStats>>>,
    descs: Vec<Desc>,
    retained_bytes: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGReplicationSlotsCollector> {
    // Collecting pg_replication since Postgres 9.6.
    if dbi.cfg.pg_version >= POSTGRES_V96 {
        match PGReplicationSlotsCollector::new(dbi) {
            Ok(result) => Some(result),
            Err(e) => {
                error!("error when create pg replication slots collector: {}", e);
                None
            }
        }
    } else {
        info!("some server-side functions are not available, required Postgres 9.6 or newer");
        None
    }
}

impl PGReplicationSlotsCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGReplicationSlotsStats::default()]));
        let label_names = vec!["database", "slot_name", "slot_type", "active"];

        let retained_bytes = IntGaugeVec::new(
            Opts::new(
                "wal_retain_bytes",
                "Number of WAL retained and required by consumers, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication_slot")
            .const_labels(dbi.labels.clone()),
            &label_names,
        )?;
        descs.extend(retained_bytes.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            retained_bytes,
        })
    }
}

impl Collector for PGReplicationSlotsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        mfs
    }
}

#[async_trait]
impl PG for PGReplicationSlotsCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

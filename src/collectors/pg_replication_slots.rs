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
        let mut mfs = Vec::with_capacity(1);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!(
                    "pg replication slots collect: can't acquire read lock: {}",
                    e
                );
                // return empty mfs
                return mfs;
            }
        };

        for row in data_lock.iter() {
            let active = row.active.unwrap_or_default();
            let database: String = row.database.clone().unwrap_or_default().to_string();
            let slot_name = row.slot_name.clone().unwrap_or_default();
            let slot_type = row.slot_type.clone().unwrap_or_default();

            self.retained_bytes
                .with_label_values(&[
                    database.as_str(),
                    slot_name.as_str(),
                    slot_type.as_str(),
                    active.to_string().as_str(),
                ])
                .set(
                    row.since_restart_bytes
                        .unwrap_or_default()
                        .to_i64()
                        .unwrap_or_default(),
                );
        }

        mfs.extend(self.retained_bytes.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGReplicationSlotsCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let mut pg_replc_slots_stat_rows = if self.dbi.cfg.pg_version < POSTGRES_V10 {
            sqlx::query_as::<_, PGReplicationSlotsStats>(POSTGRES_REPLICATION_QUERY96)
                .bind(self.dbi.cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGReplicationSlotsStats>(POSTGRES_REPLICATION_QUERY_LATEST)
                .fetch_all(&self.dbi.db)
                .await?
        };
        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!(
                "pg replication slots collector: can't acquire write lock. {}",
                e
            ),
        };

        data_lock.clear();
        data_lock.append(&mut pg_replc_slots_stat_rows);

        Ok(())
    }
}

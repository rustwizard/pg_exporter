use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use crate::instance;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
use sqlx::Row;
use tracing::{error, info};

use crate::collectors::{PG, POSTGRES_V10, POSTGRES_V96};

// Query for Postgres version 9.6 and older.
const POSTGRES_REPLICATION_QUERY96: &str = "SELECT pid, COALESCE(host(client_addr), '127.0.0.1') AS client_addr, 
		COALESCE(client_port, '0') AS client_port, 
		usename AS user, application_name, state, 
		CASE WHEN pg_is_in_recovery() THEN COALESCE(pg_xlog_location_diff(pg_last_xlog_receive_location(), sent_location), 0) 
		ELSE COALESCE(pg_xlog_location_diff(pg_current_xlog_location(), sent_location), 0) END AS pending_lag_bytes, 
		COALESCE(pg_xlog_location_diff(sent_location, write_location), 0) AS write_lag_bytes, 
		COALESCE(pg_xlog_location_diff(write_location, flush_location), 0) AS flush_lag_bytes, 
		COALESCE(pg_xlog_location_diff(flush_location, replay_location), 0) AS replay_lag_bytes, 
		CASE WHEN pg_is_in_recovery() THEN COALESCE(pg_xlog_location_diff(pg_last_xlog_replay_location(), replay_location), 0) 
		ELSE COALESCE(pg_xlog_location_diff(pg_current_xlog_location(), replay_location), 0) END AS total_lag_bytes, 
		NULL::numeric AS write_lag_seconds, NULL::numeric AS flush_lag_seconds, 
		NULL::numeric AS replay_lag_seconds, NULL::numeric AS total_lag_seconds 
		FROM pg_stat_replication";

// Query for Postgres versions from 10 and newer.
const POSTGRES_REPLICATION_QUERY_LATEST: &str = "SELECT pid, COALESCE(host(client_addr), '127.0.0.1') AS client_addr, 
		COALESCE(client_port, '0') AS client_port, 
		usename AS user, application_name, state, 
		CASE WHEN pg_is_in_recovery() THEN COALESCE(abs(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), sent_lsn)), 0) 
		ELSE COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn), 0) END AS pending_lag_bytes, 
		COALESCE(pg_wal_lsn_diff(sent_lsn, write_lsn), 0) AS write_lag_bytes, 
		COALESCE(pg_wal_lsn_diff(write_lsn, flush_lsn), 0) AS flush_lag_bytes, 
		COALESCE(pg_wal_lsn_diff(flush_lsn, replay_lsn), 0) AS replay_lag_bytes, 
		CASE WHEN pg_is_in_recovery() THEN COALESCE(pg_wal_lsn_diff(pg_last_wal_replay_lsn(), replay_lsn), 0) 
		ELSE COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0) END AS total_lag_bytes, 
		COALESCE(EXTRACT(EPOCH FROM write_lag), 0) AS write_lag_seconds, 
		COALESCE(EXTRACT(EPOCH FROM flush_lag), 0) AS flush_lag_seconds, 
		COALESCE(EXTRACT(EPOCH FROM replay_lag), 0) AS replay_lag_seconds, 
		COALESCE(EXTRACT(EPOCH FROM write_lag+flush_lag+replay_lag), 0) AS total_lag_seconds 
		FROM pg_stat_replication";

#[derive(sqlx::FromRow, Debug, Default)]
pub struct PGReplicationStats {
    pid: Option<i32>,
    client_addr: Option<String>,
    client_port: Option<i32>,
    user: Option<String>,
    application_name: Option<String>,
    state: Option<String>,
    pending_lag_bytes: Option<Decimal>,
    write_lag_bytes: Option<Decimal>,
    flush_lag_bytes: Option<Decimal>,
    replay_lag_bytes: Option<Decimal>,
    total_lag_bytes: Option<Decimal>,
    write_lag_seconds: Option<Decimal>,
    flush_lag_seconds: Option<Decimal>,
    replay_lag_seconds: Option<Decimal>,
    total_lag_seconds: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct PGReplicationCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<PGReplicationStats>>>,
    descs: Vec<Desc>,
    label_names: Vec<String>,
    lag_bytes: IntGaugeVec,
    lag_seconds: IntGaugeVec,
    lag_total_bytes: IntGaugeVec,
    lag_total_seconds: IntGaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGReplicationCollector> {
    // Collecting pg_replication since Postgres 9.6.
    if dbi.cfg.pg_version >= POSTGRES_V96 {
        match PGReplicationCollector::new(dbi) {
            Ok(result) => Some(result),
            Err(e) => {
                error!("error when create pg replication collector: {}", e);
                None
            }
        }
    } else {
        info!("some server-side functions are not available, required Postgres 9.6 or newer");
        None
    }
}

impl PGReplicationCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<Self> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(vec![PGReplicationStats::default()]));
        let label_names = vec![
            "client_addr".to_string(),
            "client_port".to_string(),
            "user".to_string(),
            "application_name".to_string(),
            "state".to_string(),
            "lag".to_string(),
        ];

        let lag_bytes = IntGaugeVec::new(
            Opts::new(
                "lag_bytes",
                "Number of bytes standby is behind than primary in each WAL processing phase.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication")
            .const_labels(dbi.labels.clone()),
            &[
                "client_addr",
                "client_port",
                "user",
                "application_name",
                "state",
                "lag",
            ],
        )?;
        descs.extend(lag_bytes.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            label_names,
            lag_bytes,
            lag_seconds: todo!(),
            lag_total_bytes: todo!(),
            lag_total_seconds: todo!(),
        })
    }
}

impl Collector for PGReplicationCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg replication collect: can't acquire read lock: {}", e);
                // return empty mfs
                return mfs;
            }
        };

        for row in data_lock.iter() {
            self.lag_bytes.with_label_values(&["pending"]).set(
                row.pending_lag_bytes
                    .unwrap_or_default()
                    .to_i64()
                    .unwrap_or_default(),
            );
        }

        mfs.extend(self.lag_bytes.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGReplicationCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

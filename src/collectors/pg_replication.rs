use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use crate::{app, instance};
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{IntGaugeVec, proto};
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
            "client_addr",
            "client_port",
            "user",
            "application_name",
            "state",
            "lag",
        ];

        let lag_bytes = IntGaugeVec::new(
            Opts::new(
                "lag_bytes",
                "Number of bytes standby is behind than primary in each WAL processing phase.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication")
            .const_labels(dbi.labels.clone()),
            &label_names,
        )?;
        descs.extend(lag_bytes.desc().into_iter().cloned());

        let lag_seconds = IntGaugeVec::new(
            Opts::new(
                "lag_seconds",
                "Number of seconds standby is behind than primary in each WAL processing phase.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication")
            .const_labels(dbi.labels.clone()),
            &label_names,
        )?;
        descs.extend(lag_seconds.desc().into_iter().cloned());

        let lag_total_bytes = IntGaugeVec::new(
            Opts::new(
                "lag_all_bytes",
                "Number of bytes standby is behind than primary including all phases.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication")
            .const_labels(dbi.labels.clone()),
            &label_names,
        )?;
        descs.extend(lag_total_bytes.desc().into_iter().cloned());

        let lag_total_seconds = IntGaugeVec::new(
            Opts::new(
                "lag_all_seconds",
                "Number of seconds standby is behind than primary including all phases.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("replication")
            .const_labels(dbi.labels.clone()),
            &label_names,
        )?;
        descs.extend(lag_total_seconds.desc().into_iter().cloned());

        Ok(Self {
            dbi,
            data,
            descs,
            lag_bytes,
            lag_seconds,
            lag_total_bytes,
            lag_total_seconds,
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
            let client_addr = row.client_addr.clone().unwrap_or_default();
            let client_port = row.client_port.unwrap_or_default().to_string();
            let user = row.user.clone().unwrap_or_default();
            let app_name = row.application_name.clone().unwrap_or_default();
            let state = row.state.clone().unwrap_or_default();

            self.lag_bytes
                .with_label_values(&[
                    client_addr.as_str(),
                    client_port.as_str(),
                    user.as_str(),
                    app_name.as_str(),
                    state.as_str(),
                    "pending",
                ])
                .set(
                    row.pending_lag_bytes
                        .unwrap_or_default()
                        .to_i64()
                        .unwrap_or_default(),
                );

            self.lag_bytes
                .with_label_values(&[
                    client_addr.as_str(),
                    client_port.as_str(),
                    user.as_str(),
                    app_name.as_str(),
                    state.as_str(),
                    "write",
                ])
                .set(
                    row.write_lag_bytes
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
        let mut pg_replc_stat_rows = if self.dbi.cfg.pg_version < POSTGRES_V10 {
            sqlx::query_as::<_, PGReplicationStats>(POSTGRES_REPLICATION_QUERY96)
                .bind(self.dbi.cfg.pg_collect_topidx)
                .fetch_all(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGReplicationStats>(POSTGRES_REPLICATION_QUERY_LATEST)
                .fetch_all(&self.dbi.db)
                .await?
        };
        let mut data_lock = match self.data.write() {
            Ok(data_lock) => data_lock,
            Err(e) => bail!("pg replication collector: can't acquire write lock. {}", e),
        };

        data_lock.clear();
        data_lock.append(&mut pg_replc_stat_rows);

        Ok(())
    }
}

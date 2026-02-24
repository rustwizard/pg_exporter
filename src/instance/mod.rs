use anyhow::bail;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn};

use crate::collectors;

#[derive(Debug, Clone)]
pub struct PGConfig {
    pub pg_version: i64,
    pub pg_block_size: i64,
    pub pg_wal_segment_size: i64,
    pub pg_collect_topidx: i64,
    pub pg_collect_topq: i64,
    pub pg_collect_top_table: i64,
    // NoTrackMode controls collector to gather and send sensitive information, such as queries texts.
    pub notrack: bool,
    // pg_stat_statements defines is pg_stat_statements available in shared_preload_libraries and available for queries.
    pub pg_stat_statements: bool,
    // pg_stat_statements_schema defines the schema name where pg_stat_statements is installed.
    pub pg_stat_statements_schema: String,
}

#[derive(Debug, Clone)]
pub struct PostgresDB {
    pub db: Pool<Postgres>,
    pub excluded_db_names: Vec<String>,
    pub labels: HashMap<String, String>,
    cfg: Arc<RwLock<Option<PGConfig>>>,
    source_cfg: Config,
}

// TODO: make fields Optional
#[derive(Debug, Default, Clone, serde_derive::Deserialize, PartialEq, Eq)]
pub struct Config {
    pub dsn: String,
    pub exclude_db_names: Option<Vec<String>>,
    pub const_labels: HashMap<String, String>,
    pub collect_top_query: Option<i64>,
    pub collect_top_index: Option<i64>,
    pub collect_top_table: Option<i64>,
    pub no_track_mode: Option<bool>,
}

pub async fn new(instance_cfg: &Config) -> anyhow::Result<PostgresDB> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .test_before_acquire(true)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .connect_lazy(&instance_cfg.dsn)?;

    let pgi = PostgresDB {
        db: pool,
        excluded_db_names: instance_cfg.exclude_db_names.clone().unwrap_or_default(),
        labels: instance_cfg.const_labels.clone(),
        cfg: Arc::new(RwLock::new(None)),
        source_cfg: instance_cfg.clone(),
    };

    // Eagerly attempt to initialize config, but do not fail if PG is unavailable.
    if let Err(e) = pgi.init_cfg().await {
        warn!(
            "pg_exporter: instance {}: unable to connect at startup, will retry on first scrape: {e}",
            instance_cfg.dsn
        );
    }

    Ok(pgi)
}

impl PostgresDB {
    /// Returns the current PGConfig without blocking if it has been initialized.
    /// Returns `None` when the database has not been reached yet.
    pub fn current_cfg(&self) -> Option<PGConfig> {
        self.cfg.read().ok()?.clone()
    }

    /// Returns the PGConfig, initializing it from the database if not yet done.
    /// Returns an error when the database is still unreachable.
    pub async fn ensure_ready(&self) -> anyhow::Result<PGConfig> {
        {
            let lock = self
                .cfg
                .read()
                .map_err(|e| anyhow::anyhow!("cfg lock poisoned: {e}"))?;
            if let Some(cfg) = lock.as_ref() {
                return Ok(cfg.clone());
            }
        }
        self.init_cfg().await?;
        let lock = self
            .cfg
            .read()
            .map_err(|e| anyhow::anyhow!("cfg lock poisoned: {e}"))?;
        lock.as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("pg_exporter: cfg not initialized after init_cfg"))
    }

    /// Clears the cached config so that the next `ensure_ready()` call re-fetches it from the
    /// database. Intended for testing lazy-reconnect behavior.
    #[allow(dead_code)]
    pub fn reset_cfg(&self) {
        if let Ok(mut lock) = self.cfg.write() {
            *lock = None;
        }
    }

    async fn init_cfg(&self) -> anyhow::Result<()> {
        let cfg = fetch_cfg(&self.db, &self.source_cfg).await?;
        let mut lock = self
            .cfg
            .write()
            .map_err(|e| anyhow::anyhow!("cfg lock poisoned: {e}"))?;
        *lock = Some(cfg);
        info!("Connection to the database is successful, configuration loaded.");
        Ok(())
    }
}

async fn fetch_cfg(pool: &Pool<Postgres>, instance_cfg: &Config) -> anyhow::Result<PGConfig> {
    let version = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'server_version_num'",
    )
    .fetch_one(pool)
    .await?;

    let pg_version = version.parse()?;

    if pg_version < collectors::POSTGRES_VMIN_NUM {
        info!(
            "Postgres version is too old, some collectors functions won't work. Minimal required version is {:?}",
            collectors::POSTGRES_VMIN_NUM
        );
    }

    let block_size = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'block_size'",
    )
    .fetch_one(pool)
    .await?;

    let pg_block_size = block_size.parse()?;

    let wal_segment_size = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'",
    )
    .fetch_one(pool)
    .await?;

    let pg_wal_segment_size = wal_segment_size.parse()?;

    let pg_stat_statements_raw = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'",
    )
    .fetch_one(pool)
    .await?;

    let exist = pg_stat_statements_raw.contains("pg_stat_statements");

    let stmnt_scheme = if exist {
        sqlx::query_scalar::<_, String>(
            "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'").fetch_optional(pool).await?
    } else {
        None
    };

    let scheme = stmnt_scheme.unwrap_or_default();

    if exist && scheme.is_empty() {
        bail!("pg_exporter: init instance: pg_stat_statement exist, but scheme is indefined");
    }

    Ok(PGConfig {
        pg_version,
        pg_block_size,
        pg_wal_segment_size,
        pg_collect_topidx: instance_cfg.collect_top_index.unwrap_or_default(),
        pg_collect_topq: instance_cfg.collect_top_query.unwrap_or_default(),
        pg_collect_top_table: instance_cfg.collect_top_table.unwrap_or_default(),
        notrack: instance_cfg.no_track_mode.unwrap_or_default(),
        pg_stat_statements: exist,
        pg_stat_statements_schema: scheme,
    })
}

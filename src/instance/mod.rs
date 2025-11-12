use anyhow::bail;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::collections::HashMap;
use tracing::info;

use crate::collectors;
use crate::config::Instance;

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
    pub cfg: PGConfig,
}

pub async fn new(instance_cfg: &Instance) -> anyhow::Result<PostgresDB> {
    let pool = match PgPoolOptions::new()
        .max_connections(10)
        .connect(&instance_cfg.dsn)
        .await
    {
        Ok(pool) => {
            info!("✅Connection to the database is successful!");
            pool
        }
        Err(err) => {
            info!("🔥 Failed to connect to the database: {err:?}");
            std::process::exit(1);
        }
    };

    let version = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'server_version_num'",
    )
    .fetch_one(&pool)
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
    .fetch_one(&pool)
    .await?;

    let pg_block_size = block_size.parse()?;

    let wal_segment_size = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'",
    )
    .fetch_one(&pool)
    .await?;

    let pg_wal_segment_size = wal_segment_size.parse()?;

    let pg_stat_statements = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'",
    )
    .fetch_one(&pool)
    .await?;

    let exist = pg_stat_statements.contains("pg_stat_statements");

    let stmnt_scheme = if exist {
        sqlx::query_scalar::<_, String>(
            "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'").fetch_optional(&pool).await?
    } else {
        None
    };

    let scheme = if let Some(val) = stmnt_scheme {
        val
    } else {
        "".to_string()
    };

    if exist && scheme.is_empty() {
        bail!("pg_exporter: init instance: pg_stat_statement exist, but scheme is indefined");
    }

    let cfg = PGConfig {
        pg_version,
        pg_block_size,
        pg_wal_segment_size,
        pg_collect_topidx: instance_cfg.collect_top_index,
        pg_collect_topq: instance_cfg.collect_top_query,
        pg_collect_top_table: instance_cfg.collect_top_table,
        notrack: instance_cfg.no_track_mode,
        pg_stat_statements: exist,
        pg_stat_statements_schema: scheme,
    };

    Ok(PostgresDB::new(
        pool,
        instance_cfg.exclude_db_names.clone(),
        instance_cfg.const_labels.clone(),
        cfg,
    ))
}

impl PostgresDB {
    pub fn new(
        db: Pool<Postgres>,
        excluded_db_names: Vec<String>,
        labels: HashMap<String, String>,
        cfg: PGConfig,
    ) -> Self {
        Self {
            db,
            excluded_db_names,
            labels,
            cfg,
        }
    }
}

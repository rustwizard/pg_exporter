use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::collections::HashMap;

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
pub struct Config {
    pub dsn: String,
    pub exclude_db_names: Vec<String>,
    pub const_labels: HashMap<String, String>,
}


#[derive(Debug, Clone)]
pub struct PGConfig {
    pub pg_version: i64,
    pub pg_block_size: i64,
    pub pg_wal_segment_size: i64,
}

#[derive(Debug, Clone)]
pub struct PostgresDB {
    pub db: Pool<Postgres>,
    pub excluded_db_names: Vec<String>,
    pub labels: HashMap<String, String>,
    pub cfg: PGConfig
}

pub async fn new(
    dsn: String,
    excluded_dbnames: Vec<String>,
    labels: HashMap<String, String>,
) -> anyhow::Result<PostgresDB> {
    let pool = match PgPoolOptions::new().max_connections(10).connect(&dsn).await {
        Ok(pool) => {
            println!("✅Connection to the database is successful!");
            pool
        }
        Err(err) => {
            println!("🔥 Failed to connect to the database: {:?}", err);
            std::process::exit(1);
        }
    };

    let version = sqlx::query_scalar::<_, String>("SELECT setting FROM pg_settings WHERE name = 'server_version_num'")
            .fetch_one(&pool).await?;

    let pg_version = version.parse()?;

    let block_size = sqlx::query_scalar::<_, String>("SELECT setting FROM pg_settings WHERE name = 'block_size'")
            .fetch_one(&pool).await?;

    let pg_block_size = block_size.parse()?;


    let wal_segment_size = sqlx::query_scalar::<_, String>("SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'")
            .fetch_one(&pool).await?;

    let pg_wal_segment_size = wal_segment_size.parse()?;
    
    let cfg = PGConfig{ 
            pg_version, 
            pg_block_size, 
            pg_wal_segment_size
        };

    Ok(PostgresDB::new(pool, excluded_dbnames, labels, cfg))
}

impl PostgresDB {
    pub fn new(
        db: Pool<Postgres>,
        excluded_db_names: Vec<String>,
        labels: HashMap<String, String>,
        cfg: PGConfig
    ) -> Self {
        Self {
            db,
            excluded_db_names,
            labels,
            cfg
        }
    }
}
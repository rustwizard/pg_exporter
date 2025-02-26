use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::collections::HashMap;

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
pub struct Config {
    pub dsn: String,
    pub exclude_db_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PostgresDB {
    pub db: Pool<Postgres>,
    pub exclude_db_names: Vec<String>,
    pub labels: HashMap<String, String>,
}

pub async fn new(dsn: String, excluded_dbnames:  Vec<String>, labels: HashMap<String, String>) -> PostgresDB {
    let pool = match PgPoolOptions::new()
            .max_connections(10)
            .connect(&dsn)
            .await
        {
            Ok(pool) => {
                println!("✅Connection to the database is successful!");
                pool
            }
            Err(err) => {
                println!("🔥 Failed to connect to the database: {:?}", err);
                std::process::exit(1);
            }
        };

        PostgresDB::new(pool, excluded_dbnames, labels)
}

impl PostgresDB {
    pub fn new(db: Pool<Postgres>, excluded_dbnames:  Vec<String>, labels: HashMap<String, String>) -> PostgresDB {
        PostgresDB{
            db: db,
            exclude_db_names: excluded_dbnames,
            labels: labels,
        }
    }
}
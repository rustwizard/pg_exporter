pub mod pg_activity;
pub mod pg_bgwirter;
pub mod pg_database;
pub mod pg_locks;
pub mod pg_postmaster;
pub mod pg_wal;
pub mod pg_stat_io;

use async_trait::async_trait;
use dyn_clone::DynClone;

const NAMESPACE: &str = "pg";

// Postgres server versions
const POSTGRES_V95: i64 = 90500;
const POSTGRES_V96: i64 = 90600;
const POSTGRES_V10: i64 = 100000;
const POSTGRES_V11: i64 = 110000;
const POSTGRES_V12: i64 = 120000;
const POSTGRES_V13: i64 = 130000;
const POSTGRES_V14: i64 = 140000;
const POSTGRES_V15: i64 = 150000;
const POSTGRES_V16: i64 = 160000;
const POSTGRES_V17: i64 = 170000;
const POSTGRES_V18: i64 = 180000;

// Minimal required version is 9.5
const POSTGRES_VMIN_NUM: i64 = POSTGRES_V95;

#[async_trait]
pub trait PG: DynClone + Send + Sync {
    async fn update(&self) -> Result<(), anyhow::Error>;
}

impl Clone for Box<dyn PG> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}


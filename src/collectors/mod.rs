pub mod pg_locks;
pub mod pg_postmaster;
pub mod pg_database;
pub mod pg_activity;
pub mod pg_bgwirter;

use async_trait::async_trait;
use dyn_clone::DynClone;

const NAMESPACE: &str = "pg";

#[async_trait]
pub trait PG: DynClone + Send + Sync {
    async fn update(&self) -> Result<(), anyhow::Error>;
}

impl Clone for Box<dyn PG> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
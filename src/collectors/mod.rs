pub mod pg_locks;
pub mod pg_postmaster;

use async_trait::async_trait;
use dyn_clone::DynClone;

#[async_trait]
pub trait PG: DynClone + Send + Sync {
    async fn update(&self) -> Result<(), anyhow::Error>;
}

impl Clone for Box<dyn PG> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
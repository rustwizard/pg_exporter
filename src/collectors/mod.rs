pub mod pg_activity;
pub mod pg_archiver;
pub mod pg_bgwriter;
pub mod pg_conflict;
pub mod pg_database;
pub mod pg_indexes;
pub mod pg_locks;
pub mod pg_postmaster;
pub mod pg_replication;
pub mod pg_replication_slots;
pub mod pg_settings;
pub mod pg_stat_io;
pub mod pg_stat_slru;
pub mod pg_statements;
pub mod pg_storage;
pub mod pg_tables;
pub mod pg_wal;

use std::sync::RwLock;

use async_trait::async_trait;
use dyn_clone::DynClone;

/// Extension methods on `RwLock<T>` shared by all collectors.
pub(crate) trait RwLockExt<T> {
    /// Acquires a read lock; logs an error and returns `None` on poison.
    fn read_or_log(&self, ctx: &str) -> Option<std::sync::RwLockReadGuard<'_, T>>;
    /// Acquires a write lock; returns an `anyhow::Error` on poison.
    fn write_or_bail(&self, ctx: &str) -> anyhow::Result<std::sync::RwLockWriteGuard<'_, T>>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_or_log(&self, ctx: &str) -> Option<std::sync::RwLockReadGuard<'_, T>> {
        self.read()
            .map_err(|e| tracing::error!("{ctx}: can't acquire read lock: {e}"))
            .ok()
    }

    fn write_or_bail(&self, ctx: &str) -> anyhow::Result<std::sync::RwLockWriteGuard<'_, T>> {
        self.write()
            .map_err(|e| anyhow::anyhow!("{ctx}: can't acquire write lock: {e}"))
    }
}

#[macro_export]
macro_rules! collector_new {
    ($dbi:ident, $name:expr, $collector:ty) => {
        pub fn new($dbi: ::std::sync::Arc<crate::instance::PostgresDB>) -> Option<$collector> {
            match <$collector>::new($dbi) {
                Ok(result) => Some(result),
                Err(e) => {
                    ::tracing::error!("error when create {} collector: {}", $name, e);
                    None
                }
            }
        }
    };
    ($dbi:ident, $name:expr, $collector:ty, $condition:expr) => {
        pub fn new($dbi: ::std::sync::Arc<crate::instance::PostgresDB>) -> Option<$collector> {
            if $condition {
                match <$collector>::new($dbi) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        ::tracing::error!("error when create {} collector: {}", $name, e);
                        None
                    }
                }
            } else {
                None
            }
        }
    };
    ($dbi:ident, $name:expr, $collector:ty, $condition:expr, $msg:expr) => {
        pub fn new($dbi: ::std::sync::Arc<crate::instance::PostgresDB>) -> Option<$collector> {
            if $condition {
                match <$collector>::new($dbi) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        ::tracing::error!("error when create {} collector: {}", $name, e);
                        None
                    }
                }
            } else {
                ::tracing::info!($msg);
                None
            }
        }
    };
}

const NAMESPACE: &str = "pg";

// Postgres server versions
const POSTGRES_V95: i64 = 90500;
const POSTGRES_V96: i64 = 90600;
const POSTGRES_V10: i64 = 100000;
const POSTGRES_V12: i64 = 120000;
const POSTGRES_V13: i64 = 130000;
const POSTGRES_V14: i64 = 140000;
const POSTGRES_V16: i64 = 160000;
const POSTGRES_V17: i64 = 170000;
const POSTGRES_V18: i64 = 180000;

// Minimal required version is 9.5
pub const POSTGRES_VMIN_NUM: i64 = POSTGRES_V95;

pub struct CollectorInfo {
    pub name: &'static str,
    pub min_pg_version: &'static str,
    pub notes: &'static str,
}

pub const COLLECTOR_INFO: &[CollectorInfo] = &[
    CollectorInfo {
        name: "pg_activity",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_archiver",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_bgwriter",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_conflict",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_database",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_indexes",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_locks",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_postmaster",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_replication",
        min_pg_version: "9.6",
        notes: "",
    },
    CollectorInfo {
        name: "pg_replication_slots",
        min_pg_version: "9.6",
        notes: "",
    },
    CollectorInfo {
        name: "pg_settings",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_stat_io",
        min_pg_version: "16",
        notes: "",
    },
    CollectorInfo {
        name: "pg_stat_slru",
        min_pg_version: "13",
        notes: "",
    },
    CollectorInfo {
        name: "pg_statements",
        min_pg_version: "9.5",
        notes: "requires pg_stat_statements extension",
    },
    CollectorInfo {
        name: "pg_storage",
        min_pg_version: "10",
        notes: "",
    },
    CollectorInfo {
        name: "pg_tables",
        min_pg_version: "9.5",
        notes: "",
    },
    CollectorInfo {
        name: "pg_wal",
        min_pg_version: "9.5",
        notes: "",
    },
];

pub const COLLECTOR_NAMES: &[&str] = &[
    "pg_activity",
    "pg_archiver",
    "pg_bgwriter",
    "pg_conflict",
    "pg_database",
    "pg_indexes",
    "pg_locks",
    "pg_postmaster",
    "pg_replication",
    "pg_replication_slots",
    "pg_settings",
    "pg_stat_io",
    "pg_stat_slru",
    "pg_statements",
    "pg_storage",
    "pg_tables",
    "pg_wal",
];

#[async_trait]
pub trait PG: DynClone + Send + Sync {
    async fn update(&self) -> Result<(), anyhow::Error>;
}

impl Clone for Box<dyn PG> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

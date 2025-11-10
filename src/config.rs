use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    /// pg_exporter.yml
    pub config: Instance,
    /// Path to pg_exporter.toml.
    pub config_path: PathBuf,
}

#[derive(Debug, Default, Clone, serde_derive::Deserialize, PartialEq, Eq)]
pub struct Instance {
    pub dsn: String,
    pub exclude_db_names: Vec<String>,
    pub const_labels: HashMap<String, String>,
    pub collect_top_query: i64,
    pub collect_top_index: i64,
    pub no_track_mode: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            config: Instance::default(),
            config_path: PathBuf::from("pg_exporter.yml"),
        }
    }
}

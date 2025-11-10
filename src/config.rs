use ::config::{Config, Environment, File};
use anyhow::bail;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct ExporterConfig {
    /// pg_exporter.yml
    pub config: PGEConfig,
    /// Path to pg_exporter.toml.
    pub config_path: PathBuf,
}

#[derive(Debug, Default, Clone, serde_derive::Deserialize, PartialEq, Eq)]
pub struct PGEConfig {
    pub listen_addr: String,
    pub endpoint: String,
    pub instances: HashMap<String, Instance>,
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

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            config: PGEConfig::default(),
            config_path: PathBuf::from("pg_exporter.yml"),
        }
    }
}

impl ExporterConfig {
    pub fn load(config_path: &Path) -> anyhow::Result<Self> {
        let path = match config_path.to_str() {
            Some(p) => p,
            None => bail!("config: path should be specified"),
        };

        let settings = Config::builder()
            .add_source(File::with_name(path))
            // Add in settings from the environment (with a prefix of PGE)
            // Eg.. `PGE_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("PGE"))
            .build()?;

        let pge_config: PGEConfig = settings.try_deserialize()?;

        Ok(Self {
            config: pge_config,
            config_path: config_path.into(),
        })
    }
}

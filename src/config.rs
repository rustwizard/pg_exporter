use ::config::{Config, Environment, File};
use anyhow::bail;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::instance;

#[derive(Debug, Clone)]
pub struct ExporterConfig {
    /// pg_exporter.yml
    pub config: PGEConfig,
    /// Path to pg_exporter.yml.
    pub config_path: PathBuf,
}

#[derive(Debug, Default, Clone, serde_derive::Deserialize, PartialEq, Eq)]
pub struct PGEConfig {
    pub listen_addr: Option<String>,
    pub endpoint: Option<String>,
    pub instances: Option<HashMap<String, instance::Config>>,
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
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub listen_addr: Option<String>,
    pub endpoint: Option<String>,
}

impl PGEConfig {
    pub fn overrides(&mut self, overrides: Overrides) {
        if let Some(listen_addr) = overrides.listen_addr {
            self.listen_addr = Some(listen_addr);
        }

        if let Some(endpoint) = overrides.endpoint {
            self.endpoint = Some(endpoint);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_tmp_config(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        fs::write(&path, content).expect("failed to write temp config");
        path
    }

    // --- ExporterConfig::load ---

    #[test]
    fn load_valid_config_full() {
        let yaml = r#"
listen_addr: "127.0.0.1:9090"
endpoint: /metrics
instances:
  "pg15:5432":
    dsn: "postgres://user:pass@localhost:5432/db"
    const_labels:
      project: my_project
      cluster: my_cluster
    collect_top_query: 10
    collect_top_index: 5
    collect_top_table: 3
    no_track_mode: false
"#;
        let path = write_tmp_config("pge_test_full.yml", yaml);
        let ec = ExporterConfig::load(&path).expect("should load valid config");

        assert_eq!(ec.config.listen_addr.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(ec.config.endpoint.as_deref(), Some("/metrics"));
        assert_eq!(ec.config_path, path);

        let instances = ec.config.instances.expect("instances should be present");
        let inst = instances
            .get("pg15:5432")
            .expect("instance pg15:5432 should exist");

        assert_eq!(inst.dsn, "postgres://user:pass@localhost:5432/db");
        assert_eq!(
            inst.const_labels.get("project").map(|s| s.as_str()),
            Some("my_project")
        );
        assert_eq!(
            inst.const_labels.get("cluster").map(|s| s.as_str()),
            Some("my_cluster")
        );
        assert_eq!(inst.collect_top_query, Some(10));
        assert_eq!(inst.collect_top_index, Some(5));
        assert_eq!(inst.collect_top_table, Some(3));
        assert_eq!(inst.no_track_mode, Some(false));
    }

    #[test]
    fn load_valid_config_minimal() {
        let yaml = r#"
listen_addr: "0.0.0.0:8080"
endpoint: /metrics
"#;
        let path = write_tmp_config("pge_test_minimal.yml", yaml);
        let ec = ExporterConfig::load(&path).expect("should load minimal config");

        assert_eq!(ec.config.listen_addr.as_deref(), Some("0.0.0.0:8080"));
        assert_eq!(ec.config.endpoint.as_deref(), Some("/metrics"));
        assert!(ec.config.instances.is_none());
    }

    #[test]
    fn load_config_instance_optional_fields_absent() {
        let yaml = r#"
listen_addr: "0.0.0.0:9090"
endpoint: /metrics
instances:
  "pg:5432":
    dsn: "postgres://u:p@localhost/db"
    const_labels: {}
"#;
        let path = write_tmp_config("pge_test_optional.yml", yaml);
        let ec = ExporterConfig::load(&path).expect("should load");

        let instances = ec.config.instances.unwrap();
        let inst = instances.get("pg:5432").unwrap();

        assert_eq!(inst.dsn, "postgres://u:p@localhost/db");
        assert!(inst.exclude_db_names.is_none());
        assert!(inst.collect_top_query.is_none());
        assert!(inst.collect_top_index.is_none());
        assert!(inst.collect_top_table.is_none());
        assert!(inst.no_track_mode.is_none());
    }

    #[test]
    fn load_config_with_exclude_db_names() {
        let yaml = r#"
listen_addr: "0.0.0.0:9090"
endpoint: /metrics
instances:
  "pg:5432":
    dsn: "postgres://u:p@localhost/db"
    const_labels: {}
    exclude_db_names: ["postgres", "template0", "template1"]
"#;
        let path = write_tmp_config("pge_test_exclude.yml", yaml);
        let ec = ExporterConfig::load(&path).unwrap();

        let instances = ec.config.instances.unwrap();
        let inst = instances.get("pg:5432").unwrap();

        let excluded = inst
            .exclude_db_names
            .as_ref()
            .expect("should have exclude_db_names");
        assert_eq!(excluded, &["postgres", "template0", "template1"]);
    }

    #[test]
    fn load_nonexistent_file_returns_error() {
        let path = PathBuf::from("/tmp/nonexistent_pge_config_xyz.yml");
        let result = ExporterConfig::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_invalid_yaml_returns_error() {
        let path = write_tmp_config("pge_test_invalid.yml", "{ this is: [not valid yaml");
        let result = ExporterConfig::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_empty_file_produces_default_pge_config() {
        let path = write_tmp_config("pge_test_empty.yml", "");
        let ec = ExporterConfig::load(&path).expect("empty file should deserialize to defaults");

        assert_eq!(ec.config, PGEConfig::default());
    }

    // --- ExporterConfig::default ---

    #[test]
    fn exporter_config_default_values() {
        let ec = ExporterConfig::default();
        assert_eq!(ec.config_path, PathBuf::from("pg_exporter.yml"));
        assert_eq!(ec.config, PGEConfig::default());
    }

    // --- PGEConfig::overrides ---

    #[test]
    fn overrides_both_fields() {
        let mut cfg = PGEConfig::default();
        cfg.overrides(Overrides {
            listen_addr: Some("127.0.0.1:1234".to_string()),
            endpoint: Some("/custom".to_string()),
        });

        assert_eq!(cfg.listen_addr.as_deref(), Some("127.0.0.1:1234"));
        assert_eq!(cfg.endpoint.as_deref(), Some("/custom"));
    }

    #[test]
    fn overrides_only_listen_addr() {
        let mut cfg = PGEConfig {
            listen_addr: Some("0.0.0.0:9090".to_string()),
            endpoint: Some("/metrics".to_string()),
            instances: None,
        };
        cfg.overrides(Overrides {
            listen_addr: Some("127.0.0.1:8080".to_string()),
            endpoint: None,
        });

        assert_eq!(cfg.listen_addr.as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(cfg.endpoint.as_deref(), Some("/metrics")); // unchanged
    }

    #[test]
    fn overrides_only_endpoint() {
        let mut cfg = PGEConfig {
            listen_addr: Some("0.0.0.0:9090".to_string()),
            endpoint: Some("/metrics".to_string()),
            instances: None,
        };
        cfg.overrides(Overrides {
            listen_addr: None,
            endpoint: Some("/prometheus".to_string()),
        });

        assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9090")); // unchanged
        assert_eq!(cfg.endpoint.as_deref(), Some("/prometheus"));
    }

    #[test]
    fn overrides_none_fields_leave_config_unchanged() {
        let mut cfg = PGEConfig {
            listen_addr: Some("0.0.0.0:9090".to_string()),
            endpoint: Some("/metrics".to_string()),
            instances: None,
        };
        cfg.overrides(Overrides::default());

        assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(cfg.endpoint.as_deref(), Some("/metrics"));
    }

    #[test]
    fn overrides_on_empty_config_sets_fields() {
        let mut cfg = PGEConfig::default();
        cfg.overrides(Overrides {
            listen_addr: Some("0.0.0.0:9090".to_string()),
            endpoint: Some("/metrics".to_string()),
        });

        assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(cfg.endpoint.as_deref(), Some("/metrics"));
    }
}

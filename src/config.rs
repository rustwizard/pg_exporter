use ::config::{Config, Environment, File};
use anyhow::bail;
use std::{
    collections::HashMap,
    net::SocketAddr,
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
    /// Timeout in milliseconds for a single /metrics scrape.
    /// If all collectors do not finish within this window the request
    /// returns whatever data was collected up to that point and logs a warning.
    /// Default: 30 000 ms (30 s).
    pub scrape_timeout_ms: Option<u64>,
    pub log_level: Option<String>,
    pub pool_max_connections: Option<u32>,
    pub pool_acquire_timeout_secs: Option<u64>,
    pub pool_idle_timeout_secs: Option<u64>,
    pub pool_max_lifetime_secs: Option<u64>,
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
    pub fn merge_pool_defaults(&self, mut cfg: instance::Config) -> instance::Config {
        cfg.pool_max_connections = cfg.pool_max_connections.or(self.pool_max_connections);
        cfg.pool_acquire_timeout_secs = cfg
            .pool_acquire_timeout_secs
            .or(self.pool_acquire_timeout_secs);
        cfg.pool_idle_timeout_secs = cfg.pool_idle_timeout_secs.or(self.pool_idle_timeout_secs);
        cfg.pool_max_lifetime_secs = cfg.pool_max_lifetime_secs.or(self.pool_max_lifetime_secs);
        cfg
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        // listen_addr must be present and parse as a valid SocketAddr
        match &self.listen_addr {
            None => bail!("config: 'listen_addr' is required"),
            Some(addr) if addr.is_empty() => bail!("config: 'listen_addr' must not be empty"),
            Some(addr) => {
                addr.parse::<SocketAddr>()
                    .map_err(|e| anyhow::anyhow!("config: 'listen_addr' is invalid: {e}"))?;
            }
        }

        // endpoint must be present and start with '/'
        match &self.endpoint {
            None => bail!("config: 'endpoint' is required"),
            Some(ep) if ep.is_empty() => bail!("config: 'endpoint' must not be empty"),
            Some(ep) if !ep.starts_with('/') => {
                bail!("config: 'endpoint' must start with '/' (got '{ep}')")
            }
            _ => {}
        }

        // at least one instance must be defined
        let instances = match self.instances.as_ref() {
            Some(m) if !m.is_empty() => m,
            _ => bail!("config: no instances defined — add at least one entry under 'instances'"),
        };

        // each instance must have a non-empty DSN parseable as a postgres:// URL
        for (name, inst) in instances {
            if inst.dsn.is_empty() {
                bail!("config: instance '{name}': 'dsn' must not be empty");
            }
            if !inst.dsn.starts_with("postgres://") && !inst.dsn.starts_with("postgresql://") {
                bail!(
                    "config: instance '{name}': 'dsn' must start with 'postgres://' or 'postgresql://' (got '{}')",
                    inst.dsn
                );
            }
            if let Some(disabled) = &inst.disable_collectors {
                for col in disabled {
                    if !crate::collectors::COLLECTOR_NAMES.contains(&col.as_str()) {
                        bail!(
                            "config: instance '{name}': unknown collector '{col}' in disable_collectors (known: {})",
                            crate::collectors::COLLECTOR_NAMES.join(", ")
                        );
                    }
                }
            }
        }

        Ok(())
    }

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

        let instances = ec.config.instances.expect("instances should be present");
        let inst = instances
            .get("pg:5432")
            .expect("instance pg:5432 should exist");

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
        let ec = ExporterConfig::load(&path).expect("should load valid config");

        let instances = ec.config.instances.expect("instances should be present");
        let inst = instances
            .get("pg:5432")
            .expect("instance pg:5432 should exist");

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
            scrape_timeout_ms: None,
            instances: None,
            ..Default::default()
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
            scrape_timeout_ms: None,
            instances: None,
            ..Default::default()
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
            scrape_timeout_ms: None,
            instances: None,
            ..Default::default()
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

    // --- PGEConfig::validate ---

    fn valid_cfg() -> PGEConfig {
        let mut instances = HashMap::new();
        instances.insert(
            "pg:5432".to_string(),
            instance::Config {
                dsn: "postgres://u:p@localhost/db".to_string(),
                ..Default::default()
            },
        );
        PGEConfig {
            listen_addr: Some("0.0.0.0:61488".to_string()),
            endpoint: Some("/metrics".to_string()),
            instances: Some(instances),
            ..Default::default()
        }
    }

    #[test]
    fn validate_valid_config_ok() {
        assert!(valid_cfg().validate().is_ok());
    }

    #[test]
    fn validate_missing_listen_addr() {
        let mut cfg = valid_cfg();
        cfg.listen_addr = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_invalid_listen_addr() {
        let mut cfg = valid_cfg();
        cfg.listen_addr = Some("not-an-addr".to_string());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_missing_endpoint() {
        let mut cfg = valid_cfg();
        cfg.endpoint = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_endpoint_without_slash() {
        let mut cfg = valid_cfg();
        cfg.endpoint = Some("metrics".to_string());
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("must start with '/'"));
    }

    #[test]
    fn validate_no_instances() {
        let mut cfg = valid_cfg();
        cfg.instances = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_empty_instances_map() {
        let mut cfg = valid_cfg();
        cfg.instances = Some(HashMap::new());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_empty_dsn() {
        let mut cfg = valid_cfg();
        cfg.instances
            .as_mut()
            .unwrap()
            .get_mut("pg:5432")
            .unwrap()
            .dsn = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_dsn_wrong_scheme() {
        let mut cfg = valid_cfg();
        cfg.instances
            .as_mut()
            .unwrap()
            .get_mut("pg:5432")
            .unwrap()
            .dsn = "mysql://u:p@localhost/db".to_string();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("postgres://"));
    }

    #[test]
    fn validate_postgresql_scheme_ok() {
        let mut cfg = valid_cfg();
        cfg.instances
            .as_mut()
            .unwrap()
            .get_mut("pg:5432")
            .unwrap()
            .dsn = "postgresql://u:p@localhost/db".to_string();
        assert!(cfg.validate().is_ok());
    }
}

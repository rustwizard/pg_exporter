use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::GaugeVec;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use tracing::error;

use crate::collectors::PG;
use crate::instance;

const QUERY: &str = "SELECT name, COALESCE(setting, '') AS setting, \
    COALESCE(unit, '') AS unit, vartype \
    FROM pg_show_all_settings() \
    WHERE source IN ('default','configuration file','override','environment variable','command line','global')";

#[derive(sqlx::FromRow, Debug, Default)]
struct Row {
    name: String,
    setting: String,
    unit: String,
    vartype: String,
}

#[derive(Debug, Clone)]
struct Setting {
    name: String,
    setting: String,
    unit: String,
    vartype: String,
    value: f64,
}

#[derive(Debug, Clone)]
pub struct PGSettingsCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<Vec<Setting>>>,
    descs: Vec<Desc>,
    settings_info: GaugeVec,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGSettingsCollector> {
    match PGSettingsCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            error!("error when create pg settings collector: {}", e);
            None
        }
    }
}

impl PGSettingsCollector {
    fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGSettingsCollector> {
        let mut descs = Vec::new();
        let data = Arc::new(RwLock::new(Vec::<Setting>::new()));

        let var_labels = vec!["name", "setting", "unit", "vartype", "source"];

        let settings_info = GaugeVec::new(
            Opts::new(
                "settings_info",
                "Labeled information about Postgres configuration settings.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("service")
            .const_labels(dbi.labels.clone()),
            &var_labels,
        )?;
        descs.extend(settings_info.desc().into_iter().cloned());

        Ok(PGSettingsCollector {
            dbi,
            data,
            descs,
            settings_info,
        })
    }
}

// Returns (multiplication factor, base unit name).
fn parse_unit(unit: &str) -> (f64, &'static str) {
    if unit.is_empty() {
        return (1.0, "");
    }

    let split = unit.find(|c: char| c.is_alphabetic()).unwrap_or(unit.len());
    let (num_str, suffix) = unit.split_at(split);
    let factor: f64 = if num_str.is_empty() {
        1.0
    } else {
        num_str.parse().unwrap_or(1.0)
    };

    match suffix {
        "B" => (factor, "bytes"),
        "kB" => (factor * 1024.0, "bytes"),
        "MB" => (factor * 1024.0 * 1024.0, "bytes"),
        "GB" => (factor * 1024.0 * 1024.0 * 1024.0, "bytes"),
        "TB" => (factor * 1024.0 * 1024.0 * 1024.0 * 1024.0, "bytes"),
        "ms" => (factor * 0.001, "seconds"),
        "s" => (factor, "seconds"),
        "min" => (factor * 60.0, "seconds"),
        "h" => (factor * 3600.0, "seconds"),
        "d" => (factor * 86400.0, "seconds"),
        _ => (1.0, ""),
    }
}

fn parse_row(row: Row) -> Option<Setting> {
    match row.vartype.as_str() {
        "enum" | "string" => Some(Setting {
            name: row.name,
            setting: row.setting,
            unit: row.unit,
            vartype: row.vartype,
            value: 0.0,
        }),
        "bool" => {
            let value = match row.setting.as_str() {
                "off" => 0.0,
                "on" => 1.0,
                other => {
                    error!("invalid bool value '{}' for setting '{}'", other, row.name);
                    return None;
                }
            };
            Some(Setting {
                name: row.name,
                setting: row.setting,
                unit: row.unit,
                vartype: row.vartype,
                value,
            })
        }
        "integer" | "real" => {
            let (factor, base_unit) = parse_unit(&row.unit);
            let v: f64 = match row.setting.parse() {
                Ok(v) => v,
                Err(e) => {
                    error!("parse setting '{}' value '{}': {}", row.name, row.setting, e);
                    return None;
                }
            };

            // Negative values are special (e.g. old_snapshot_threshold = -1 means disabled).
            let v = if v >= 0.0 { v * factor } else { v };

            let normalized = if row.vartype == "integer" && v >= 1.0 {
                format!("{:.0}", v)
            } else {
                let s = format!("{:.5}", v);
                let s = s.trim_end_matches('0');
                let s = s.trim_end_matches('.');
                if s.is_empty() { "0".to_string() } else { s.to_string() }
            };

            Some(Setting {
                name: row.name,
                setting: normalized,
                unit: base_unit.to_string(),
                vartype: row.vartype,
                value: v,
            })
        }
        other => {
            error!("unknown vartype '{}' for setting '{}'", other, row.name);
            None
        }
    }
}

impl Collector for PGSettingsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut mfs = Vec::with_capacity(1);

        let data_lock = match self.data.read() {
            Ok(lock) => lock,
            Err(e) => {
                error!("pg settings collect: can't acquire read lock: {}", e);
                return mfs;
            }
        };

        for s in data_lock.iter() {
            let vals = [
                s.name.as_str(),
                s.setting.as_str(),
                s.unit.as_str(),
                s.vartype.as_str(),
                "main",
            ];
            self.settings_info.with_label_values(&vals).set(s.value);
        }

        mfs.extend(self.settings_info.collect());
        mfs
    }
}

#[async_trait]
impl PG for PGSettingsCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        self.dbi.ensure_ready().await?;
        let rows = sqlx::query_as::<_, Row>(QUERY)
            .fetch_all(&self.dbi.db)
            .await?;

        let settings: Vec<Setting> = rows.into_iter().filter_map(parse_row).collect();

        let mut data_lock = match self.data.write() {
            Ok(lock) => lock,
            Err(e) => bail!("pg settings collector: can't acquire write lock. {}", e),
        };

        *data_lock = settings;

        Ok(())
    }
}

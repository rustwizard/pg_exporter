use clap::{Parser, Subcommand};
use std::path::PathBuf;

// PgExporter is a PostgreSQL Prometheus metrics exporter.
#[derive(Parser, Debug)]
#[command(name = "", version = concat!("PgExporter v", env!("GIT_HASH")))]
pub struct Cli {
    /// Path to the configuration file. Default: "pg_exporter.yaml"
    #[arg(short, long, default_value = "pg_exporter.yaml")]
    pub config: PathBuf,
    /// Subcommand.
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {}

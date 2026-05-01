use std::sync::Arc;

use prometheus::Registry;

use crate::{collectors, instance};

pub const DEFAULT_SCRAPE_TIMEOUT_MS: u64 = 30_000;

#[derive(Clone)]
pub struct PGEApp {
    pub instances: Vec<Arc<instance::PostgresDB>>,
    pub collectors: Vec<Box<dyn collectors::PG>>,
    pub registry: Registry,
    pub scrape_timeout_ms: u64,
}

impl Default for PGEApp {
    fn default() -> Self {
        Self {
            instances: Vec::new(),
            collectors: Vec::new(),
            registry: Registry::default(),
            scrape_timeout_ms: DEFAULT_SCRAPE_TIMEOUT_MS,
        }
    }
}

impl PGEApp {
    pub fn new() -> Self {
        PGEApp::default()
    }

    pub fn add_collector(&mut self, col: Box<dyn collectors::PG>) {
        self.collectors.push(col);
    }
}

use std::sync::Arc;

use prometheus::Registry;

use crate::{collectors, instance};

#[derive(Clone, Default)]
pub struct PGEApp {
    pub instances: Vec<Arc<instance::PostgresDB>>,
    pub collectors: Vec<Box<dyn collectors::PG>>,
    pub registry: Registry,
}

impl PGEApp {
    pub fn new() -> Self {
        PGEApp::default()
    }

    pub fn add_collector(&mut self, col: Box<dyn collectors::PG>) {
        self.collectors.push(col);
    }
}

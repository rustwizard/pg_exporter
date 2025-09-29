use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::{GaugeVec, IntCounterVec};
use prometheus::{IntGaugeVec, proto};

use crate::collectors::PG;
use crate::instance;

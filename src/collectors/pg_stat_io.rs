use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Counter, CounterVec, IntCounter, IntGauge};

use crate::collectors::{PG, POSTGRES_V17};
use crate::instance;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;

use prometheus::core::{Collector, Desc, Opts};
use prometheus::proto::MetricFamily;
use prometheus::{Counter, CounterVec, IntCounter, IntGauge};

use crate::collectors::{PG, POSTGRES_V17};
use crate::instance;

const POSTGRES_STAT_IO_QUERY17: &str = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time,
		COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, 
		COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, 
		COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, 
		COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, 
		COALESCE(reads, 0) * COALESCE(op_bytes, 0) AS read_bytes, 
		COALESCE(writes, 0) * COALESCE(op_bytes, 0) AS write_bytes, 
		COALESCE(extends, 0) * COALESCE(op_bytes, 0) AS extend_bytes 
		FROM pg_stat_io";

const POSTGRES_STAT_IO_LATEST: &str = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time, 
		COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, 
		COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, 
		COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, 
		COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, 
		COALESCE(read_bytes, 0) AS read_bytes, COALESCE(write_bytes, 0) AS write_bytes, COALESCE(extend_bytes, 0) AS extend_bytes 
		FROM pg_stat_io";
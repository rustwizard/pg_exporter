use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use prometheus::proto;
use prometheus::{
    Counter, IntCounter, IntCounterVec, Opts,
    core::{Collector, Desc},
};

use crate::{collectors::POSTGRES_V17, instance};

use super::PG;

const BGWRITER_QUERY16: &str = "SELECT 
		checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, 
		buffers_checkpoint, buffers_clean, maxwritten_clean, 
		buffers_backend::FLOAT8, buffers_backend_fsync::FLOAT8, buffers_alloc, 
		COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0)::FLOAT8 as bgwr_stats_age_seconds
		FROM pg_stat_bgwriter";

const BGWRITER_QUERY_LATEST: &str = "WITH ckpt AS (
		SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, restartpoints_timed, restartpoints_req, 
		restartpoints_done, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint, 
		COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0)::FLOAT8 as ckpt_stats_age_seconds FROM pg_stat_checkpointer), 
		bgwr AS (
		SELECT buffers_clean, maxwritten_clean, buffers_alloc, 
		COALESCE(EXTRACT(EPOCH FROM age(now(), stats_reset)), 0)::FLOAT8 as bgwr_stats_age_seconds FROM pg_stat_bgwriter), 
		stat_io AS ( 
		SELECT SUM(writes)::FLOAT8 AS buffers_backend, SUM(fsyncs)::FLOAT8 AS buffers_backend_fsync FROM pg_stat_io WHERE backend_type='background writer') 
		SELECT ckpt.*, bgwr.*, stat_io.* FROM ckpt, bgwr, stat_io";

#[derive(sqlx::FromRow, Debug)]
pub struct PGBGwriterStats {
    checkpoints_timed: i64,
    checkpoints_req: i64,
    #[sqlx(default)]
    restartpoints_timed: i64,
    #[sqlx(default)]
    restartpoints_req: i64,
    #[sqlx(default)]
    restartpoints_done: i64,
    checkpoint_write_time: f64,
    checkpoint_sync_time: f64,
    buffers_checkpoint: i64,
    #[sqlx(default)]
    ckpt_stats_age_seconds: f64,
    buffers_clean: i64,
    maxwritten_clean: i64,
    buffers_alloc: i64,
    bgwr_stats_age_seconds: f64,
    buffers_backend: f64,
    buffers_backend_fsync: f64,
}

impl PGBGwriterStats {
    fn new() -> Self {
        PGBGwriterStats {
            checkpoints_timed: (0),
            checkpoints_req: (0),
            restartpoints_timed: (0),
            restartpoints_req: (0),
            restartpoints_done: (0),
            checkpoint_write_time: (0.0),
            checkpoint_sync_time: (0.0),
            buffers_checkpoint: (0),
            ckpt_stats_age_seconds: (0.0),
            buffers_clean: (0),
            maxwritten_clean: (0),
            buffers_alloc: (0),
            bgwr_stats_age_seconds: (0.0),
            buffers_backend: (0.0),
            buffers_backend_fsync: (0.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGBGwriterCollector {
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGBGwriterStats>>,
    descs: Vec<Desc>,
    checkpoints: IntCounterVec,
    checkpoints_all: IntCounter,
    checkpoint_time: IntCounterVec,
    checkpoint_time_all: Counter,
    maxwritten_clean: IntCounter,
    written_bytes: IntCounterVec,
    buffers_backend_fsync: IntCounter,
    alloc_bytes: IntCounter,
    bgwr_stats_age_seconds: IntCounter,
    ckpt_stats_age_seconds: IntCounter,
    checkpoint_restartpointstimed: IntCounter,
    checkpoint_restartpointsreq: IntCounter,
    checkpoint_restartpointsdone: IntCounter,
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> Option<PGBGwriterCollector> {
    match PGBGwriterCollector::new(dbi) {
        Ok(result) => Some(result),
        Err(e) => {
            eprintln!("error when create pg bgwriter collector: {}", e);
            None
        }
    }
}

impl PGBGwriterCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> anyhow::Result<PGBGwriterCollector> {
        let mut descs = Vec::new();

        let checkpoints_total = IntCounterVec::new(
            Opts::new(
                "total",
                "Total number of checkpoints that have been performed of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
            &["checkpoint"],
        )
        .unwrap();

        descs.extend(checkpoints_total.desc().into_iter().cloned());

        let all_total = IntCounter::with_opts(
            Opts::new(
                "all_total",
                "Total number of checkpoints that have been performed of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();

        descs.extend(all_total.desc().into_iter().cloned());

        let seconds_total = IntCounterVec::new(
            Opts::new(
                "seconds_total",
                "Total amount of time that has been spent processing data during checkpoint in each stage, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
            &["stage"],
        )
        .unwrap();
        descs.extend(seconds_total.desc().into_iter().cloned());

        let seconds_all_total = Counter::with_opts(
            Opts::new(
                "seconds_all_total",
                "Total amount of time that has been spent processing data during checkpoint, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(seconds_all_total.desc().into_iter().cloned());

        let bytes_total = IntCounterVec::new(
            Opts::new(
                "bytes_total",
                "Total number of bytes written by each subsystem, in bytes.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("written")
            .const_labels(dbi.labels.clone()),
            &["process"],
        )
        .unwrap();
        descs.extend(bytes_total.desc().into_iter().cloned());

        let maxwritten_clean_total = IntCounter::with_opts(
            Opts::new(
                "maxwritten_clean_total",
                "Total number of times the background writer stopped a cleaning scan because it had written too many buffers.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("bgwriter")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(maxwritten_clean_total.desc().into_iter().cloned());

        let fsync_total = IntCounter::with_opts(
            Opts::new(
                "fsync_total",
                "Total number of times a backends had to execute its own fsync() call.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("backends")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(fsync_total.desc().into_iter().cloned());

        let allocated_bytes_total = IntCounter::with_opts(
            Opts::new(
                "allocated_bytes_total",
                "Total number of bytes allocated by backends.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("backends")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(allocated_bytes_total.desc().into_iter().cloned());

        let bgwr_stats_age_seconds = IntCounter::with_opts(
            Opts::new(
                "stats_age_seconds_total",
                "The age of the background writer activity statistics, in seconds.",
            )
            .namespace(super::NAMESPACE)
            .subsystem("bgwriter")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(bgwr_stats_age_seconds.desc().into_iter().cloned());

        let ckpt_stats_age_seconds = IntCounter::with_opts(
            Opts::new(
                "stats_age_seconds_total",
                "The age of the checkpointer activity statistics, in seconds (since v17).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(ckpt_stats_age_seconds.desc().into_iter().cloned());

        let restartpoints_timed = IntCounter::with_opts(
            Opts::new(
                "restartpoints_timed",
                "Number of scheduled restartpoints due to timeout or after a failed attempt to perform it (since v17).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(restartpoints_timed.desc().into_iter().cloned());

        let restartpoints_req = IntCounter::with_opts(
            Opts::new(
                "restartpoints_req",
                "Number of requested restartpoints (since v17).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(restartpoints_req.desc().into_iter().cloned());

        let restartpoints_done = IntCounter::with_opts(
            Opts::new(
                "restartpoints_done",
                "Number of restartpoints that have been performed (since v17).",
            )
            .namespace(super::NAMESPACE)
            .subsystem("checkpoints")
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(restartpoints_done.desc().into_iter().cloned());

        Ok(PGBGwriterCollector {
            dbi,
            data: Arc::new(RwLock::new(PGBGwriterStats::new())),
            descs,
            checkpoints: checkpoints_total,
            checkpoints_all: all_total,
            checkpoint_time: seconds_total,
            checkpoint_time_all: seconds_all_total,
            maxwritten_clean: maxwritten_clean_total,
            written_bytes: bytes_total,
            buffers_backend_fsync: fsync_total,
            alloc_bytes: allocated_bytes_total,
            bgwr_stats_age_seconds,
            ckpt_stats_age_seconds,
            checkpoint_restartpointstimed: restartpoints_timed,
            checkpoint_restartpointsreq: restartpoints_req,
            checkpoint_restartpointsdone: restartpoints_done,
        })
    }
}

impl Collector for PGBGwriterCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs: Vec<proto::MetricFamily> = Vec::with_capacity(13);

        let data_lock_result = self.data.read();

        if data_lock_result.is_err() {
            println!("collect error: {:?}", data_lock_result.unwrap_err());
            return mfs;
        }

        let data_lock = data_lock_result.unwrap();

        self.alloc_bytes.inc_by(data_lock.buffers_alloc as u64);
        self.bgwr_stats_age_seconds
            .inc_by(data_lock.bgwr_stats_age_seconds as u64);
        self.buffers_backend_fsync
            .inc_by(data_lock.buffers_backend_fsync as u64);
        self.checkpoint_restartpointsdone
            .inc_by(data_lock.restartpoints_done as u64);
        self.checkpoint_restartpointsreq
            .inc_by(data_lock.restartpoints_req as u64);
        self.checkpoint_restartpointstimed
            .inc_by(data_lock.restartpoints_timed as u64);

        self.checkpoints
            .with_label_values(&["timed"])
            .inc_by(data_lock.checkpoints_timed as u64);
        self.checkpoints
            .with_label_values(&["req"])
            .inc_by(data_lock.checkpoints_req as u64);

        self.checkpoint_time_all
            .inc_by(data_lock.checkpoint_write_time + data_lock.checkpoint_sync_time);

        self.checkpoint_time
            .with_label_values(&["write"])
            .inc_by(data_lock.checkpoint_write_time as u64);
        self.checkpoint_time
            .with_label_values(&["sync"])
            .inc_by(data_lock.checkpoint_sync_time as u64);

        self.checkpoints_all
            .inc_by((data_lock.checkpoints_timed + data_lock.checkpoints_req) as u64);

        self.written_bytes
            .with_label_values(&["checkpointer"])
            .inc_by((data_lock.buffers_checkpoint * self.dbi.cfg.pg_block_size) as u64);
        self.written_bytes
            .with_label_values(&["bgwriter"])
            .inc_by((data_lock.buffers_clean * self.dbi.cfg.pg_block_size) as u64);
        self.written_bytes
            .with_label_values(&["backend"])
            .inc_by(data_lock.buffers_backend as u64 * self.dbi.cfg.pg_block_size as u64);

        self.ckpt_stats_age_seconds
            .inc_by(data_lock.ckpt_stats_age_seconds as u64);

        self.maxwritten_clean
            .inc_by(data_lock.maxwritten_clean as u64);

        mfs.extend(self.alloc_bytes.collect());
        mfs.extend(self.bgwr_stats_age_seconds.collect());
        mfs.extend(self.buffers_backend_fsync.collect());
        mfs.extend(self.checkpoint_restartpointsdone.collect());
        mfs.extend(self.checkpoint_restartpointsreq.collect());
        mfs.extend(self.checkpoint_restartpointstimed.collect());
        mfs.extend(self.checkpoint_time.collect());
        mfs.extend(self.checkpoint_time_all.collect());
        mfs.extend(self.checkpoints.collect());
        mfs.extend(self.checkpoints_all.collect());
        mfs.extend(self.ckpt_stats_age_seconds.collect());
        mfs.extend(self.written_bytes.collect());
        mfs.extend(self.maxwritten_clean.collect());

        mfs
    }
}

#[async_trait]
impl PG for PGBGwriterCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        let maybe_bgwr_stats = if self.dbi.cfg.pg_version < POSTGRES_V17 {
            sqlx::query_as::<_, PGBGwriterStats>(BGWRITER_QUERY16)
                .fetch_optional(&self.dbi.db)
                .await?
        } else {
            sqlx::query_as::<_, PGBGwriterStats>(BGWRITER_QUERY_LATEST)
                .fetch_optional(&self.dbi.db)
                .await?
        };

        if let Some(bgwr_stats) = maybe_bgwr_stats {
            let mut data_lock = self.data.write().unwrap();

            data_lock.bgwr_stats_age_seconds = bgwr_stats.bgwr_stats_age_seconds;
            data_lock.buffers_alloc = bgwr_stats.buffers_alloc;
            data_lock.buffers_backend = bgwr_stats.buffers_backend;
            data_lock.buffers_backend_fsync = bgwr_stats.buffers_backend_fsync;
            data_lock.buffers_checkpoint = bgwr_stats.buffers_checkpoint;
            data_lock.buffers_clean = bgwr_stats.buffers_clean;
            data_lock.checkpoint_sync_time = bgwr_stats.checkpoint_sync_time;
            data_lock.checkpoint_write_time = bgwr_stats.checkpoint_write_time;
            data_lock.checkpoints_req = bgwr_stats.checkpoints_req;
            data_lock.checkpoints_timed = bgwr_stats.checkpoints_timed;
            data_lock.maxwritten_clean = bgwr_stats.maxwritten_clean;
            data_lock.restartpoints_done = bgwr_stats.restartpoints_done;
            data_lock.restartpoints_req = bgwr_stats.restartpoints_req;
            data_lock.restartpoints_timed = bgwr_stats.restartpoints_timed;
        }

        Ok(())
    }

    async fn collect(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

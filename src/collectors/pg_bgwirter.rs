use crate::instance;

use super::PG;

const BGWRITER_QUERY16: &str = "SELECT 
		checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, 
		buffers_checkpoint, buffers_clean, maxwritten_clean, 
		buffers_backend, buffers_backend_fsync, buffers_alloc, 
		COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0)::FLOAT8 as bgwr_stats_age_seconds
		FROM pg_stat_bgwriter";

const BGWRITER_QUERY_LATEST: &str = "WITH ckpt AS (
		SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, restartpoints_timed, restartpoints_req, 
		restartpoints_done, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint, 
		COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0) as ckpt_stats_age_seconds FROM pg_stat_checkpointer), 
		bgwr AS (
		SELECT buffers_clean, maxwritten_clean, buffers_alloc, 
		COALESCE(EXTRACT(EPOCH FROM age(now(), stats_reset)), 0) as bgwr_stats_age_seconds FROM pg_stat_bgwriter), 
		stat_io AS ( 
		SELECT SUM(writes) AS buffers_backend, SUM(fsyncs) AS buffers_backend_fsync FROM pg_stat_io WHERE backend_type='background writer') 
		SELECT ckpt.*, bgwr.*, stat_io.* FROM ckpt, bgwr, stat_io";


#[derive(sqlx::FromRow, Debug)]
pub struct PGBGwriterStats16 {
    checkpoints_timed: i64,
    checkpoints_req: i64,
    checkpoint_write_time: f64,
    checkpoint_sync_time: f64,
    buffers_checkpoint: i64,
    buffers_clean: i64,
    maxwritten_clean: i64,
    buffers_backend: i64,
    buffers_backend_fsync: i64,
    buffers_alloc: i64,
    bgwr_stats_age_seconds: f64
}

impl PGBGwriterStats16 {
    fn new() -> Self {
        PGBGwriterStats16 {
            checkpoints_timed: (0),
            checkpoints_req: (0),
            checkpoint_write_time: (0.0),
            checkpoint_sync_time: (0.0),
            buffers_checkpoint: (0),
            buffers_clean: (0),
            maxwritten_clean: (0),
            buffers_backend: (0),
            buffers_backend_fsync: (0),
            buffers_alloc: (0),
            bgwr_stats_age_seconds: (0.0),
        }
    }
}
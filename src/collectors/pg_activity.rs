const ACTIVITY_QUERY: &str = "SELECT " +
		"COALESCE(usename, backend_type) AS user, datname AS database, state, wait_event_type, wait_event, " +
		"COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - xact_start), 0) AS active_seconds, " +
		"CASE WHEN wait_event_type = 'Lock' " +
		"THEN (SELECT EXTRACT(EPOCH FROM clock_timestamp() - MAX(waitstart)) FROM pg_locks l WHERE l.pid = a.pid) " +
		"ELSE 0 END AS waiting_seconds, " +
		"LEFT(query, 32) AS query " +
		"FROM pg_stat_activity a";

const PREPARED_XACT_QUERY: &str = "SELECT count(*) AS total FROM pg_prepared_xacts";

const START_TIME_QUERY: &str = "SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())";


#[derive(sqlx::FromRow, Debug)]
pub struct PGActivityStats {
    start_time_seconds: f64
}
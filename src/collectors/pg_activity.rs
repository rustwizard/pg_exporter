use regex::Regex;
use std::collections::HashMap;

const ACTIVITY_QUERY: &str =
    "SELECT " +
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
    start_time_seconds: f64, // unix time when postmaster has been started
    query_select: f64, // number of select queries: SELECT, TABLE
    query_mod: f64, // number of DML: INSERT, UPDATE, DELETE, TRUNCATE
    query_ddl: f64, // number of DDL queries: CREATE, ALTER, DROP
    query_maint: f64, // number of maintenance queries: VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH, CHECKPOINT
    query_with: f64, // number of CTE queries
    query_copy: f64, // number of COPY queries
    query_other: f64, // number of queries of other types: BEGIN, END, COMMIT, ABORT, SET, etc...

    vacuum_ops: HashMap<String, i64>, // vacuum operations by type

    max_idle_user: HashMap<String, i64>, // longest duration among idle transactions opened by user/database
    max_idle_maint: HashMap<String, i64>, // longest duration among idle transactions initiated by maintenance operations (autovacuum, vacuum. analyze)
    max_active_user: HashMap<String, i64>, // longest duration among client queries
    max_active_maint: HashMap<String, i64>, // longest duration among maintenance operations (autovacuum, vacuum. analyze)
    max_wait_user: HashMap<String, i64>, // longest duration being in waiting state (all activity)
    max_wait_maint: HashMap<String, i64>, // longest duration being in waiting state (all activity)

    idle: HashMap<String, i64>, // state = 'idle'
    idlexact: HashMap<String, i64>, // state IN ('idle in transaction', 'idle in transaction (aborted)'))
    active: HashMap<String, i64>, // state = 'active'
    other: HashMap<String, i64>, // state IN ('fastpath function call','disabled')
    waiting: HashMap<String, i64>, // wait_event_type = 'Lock' (or waiting = 't')
    wait_events: HashMap<String, i64>, // wait_event_type/wait_event counters
}

struct queryRegexp {
    // query regexps
    selects: Regex, // SELECT|TABLE
    modify: Regex, // INSERT|UPDATE|DELETE|TRUNCATE
    ddl: Regex, // CREATE|ALTER|DROP
    maint: Regex, // ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT
    vacuum: Regex, // VACUUM|autovacuum: .+
    vacanl: Regex, // VACUUM|ANALYZE|autovacuum:
    with: Regex, // WITH
    copy: Regex, // COPY
}

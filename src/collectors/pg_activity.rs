use regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::Gauge;
use prometheus::{proto, GaugeVec};

use crate::instance;

use super::PG;

const ACTIVITY_QUERY: &str = "SELECT 
    COALESCE(usename, backend_type) AS user, datname AS database, state, wait_event_type, wait_event, 
    COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - xact_start), 0) AS active_seconds, 
    CASE WHEN wait_event_type = 'Lock' 
    THEN (SELECT EXTRACT(EPOCH FROM clock_timestamp() - MAX(waitstart)) FROM pg_locks l WHERE l.pid = a.pid) 
    ELSE 0 END AS waiting_seconds,
    LEFT(query, 32) AS query 
    FROM pg_stat_activity a";

const PREPARED_XACT_QUERY: &str = "SELECT count(*) AS total FROM pg_prepared_xacts";

const START_TIME_QUERY: &str = "SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())";

const ACTIVITY_SUBSYSTEM: &str = "activity";

#[derive(sqlx::FromRow, Debug)]
pub struct PGActivityStats {
    start_time_seconds: f64, // unix time when postmaster has been started
    query_select: f64,       // number of select queries: SELECT, TABLE
    query_mod: f64,          // number of DML: INSERT, UPDATE, DELETE, TRUNCATE
    query_ddl: f64,          // number of DDL queries: CREATE, ALTER, DROP
    query_maint: f64, // number of maintenance queries: VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH, CHECKPOINT
    query_with: f64,  // number of CTE queries
    query_copy: f64,  // number of COPY queries
    query_other: f64, // number of queries of other types: BEGIN, END, COMMIT, ABORT, SET, etc...
    prepared: f64,    // FROM pg_prepared_xacts

    vacuum_ops: HashMap<String, i64>, // vacuum operations by type

    max_idle_user: HashMap<String, i64>, // longest duration among idle transactions opened by user/database
    max_idle_maint: HashMap<String, i64>, // longest duration among idle transactions initiated by maintenance operations (autovacuum, vacuum. analyze)
    max_active_user: HashMap<String, i64>, // longest duration among client queries
    max_active_maint: HashMap<String, i64>, // longest duration among maintenance operations (autovacuum, vacuum. analyze)
    max_wait_user: HashMap<String, i64>, // longest duration being in waiting state (all activity)
    max_wait_maint: HashMap<String, i64>, // longest duration being in waiting state (all activity)

    idle: HashMap<String, i64>,        // state = 'idle'
    idlexact: HashMap<String, i64>, // state IN ('idle in transaction', 'idle in transaction (aborted)'))
    active: HashMap<String, i64>,   // state = 'active'
    other: HashMap<String, i64>,    // state IN ('fastpath function call','disabled')
    waiting: HashMap<String, i64>,  // wait_event_type = 'Lock' (or waiting = 't')
    wait_events: HashMap<String, i64>, // wait_event_type/wait_event counters

    re: QueryRegexp,
}

impl PGActivityStats {
    pub fn new() -> PGActivityStats {
        PGActivityStats {
            start_time_seconds: (0.0),
            query_select: (0.0),
            query_mod: (0.0),
            query_ddl: (0.0),
            query_maint: (0.0),
            query_with: (0.0),
            query_copy: (0.0),
            query_other: (0.0),
            prepared: (0.0),
            vacuum_ops: HashMap::new(),
            max_idle_user: HashMap::new(),
            max_idle_maint: HashMap::new(),
            max_active_user: HashMap::new(),
            max_active_maint: HashMap::new(),
            max_wait_user: HashMap::new(),
            max_wait_maint: HashMap::new(),
            idle: HashMap::new(),
            idlexact: HashMap::new(),
            active: HashMap::new(),
            other: HashMap::new(),
            waiting: HashMap::new(),
            wait_events: HashMap::new(),
            re: QueryRegexp::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct QueryRegexp {
    // query regexps
    selects: Regex, // SELECT|TABLE
    modify: Regex,  // INSERT|UPDATE|DELETE|TRUNCATE
    ddl: Regex,     // CREATE|ALTER|DROP
    maint: Regex,   // ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT
    vacuum: Regex,  // VACUUM|autovacuum: .+
    vacanl: Regex,  // VACUUM|ANALYZE|autovacuum:
    with: Regex,    // WITH
    copy: Regex,    // COPY
}

impl QueryRegexp {
    fn new() -> QueryRegexp {
        QueryRegexp {
            selects: Regex::new("^(?i)(SELECT|TABLE)").unwrap(),
            modify: Regex::new("^(?i)(INSERT|UPDATE|DELETE|TRUNCATE)").unwrap(),
            ddl: Regex::new("^(?i)(CREATE|ALTER|DROP)").unwrap(),
            maint: Regex::new("^(?i)(ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT)").unwrap(),
            vacuum: Regex::new("^(?i)(VACUUM|autovacuum: .+)").unwrap(),
            vacanl: Regex::new("^(?i)(VACUUM|ANALYZE|autovacuum:)").unwrap(),
            with: Regex::new("^(?i)WITH").unwrap(),
            copy: Regex::new("^(?i)COPY").unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PGActivityCollector {
    dbi: instance::PostgresDB,
    data: Arc<RwLock<PGActivityStats>>,
    descs: Vec<Desc>,
    up: Gauge,
    start_time: Gauge,
    wait_events: GaugeVec,
    states: GaugeVec,
    states_all: Gauge,
    activity: GaugeVec,
    prepared: Gauge,
    inflight: GaugeVec,
    vacuums: GaugeVec,
}

impl PGActivityCollector {
    pub fn new(dbi: instance::PostgresDB) -> PGActivityCollector {
        let up = Gauge::with_opts(
            Opts::new("up", "State of PostgreSQL service: 0 is down, 1 is up.")
                .namespace(super::NAMESPACE)
                .const_labels(dbi.labels.clone()),
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(up.desc().into_iter().cloned());

        let start_time = Gauge::with_opts(
            Opts::new("start_time_seconds", "Postgres start time, in unixtime.")
                .namespace(super::NAMESPACE)
                .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(start_time.desc().into_iter().cloned());

        let wait_events = GaugeVec::new(
            Opts::new(
                "wait_events_in_flight",
                "Number of wait events in-flight in each state.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["type", "event"],
        )
        .unwrap();
        descs.extend(wait_events.desc().into_iter().cloned());

        let states = GaugeVec::new(
            Opts::new(
                "connections_in_flight",
                "Number of connections in-flight in each state.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["user", "database", "state"],
        )
        .unwrap();
        descs.extend(states.desc().into_iter().cloned());

        let states_all = Gauge::with_opts(
            Opts::new(
                "connections_all_in_flight",
                "Number of all connections in-flight.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(states_all.desc().into_iter().cloned());

        let activity = GaugeVec::new(
            Opts::new(
                "max_seconds",
                "Longest activity for each user, database and activity type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["user", "database", "state", "type"],
        )
        .unwrap();
        descs.extend(activity.desc().into_iter().cloned());

        let prepared = Gauge::with_opts(
            Opts::new(
                "prepared_transactions_in_flight",
                "Number of transactions that are currently prepared for two-phase commit.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
        )
        .unwrap();
        descs.extend(prepared.desc().into_iter().cloned());

        let inflight = GaugeVec::new(
            Opts::new(
                "queries_in_flight",
                "Number of queries running in-flight of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["type"]
        )
        .unwrap();
        descs.extend(inflight.desc().into_iter().cloned());

        let vacuums = GaugeVec::new(
            Opts::new(
                "vacuums_in_flight",
                "Number of vacuum operations running in-flight of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["type"]
        )
        .unwrap();
        descs.extend(vacuums.desc().into_iter().cloned());

        PGActivityCollector {
            dbi: dbi,
            data: Arc::new(RwLock::new(PGActivityStats::new())),
            descs: descs,
            up: up,
            start_time: start_time,
            wait_events: wait_events,
            states: states,
            states_all: states_all,
            activity: activity,
            prepared: prepared,
            inflight: inflight,
            vacuums: vacuums,
        }
    }
}

#[async_trait]
impl PG for PGActivityCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        // get pg_prepared_xacts stats
        let prepared: f64 = sqlx::query_scalar(PREPARED_XACT_QUERY)
            .fetch_one(&self.dbi.db)
            .await?;

        let start_time: f64 = sqlx::query_scalar(START_TIME_QUERY)
            .fetch_one(&self.dbi.db)
            .await?;

        let mut data_lock = self.data.write().unwrap();
        data_lock.prepared = prepared;
        data_lock.start_time_seconds = start_time;

        Ok(())
    }
}

pub fn new(dbi: instance::PostgresDB) -> PGActivityCollector {
    PGActivityCollector::new(dbi)
}

impl Collector for PGActivityCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::new();

        let data_lock = self.data.read().unwrap();

        // TODO: set collected metrics

        // All activity metrics collected successfully, now we can collect up metric.
        self.start_time.set(data_lock.start_time_seconds);
        self.prepared.set(data_lock.prepared);

        mfs.extend(self.up.collect());
        mfs.extend(self.start_time.collect());
        mfs.extend(self.prepared.collect());

        mfs
    }
}

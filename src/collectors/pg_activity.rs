use regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use anyhow::anyhow;
use async_trait::async_trait;
use prometheus::core::{Collector, Desc, Opts};
use prometheus::{proto, GaugeVec, IntGaugeVec};
use prometheus::{Gauge, IntGauge};

use crate::instance;

use super::PG;

const ACTIVITY_QUERY: &str = "SELECT 
    COALESCE(usename, backend_type) AS user, datname AS database, state, wait_event_type, wait_event, 
    COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - xact_start), 0)::FLOAT8 AS active_seconds, 
    CASE WHEN wait_event_type = 'Lock' 
    THEN (SELECT EXTRACT(EPOCH FROM clock_timestamp() - MAX(waitstart))::FLOAT8 FROM pg_locks l WHERE l.pid = a.pid) 
    ELSE 0 END AS waiting_seconds,
    LEFT(query, 32) AS query 
    FROM pg_stat_activity a";

const PREPARED_XACT_QUERY: &str = "SELECT count(*) AS total FROM pg_prepared_xacts";

const START_TIME_QUERY: &str = "SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())::FLOAT8";

const ACTIVITY_SUBSYSTEM: &str = "activity";

// Backend states accordingly to pg_stat_activity.state
const ST_ACTIVE: &str = "active";
const ST_IDLE: &str = "idle";
const ST_IDLE_XACT: &str = "idle in transaction";
const ST_IDLE_XACT_ABORTED: &str = "idle in transaction (aborted)";
const ST_FAST_PATH: &str = "fastpath function call";
const ST_DISABLED: &str = "disabled";
const ST_WAITING: &str = "waiting"; // fake state based on 'wait_event_type == Lock'

// Wait event type names
const WE_LOCK: &str = "Lock";

#[derive(sqlx::FromRow, Debug)]
pub struct PGActivityStats {
    start_time_seconds: f64, // unix time when postmaster has been started
    query_select: i64,       // number of select queries: SELECT, TABLE
    query_mod: i64,          // number of DML: INSERT, UPDATE, DELETE, TRUNCATE
    query_ddl: i64,          // number of DDL queries: CREATE, ALTER, DROP
    query_maint: i64, // number of maintenance queries: VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH, CHECKPOINT
    query_with: i64,  // number of CTE queries
    query_copy: i64,  // number of COPY queries
    query_other: i64, // number of queries of other types: BEGIN, END, COMMIT, ABORT, SET, etc...
    prepared: i64,    // FROM pg_prepared_xacts

    vacuum_ops: HashMap<String, i64>, // vacuum operations by type

    max_idle_user: HashMap<String, f64>, // longest duration among idle transactions opened by user/database
    max_idle_maint: HashMap<String, f64>, // longest duration among idle transactions initiated by maintenance operations (autovacuum, vacuum. analyze)
    max_active_user: HashMap<String, f64>, // longest duration among client queries
    max_active_maint: HashMap<String, f64>, // longest duration among maintenance operations (autovacuum, vacuum. analyze)
    max_wait_user: HashMap<String, f64>, // longest duration being in waiting state (all activity)
    max_wait_maint: HashMap<String, f64>, // longest duration being in waiting state (all activity)

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
            query_select: (0),
            query_mod: (0),
            query_ddl: (0),
            query_maint: (0),
            query_with: (0),
            query_copy: (0),
            query_other: (0),
            prepared: (0),
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

    pub fn update_state(&mut self, usename: &str, datname: &str, state: &str) {
        let key = format!("{}{}{}", usename, "/", datname);

        match state {
            ST_ACTIVE => {
                if let Some(count) = self.active.get(&key) {
                    self.active.insert(key, count + 1);
                } else {
                    self.active.insert(key, 1);
                }
            }

            ST_IDLE => {
                if let Some(count) = self.idle.get(&key) {
                    self.idle.insert(key, count + 1);
                } else {
                    self.idle.insert(key, 1);
                }
            }

            ST_IDLE_XACT | ST_IDLE_XACT_ABORTED => {
                if let Some(count) = self.idlexact.get(&key) {
                    self.idlexact.insert(key, count + 1);
                } else {
                    self.idlexact.insert(key, 1);
                }
            }

            ST_FAST_PATH | ST_DISABLED => {
                if let Some(count) = self.other.get(&key) {
                    self.other.insert(key, count + 1);
                } else {
                    self.other.insert(key, 1);
                }
            }

            ST_WAITING => {
                if let Some(count) = self.waiting.get(&key) {
                    self.waiting.insert(key, count + 1);
                } else {
                    self.waiting.insert(key, 1);
                }
            }

            _ => println!("pg activity stats: unknown state"),
        }
    }

    pub fn update_wait_events(&mut self, ev_type: &str, state: &str) {
        let key = format!("{}{}{}", ev_type, "/", state);
        if let Some(count) = self.wait_events.get(&key) {
            self.wait_events.insert(key, count + 1);
        } else {
            self.wait_events.insert(key, 1);
        }
    }

    pub fn update_max_idletime_duration(
        &mut self,
        value: f64,
        usename: &Option<String>,
        datname: &Option<String>,
        state: &Option<String>,
        query: &Option<String>,
    ) {
        // necessary values should not be empty (except wait_event_type)
        if state.is_none() || query.is_none() {
            return;
        }

        if let Some(state) = state {
            if state != ST_IDLE_XACT && state != ST_IDLE_XACT_ABORTED {
                return;
            }
        }

        // all validations ok, update stats

        // inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
        let key = format!(
            "{}{}{}",
            usename.clone().unwrap(),
            "/",
            datname.clone().unwrap()
        );

        if self.re.vacanl.is_match(&query.clone().unwrap()) {
            let v = self.max_idle_maint.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_idle_maint.insert(key, value);
                }
            } else {
                self.max_idle_maint.insert(key, value);
            }
        } else {
            let v = self.max_idle_user.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_idle_user.insert(key, value);
                }
            } else {
                self.max_idle_user.insert(key, value);
            }
        }
    }

    pub fn update_max_runtime_duration(
        &mut self,
        value: f64,
        usename: &Option<String>,
        datname: &Option<String>,
        state: &Option<String>,
        etype: &Option<String>,
        query: &Option<String>,
    ) {
        // necessary values should not be empty (except wait_event_type)
        if state.is_none() || query.is_none() {
            return;
        }

        if let Some(state) = state {
            let ev_type = etype.clone().or(Some("".to_string()));
            if state != ST_ACTIVE || ev_type.unwrap() == WE_LOCK {
                return;
            }
        }

        // inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
        let key = format!(
            "{}{}{}",
            usename.clone().unwrap(),
            "/",
            datname.clone().unwrap()
        );

        if self.re.vacanl.is_match(&query.clone().unwrap()) {
            let v = self.max_active_maint.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_active_maint.insert(key, value);
                }
            } else {
                self.max_active_maint.insert(key, value);
            }
        } else {
            let v = self.max_active_user.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_active_user.insert(key, value);
                }
            } else {
                self.max_active_user.insert(key, value);
            }
        }
    }

    pub fn update_max_waitime_duration(
        &mut self,
        value: f64,
        usename: &Option<String>,
        datname: &Option<String>,
        waiting: &Option<String>,
        query: &Option<String>,
    ) {
        if waiting.is_none() || query.is_none() {
            return;
        }

        if let Some(wait) = waiting {
            // waiting activity is considered only with wait_event_type = 'Lock' (or waiting = 't')
            if *wait != WE_LOCK && *wait != "t" {
                return;
            }
        }

        let key = format!(
            "{}{}{}",
            usename.clone().unwrap(),
            "/",
            datname.clone().unwrap()
        );

        if self.re.vacanl.is_match(&query.clone().unwrap()) {
            let v = self.max_wait_maint.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_wait_maint.insert(key, value);
                }
            } else {
                self.max_wait_maint.insert(key, value);
            }
        } else {
            let v = self.max_wait_user.get(&key);
            if v.is_some() {
                if value > *v.unwrap() {
                    self.max_wait_user.insert(key, value);
                }
            } else {
                self.max_wait_user.insert(key, value);
            }
        }
    }

    fn update_query_stat(&mut self, query: &Option<String>, state: &Option<String>) {
        if query.is_none() || state.is_none() {
            return;
        }

        if state.clone().unwrap() != ST_ACTIVE {
            return;
        }

        if self.re.selects.is_match(&query.clone().unwrap()) {
            self.query_select += 1;
            return;
        }

        if self.re.modify.is_match(&query.clone().unwrap()) {
            self.query_mod += 1;
            return;
        }

        if self.re.ddl.is_match(&query.clone().unwrap()) {
            self.query_ddl += 1;
            return;
        }

        if self.re.maint.is_match(&query.clone().unwrap()) {
            self.query_maint += 1;
            return;
        }

        let binding = query.clone().unwrap();
        let maybe_str = self.re.vacuum.find(&binding);

        let mut str: &str = "";

        if let Some(s) = maybe_str {
            str = s.as_str();
        }

        if !str.is_empty() {
            self.query_maint += 1;
            
            if str.starts_with("autovacuum:") && str.contains("(to prevent wraparound)") {
                *self.vacuum_ops.entry("wraparound".to_string()).or_insert(0) += 1;
                return;
            }

            if str.starts_with("autovacuum:") {
                *self.vacuum_ops.entry("regular".to_string()).or_insert(0) += 1;
                return;
            }

            *self.vacuum_ops.entry("user".to_string()).or_insert(0) += 1;
            return;
        }

        if self.re.with.is_match(&query.clone().unwrap()) {
            self.query_with += 1;
            return;
        }

        if self.re.copy.is_match(&query.clone().unwrap()) {
            self.query_copy += 1;
            return;
        }

        // still here? ok, increment others and return
	    self.query_other += 1;
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
    dbi: Arc<instance::PostgresDB>,
    data: Arc<RwLock<PGActivityStats>>,
    descs: Vec<Desc>,
    up: Gauge,
    start_time: Gauge,
    wait_events: IntGaugeVec,
    states: IntGaugeVec,
    states_all: IntGauge,
    activity: GaugeVec,
    prepared: IntGauge,
    inflight: IntGaugeVec,
    vacuums: IntGaugeVec,
}

impl PGActivityCollector {
    pub fn new(dbi: Arc<instance::PostgresDB>) -> PGActivityCollector {
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

        let wait_events = IntGaugeVec::new(
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

        let states = IntGaugeVec::new(
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

        let states_all = IntGauge::with_opts(
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

        let prepared = IntGauge::with_opts(
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

        let inflight = IntGaugeVec::new(
            Opts::new(
                "queries_in_flight",
                "Number of queries running in-flight of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["type"],
        )
        .unwrap();
        descs.extend(inflight.desc().into_iter().cloned());

        let vacuums = IntGaugeVec::new(
            Opts::new(
                "vacuums_in_flight",
                "Number of vacuum operations running in-flight of each type.",
            )
            .namespace(super::NAMESPACE)
            .subsystem(ACTIVITY_SUBSYSTEM)
            .const_labels(dbi.labels.clone()),
            &["type"],
        )
        .unwrap();
        descs.extend(vacuums.desc().into_iter().cloned());

        PGActivityCollector {
            dbi,
            data: Arc::new(RwLock::new(PGActivityStats::new())),
            descs,
            up,
            start_time,
            wait_events,
            states,
            states_all,
            activity,
            prepared,
            inflight,
            vacuums,
        }
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PGActivity {
    user: Option<String>,
    database: Option<String>,
    state: Option<String>,
    wait_event_type: Option<String>,
    wait_event: Option<String>,
    active_seconds: Option<f64>,
    waiting_seconds: Option<f64>,
    query: Option<String>,
}

#[async_trait]
impl PG for PGActivityCollector {
    async fn update(&self) -> Result<(), anyhow::Error> {
        //get pg_prepared_xacts stats
        let prepared = sqlx::query_scalar::<_, i64>(PREPARED_XACT_QUERY)
            .fetch_one(&self.dbi.db)
            .await?;

        let start_time: f64 = sqlx::query_scalar(START_TIME_QUERY)
            .fetch_one(&self.dbi.db)
            .await?;

        let pg_activity_rows: Vec<PGActivity> = sqlx::query_as(ACTIVITY_QUERY)
            .fetch_all(&self.dbi.db)
            .await?;

        let data_lock_result = self.data.write();

        if data_lock_result.is_err() {
            println!("error: {:?}", data_lock_result.unwrap_err());
            return Err(anyhow!("can't acuire data lock"));
        }

        let mut data_lock = data_lock_result.unwrap();

        // clear all previous states
        data_lock.active.clear();
        data_lock.idle.clear();
        data_lock.idlexact.clear();
        data_lock.other.clear();
        data_lock.waiting.clear();
        data_lock.wait_events.clear();
        data_lock.query_select = 0;
        data_lock.query_mod = 0;
        data_lock.query_copy = 0;
        data_lock.query_ddl = 0;
        data_lock.query_maint = 0;
        data_lock.query_other = 0;
        data_lock.query_with = 0;

        for activity in &pg_activity_rows {
            if let Some(u) = &activity.user {
                if let Some(d) = &activity.database {
                    if let Some(s) = &activity.state {
                        data_lock.update_state(u.as_str(), d.as_str(), s.as_str());
                    }
                }
            }

            if let Some(asec) = &activity.active_seconds {
                data_lock.update_max_idletime_duration(
                    *asec,
                    &activity.user,
                    &activity.database,
                    &activity.state,
                    &activity.query,
                );
                data_lock.update_max_runtime_duration(
                    *asec,
                    &activity.user,
                    &activity.database,
                    &activity.state,
                    &activity.wait_event_type,
                    &activity.query,
                );
            }

            if let Some(wait) = &activity.waiting_seconds {
                data_lock.update_max_waitime_duration(
                    *wait,
                    &activity.user,
                    &activity.database,
                    &activity.wait_event_type,
                    &activity.query,
                );
            }

            if let Some(we) = &activity.wait_event_type {
                // Count waiting activity only if waiting = 't' or wait_event_type = 'Lock'.
                if we == WE_LOCK || we == "t" {
                    data_lock.update_state(
                        &activity.user.clone().unwrap(),
                        &activity.database.clone().unwrap(),
                        "waiting",
                    );
                }

                data_lock.update_wait_events(we, &activity.wait_event.clone().unwrap());
            }

            data_lock.update_query_stat(&activity.query, &activity.state);
        }

        let states: HashMap<&str, &HashMap<String, i64>> = HashMap::from([
            ("active", &data_lock.active),
            ("idle", &data_lock.idle),
            ("idlexact", &data_lock.idlexact),
            ("other", &data_lock.other),
            ("waiting", &data_lock.waiting),
        ]);

        // connection states
        let mut total: i64 = 0;
        for (tag, values) in states {
            for (k, v) in values {
                let names: Vec<&str> = k.split("/").collect();
                if names.len() >= 2 {
                    // totals shouldn't include waiting state, because it's already included in 'active' state.
                    if tag != "waiting" {
                        total += v
                    }
                } else {
                    println!("create state '{:?}' activity failed: insufficient number of fields in key '{:?}'; skip", tag, k);
                }
            }
        }

        data_lock.prepared = prepared;
        data_lock.start_time_seconds = start_time;

        Ok(())
    }
}

pub fn new(dbi: Arc<instance::PostgresDB>) -> PGActivityCollector {
    PGActivityCollector::new(dbi)
}

impl Collector for PGActivityCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(9);

        let data_lock_result = self.data.read();

        if data_lock_result.is_err() {
            println!("collect error: {:?}", data_lock_result.unwrap_err());
            return mfs;
        }

        let data_lock = data_lock_result.unwrap();

        let states: HashMap<&str, &HashMap<String, i64>> = HashMap::from([
            ("active", &data_lock.active),
            ("idle", &data_lock.idle),
            ("idlexact", &data_lock.idlexact),
            ("other", &data_lock.other),
            ("waiting", &data_lock.waiting),
        ]);

        // connection states
        let mut total: i64 = 0;
        for (tag, values) in states {
            for (k, v) in values {
                let names: Vec<&str> = k.split("/").collect();
                if names.len() >= 2 {
                    self.states
                        .with_label_values(&[names[0], names[1], tag])
                        .set(*v);

                    // totals shouldn't include waiting state, because it's already included in 'active' state.
                    if tag != "waiting" {
                        total += v
                    }
                } else {
                    println!("create state '{:?}' activity failed: insufficient number of fields in key '{:?}'; skip", tag, k);
                }
            }
        }

        let maint_states: HashMap<&str, &HashMap<String, f64>> = HashMap::from([
            ("idlexact/user", &data_lock.max_idle_user),
            ("idlexact/maintenance", &data_lock.max_idle_maint),
            ("active/user", &data_lock.max_active_user),
            ("active/maintenance", &data_lock.max_active_maint),
            ("waiting/user", &data_lock.max_wait_user),
            ("waiting/maintenance", &data_lock.max_wait_maint),
        ]);

        for (tag, values) in maint_states {
            for (k, v) in values {
                let names: Vec<&str> = k.split("/").collect();
                if names.len() >= 2 {
                    let ff: Vec<&str> = tag.split("/").collect();
                    self.activity
                        .with_label_values(&[names[0], names[1], ff[0], ff[1]])
                        .set(*v);
                } else {
                    println!("create state '{:?}' activity failed: insufficient number of fields in key '{:?}'; skip", tag, k);
                }
            }
        }

        // wait_events
        for (k, v) in &data_lock.wait_events {
            let labels: Vec<&str> = k.split("/").collect();
            if labels.len() >= 2 {
                self.wait_events
                    .with_label_values(&[labels[0], labels[1]])
                    .set(*v)
            } else {
                println!(
                    "create wait_event activity failed: invalid input '{:?}'; skip",
                    k
                );
            }
        }

        // in flight queries
        self.inflight.with_label_values(&["select"]).set(data_lock.query_select);
        self.inflight.with_label_values(&["mod"]).set(data_lock.query_mod);
        self.inflight.with_label_values(&["ddl"]).set(data_lock.query_ddl);
        self.inflight.with_label_values(&["maintenance"]).set(data_lock.query_maint);
        self.inflight.with_label_values(&["with"]).set(data_lock.query_with);
        self.inflight.with_label_values(&["copy"]).set(data_lock.query_copy);
        self.inflight.with_label_values(&["other"]).set(data_lock.query_other);

        // vacuums
        for (k, v) in &data_lock.vacuum_ops {
            self.vacuums.with_label_values(&[k]).set(*v);
        }


        // All activity metrics collected successfully, now we can collect up metric.
        self.up.set(1.0);
        self.start_time.set(data_lock.start_time_seconds);
        self.prepared.set(data_lock.prepared);
        self.states_all.set(total);

        mfs.extend(self.up.collect());
        mfs.extend(self.start_time.collect());
        mfs.extend(self.prepared.collect());
        mfs.extend(self.states.collect());
        mfs.extend(self.states_all.collect());
        mfs.extend(self.activity.collect());
        mfs.extend(self.wait_events.collect());
        mfs.extend(self.inflight.collect());
        mfs.extend(self.vacuums.collect());

        mfs
    }
}

# pg_exporter — Architecture (C4 Model)

## Level 1 — System Context

Who uses the system and what it talks to.

```mermaid
graph TD
    SRE["👤 SRE / Operator\nMonitors PostgreSQL health\nvia dashboards and alerts"]

    subgraph pg_exporter_sys ["pg_exporter [System]"]
        EXP["pg_exporter\nCollects metrics from one or more\nPostgreSQL instances and exposes\nthem in Prometheus format over HTTP"]
    end

    PROM["Prometheus\nScrapes metrics, stores\ntime-series, evaluates alerts"]
    GRAF["Grafana\nVisualises Prometheus data;\n9 pre-provisioned dashboards"]
    PG["PostgreSQL\nOne or more instances\n(versions 9.5–18)"]

    PROM -->|"GET /metrics [HTTP]"| EXP
    EXP -->|"SQL queries via pg_stat_* views [TCP/sqlx]"| PG
    GRAF -->|"PromQL [HTTP]"| PROM
    SRE -->|"views dashboards"| GRAF
    SRE -->|"queries metrics, manages alerts"| PROM
```

---

## Level 2 — Containers

Processes and datastores that make up the running system.

```mermaid
graph TD
    SRE["👤 SRE / Operator"]

    subgraph docker ["Docker Compose environment"]
        EXP["pg_exporter\n[Rust · Actix-web · :61488]\nHTTP server. On every scrape,\nruns all collectors in parallel,\ngathers results, returns\nPrometheus text format."]

        PROM["Prometheus\n[Go · :61490]\nScrapes pg_exporter.\nStores time-series.\nEvaluates alert rules."]

        GRAF["Grafana\n[Go · :61491]\nPre-provisioned dashboards:\nactivity, WAL, I/O, replication,\nstatements, tables, storage,\nsettings, process."]

        PG1[("PostgreSQL 15\n[pge15 · :5432]\npg_stat_statements\ntrack_io_timing=on")]
        PGN[("PostgreSQL 17\n[pge17 · :6432]\npg_stat_statements\ntrack_io_timing=on")]
    end

    PROM -->|"GET /metrics every N sec"| EXP
    EXP -->|"version-aware SQL queries"| PG1
    EXP -->|"version-aware SQL queries"| PGN
    GRAF -->|"PromQL"| PROM
    SRE --> GRAF
    SRE --> PROM
```

---

## Level 3 — Components

Components inside the `pg_exporter` binary.

```mermaid
graph TD
    PROM["Prometheus"]

    subgraph binary ["pg_exporter binary"]
        CLI["CLI & Config\n[clap · config crate]\nParses flags, loads pg_exporter.yml,\napplies PGE_* env overrides.\nProduces ExporterConfig."]

        ROUTER["HTTP Router\n[Actix-web]\nGET /  → hello\nGET /metrics → metrics handler"]

        HANDLER["Metrics Handler\n[async fn metrics()]\nSpawns tokio task per collector.\nWaits for all tasks.\nGathers from PG registry +\nglobal process registry.\nEncodes to Prometheus text."]

        COLLECTORS["Collector Set\n[16 structs · trait PG + prometheus::Collector]\nOne set per PostgreSQL instance.\nEach holds Arc&lt;PostgresDB&gt; +\nArc&lt;RwLock&lt;Vec&lt;Row&gt;&gt;&gt;.\nupdate() writes rows;\ncollect() resets + re-populates.\nVersion-aware: new() → None if unsupported."]

        INSTANCE["Instance Manager\n[PostgresDB · SQLx PgPool]\nLazy pool (max 10 conns, 5s timeout).\nCaches PGConfig on first ensure_ready():\nversion, block_size, wal_segment_size,\npg_stat_statements availability.\nOne instance per DSN."]

        REGISTRY["Prometheus Registry\n[prometheus::Registry]\nIsolated registry for PG metrics.\nCollectors register Desc at startup.\nProcess metrics → global registry."]
    end

    PG[("PostgreSQL")]

    CLI -->|"configures address and endpoint"| ROUTER
    ROUTER -->|"dispatches GET /metrics"| HANDLER
    HANDLER -->|"spawns update() per collector"| COLLECTORS
    COLLECTORS -->|"ensure_ready() · query_as()"| INSTANCE
    INSTANCE -->|"SQL"| PG
    COLLECTORS -->|"register Desc on startup\ncollect() on gather"| REGISTRY
    HANDLER -->|"registry.gather()"| REGISTRY
    PROM -->|"GET /metrics"| ROUTER
```

---

## Level 4 — Code

The collector pattern that all 16 collectors follow.

### Structs and trait relationships

```
┌─────────────────────────────────────────────────────────────┐
│  PGFooCollector                                             │
│                                                             │
│  dbi:  Arc<PostgresDB>       ← shared connection pool      │
│  data: Arc<RwLock<Vec<Row>>> ← snapshot of last query      │
│  descs: Vec<Desc>            ← metric descriptors          │
│  my_gauge: GaugeVec          ← Prometheus metric           │
├─────────────────────────────────────────────────────────────┤
│  impl PG                                                    │
│    async fn update()                                        │
│      1. dbi.ensure_ready().await?      ← connect / cache   │
│      2. sqlx::query_as(QUERY)          ← version-aware SQL │
│      3. *data.write() = rows           ← overwrite state   │
├─────────────────────────────────────────────────────────────┤
│  impl prometheus::Collector                                 │
│    fn collect() → Vec<MetricFamily>                         │
│      1. data.read()                    ← snapshot          │
│      2. gauge.reset()                  ← purge stale labels│
│      3. gauge.with_label_values().set()                     │
│      4. gauge.collect()               ← emit families      │
└─────────────────────────────────────────────────────────────┘

update() and collect() are intentionally separate:
  update() — async, hits PostgreSQL, writes data
  collect() — sync, reads data, builds metric families
```

### Sequence — one scrape cycle

```mermaid
sequenceDiagram
    participant P as Prometheus
    participant R as HTTP Router
    participant H as Metrics Handler
    participant C as Collector ×16
    participant DB as PostgreSQL

    P->>R: GET /metrics
    R->>H: dispatch

    loop for each collector
        H->>C: spawn update()
        C->>DB: ensure_ready() + query_as(QUERY)
        DB-->>C: rows
        C->>C: *lock = rows
    end

    H->>H: wait for all tasks
    H->>C: collect() × 16
    C-->>H: Vec<MetricFamily>
    H->>H: encode to text
    H-->>P: 200 OK  text/plain
```

### Version gate in `new()`

```mermaid
graph TD
    A["new(dbi)"] --> B{"pg_version\n>= POSTGRES_V16?"}
    B -->|yes| C["Collector::build(dbi)"]
    C --> D{"build OK?"}
    D -->|yes| E["Some(collector)"]
    D -->|error| F["log error\nNone"]
    B -->|no| G["None\n(silently disabled)"]
```

---

## Key design decisions

| Decision | Rationale |
|---|---|
| `update()` async, `collect()` sync | Prometheus calls `collect()` synchronously from its gather loop; async DB queries must not block it |
| `Arc<RwLock<Vec<Row>>>` between update and collect | Zero-copy share of the row snapshot; multiple concurrent readers unblocked during a write |
| Lazy pool connection (`connect_lazy`) | Exporter starts and serves even when PostgreSQL is temporarily unreachable; errors surface in `update()`, not at startup |
| `PGConfig` cached after first `ensure_ready()` | PG version, block size, and extension availability are stable; no need to re-query on every scrape |
| `GaugeVec::reset()` before re-populating | Prevents stale label combinations from accumulating when a setting value or query text changes between scrapes |
| `new()` returns `Option` | Each collector self-selects based on PG version; no central version dispatch needed |
| One `Registry` per exporter, separate from global | Avoids metric name collisions when multiple PostgreSQL instances are configured |
| `const_labels` per instance | Differentiates metrics from multiple PG instances (e.g. `cluster="prod"`) without separate exporters |

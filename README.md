# pg_exporter

PostgreSQL exporter for Prometheus written in Rust.

Monitors one or more PostgreSQL instances from a single process. Collectors are version-aware and automatically adjust queries for PostgreSQL 9.5 through 18.

## Requirements

- PostgreSQL >= 9.5
- `pg_stat_statements` extension in `shared_preload_libraries` (required by the statements collector)
- `track_io_timing = on` recommended for IO metrics

## PostgreSQL user setup

Create a dedicated monitoring role with the minimum required privileges:

```sql
CREATE USER postgres_exporter WITH PASSWORD 'your_password';
GRANT pg_monitor TO postgres_exporter;
```

## Configuration

`pg_exporter.yml` — annotated example:

```yaml
listen_addr: 0.0.0.0:61488   # host:port for the HTTP server
endpoint: /metrics            # path that Prometheus scrapes

instances:
  "pg15:5432":                # arbitrary label used in logs
    dsn: "postgres://postgres_exporter:password@host:5432/dbname"
    const_labels:             # added to every metric from this instance
      project: my_project
      cluster: my_cluster
    # exclude_db_names:       # databases to skip in per-DB collectors
    #   - postgres
    #   - template0
    #   - template1
    # collect_top_query: 10   # top-N queries from pg_stat_statements (0 = all)
    # collect_top_index: 10   # top-N indexes by usage
    # collect_top_table: 10   # top-N tables by size/activity
    # no_track_mode: true     # suppress query text in metrics (privacy mode)
```

| Field | Description | Default |
|---|---|---|
| `listen_addr` | Host and port for the HTTP server | — |
| `endpoint` | HTTP path that exposes Prometheus metrics | `/metrics` |
| `instances.<name>.dsn` | PostgreSQL connection string | required |
| `instances.<name>.const_labels` | Labels added to all metrics for this instance | `{}` |
| `instances.<name>.exclude_db_names` | Databases to skip in per-DB collectors | `[]` |
| `instances.<name>.collect_top_query` | Top-N queries from `pg_stat_statements` (`0` = all) | `0` |
| `instances.<name>.collect_top_index` | Top-N indexes by usage | `0` |
| `instances.<name>.collect_top_table` | Top-N tables by size/activity | `0` |
| `instances.<name>.no_track_mode` | Omit query text from metrics | `false` |

Settings can also be overridden via environment variables with the `PGE_` prefix:

```bash
PGE_LISTEN_ADDR=0.0.0.0:9090 ./pg_exporter run
```

## CLI

```
pg_exporter [OPTIONS] [COMMAND]

Options:
  -c, --config <PATH>   Path to config file [default: pg_exporter.yml]

Commands:
  run           Start the exporter
    -l, --listen-addr <ADDR>   Override listen address from config
    -e, --endpoint <PATH>      Override metrics endpoint from config
  configcheck   Validate the config file and exit with status 0 or 1
```

## Quick start with Docker Compose

```bash
docker-compose build && docker-compose up
```

| Service | URL |
|---|---|
| pg_exporter metrics | http://127.0.0.1:61488/metrics |
| Prometheus | http://localhost:61490 |
| Grafana (admin / admin) | http://localhost:61491 |
| PostgreSQL 15 | localhost:5432 |
| PostgreSQL 17 | localhost:6432 |

PostgreSQL containers start with `pg_stat_statements` preloaded and `track_io_timing = on`.

## Building from source

```bash
cargo build --release
./target/release/pg_exporter --config pg_exporter.yml run
```

## Collectors

| Collector | Key metrics | Notes |
|---|---|---|
| `pg_activity` | connections by state, query types in-flight, wait events, vacuum operations | — |
| `pg_locks` | lock counts by type, not-granted locks | — |
| `pg_bgwriter` | checkpoints, buffers written by process, bgwriter/backend stats | PG 17+ adds restartpoints |
| `pg_database` | per-DB size, transactions, dead tuples | — |
| `pg_postmaster` | server start time | — |
| `pg_wal` | WAL generation rate, LSN position | — |
| `pg_stat_io` | reads/writes/fsyncs by backend type | PG 16+ |
| `pg_archiver` | archived/failed WAL segment counts, archiving lag | — |
| `pg_conflict` | recovery conflicts by type (tablespace, lock, snapshot, bufferpin, deadlock, logical slot) | Standby only |
| `pg_indexes` | index size, scans, tuples fetched | — |
| `pg_statements` | top-N queries: calls, rows, execution time, block I/O | Requires `pg_stat_statements` |
| `pg_tables` | table size, sequential/index scans, dead tuples | — |
| `pg_storage` | data directory disk usage | — |
| `pg_replication` | replication lag by slot | — |
| `pg_replication_slots` | slot retained WAL bytes | — |

## Querying metrics

### Fetch all metrics

```bash
curl -s http://127.0.0.1:61488/metrics
```

### Filter to a specific collector

```bash
# All activity metrics
curl -s http://127.0.0.1:61488/metrics | grep '^pg_activity'

# Checkpoint metrics
curl -s http://127.0.0.1:61488/metrics | grep '^pg_checkpoints'

# WAL metrics
curl -s http://127.0.0.1:61488/metrics | grep '^pg_wal'

# Statement metrics (requires pg_stat_statements)
curl -s http://127.0.0.1:61488/metrics | grep '^pg_statements'
```

### Filter by const label

```bash
# Metrics from a specific cluster
curl -s http://127.0.0.1:61488/metrics | grep 'cluster="my_cluster"'
```

### Check the exporter root endpoint

```bash
curl http://127.0.0.1:61488/
# This is a PgExporter for Prometheus written in Rust
```

### Sample output

```
# HELP pg_up State of PostgreSQL service: 0 is down, 1 is up.
# TYPE pg_up gauge
pg_up{cluster="my_cluster",project="my_project"} 1
# HELP pg_activity_connections_all_in_flight Number of all connections in-flight.
# TYPE pg_activity_connections_all_in_flight gauge
pg_activity_connections_all_in_flight{cluster="my_cluster",project="my_project"} 3
# HELP pg_recovery_conflicts_total Total number of recovery conflicts occurred by each conflict type.
# TYPE pg_recovery_conflicts_total counter
pg_recovery_conflicts_total{cluster="my_cluster",conflict="deadlock",database="mydb",project="my_project"} 0
```

## Running integration tests

Integration tests start a real PostgreSQL container via Docker:

```bash
cargo test --test integration
```

## License

BSD 2-Clause License. See [LICENSE](LICENSE) for details.

## Thanks to

### pgSCV
- [collects](https://github.com/cherts/pgscv/wiki/Collectors) a lot of stats about PostgreSQL environment.

### postgres_exporter
- [collects](https://github.com/prometheus-community/postgres_exporter) a Prometheus exporter for PostgreSQL server metrics.

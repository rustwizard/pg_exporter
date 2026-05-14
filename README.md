# pg_exporter

PostgreSQL exporter for Prometheus written in Rust.

Monitors one or more PostgreSQL instances from a single process. Collectors are version-aware and automatically adjust queries for PostgreSQL 9.5 through 18.

[Architecture (C4 model)](docs/architecture.md)

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
# scrape_timeout_ms: 30000   # max time per scrape; partial metrics returned on timeout

# Global pool defaults — applied to all instances unless overridden per-instance
# pool_max_connections: 10
# pool_acquire_timeout_secs: 5
# pool_idle_timeout_secs: 300
# pool_max_lifetime_secs: 1800

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
    # disable_collectors:     # skip specific collectors for this instance
    #   - pg_statements       # e.g. no pg_stat_statements on replica
    #   - pg_stat_io          # e.g. PG 15, not supported
    # pool_max_connections: 5 # override global default for this instance only
```

| Field | Description | Default |
|---|---|---|
| `listen_addr` | Host and port for the HTTP server | — |
| `endpoint` | HTTP path that exposes Prometheus metrics | `/metrics` |
| `scrape_timeout_ms` | Max time in ms to wait for all collectors on each scrape; partial metrics are returned on timeout | `30000` |
| `instances.<name>.dsn` | PostgreSQL connection string | required |
| `instances.<name>.const_labels` | Labels added to all metrics for this instance | `{}` |
| `instances.<name>.exclude_db_names` | Databases to skip in per-DB collectors | `[]` |
| `instances.<name>.collect_top_query` | Top-N queries from `pg_stat_statements` (`0` = all) | `0` |
| `instances.<name>.collect_top_index` | Top-N indexes by usage | `0` |
| `instances.<name>.collect_top_table` | Top-N tables by size/activity | `0` |
| `instances.<name>.no_track_mode` | Omit query text from metrics | `false` |
| `instances.<name>.disable_collectors` | List of collector names to skip for this instance | `[]` |
| `instances.<name>.pool_max_connections` | Max pool connections (overrides global) | `10` |
| `instances.<name>.pool_acquire_timeout_secs` | Seconds to wait for a free connection (overrides global) | `5` |
| `instances.<name>.pool_idle_timeout_secs` | Seconds before idle connection is closed (overrides global) | `300` |
| `instances.<name>.pool_max_lifetime_secs` | Max lifetime of a connection in seconds (overrides global) | `1800` |
| `pool_max_connections` | Global default max pool connections for all instances | `10` |
| `pool_acquire_timeout_secs` | Global default acquire timeout in seconds | `5` |
| `pool_idle_timeout_secs` | Global default idle timeout in seconds | `300` |
| `pool_max_lifetime_secs` | Global default max connection lifetime in seconds | `1800` |

Settings can also be overridden via environment variables with the `PGE_` prefix:

```bash
PGE_LISTEN_ADDR=0.0.0.0:9090 ./pg_exporter run
```

Log level is controlled via the standard `RUST_LOG` environment variable (default: `info`):

```bash
RUST_LOG=debug ./pg_exporter run                          # all debug output
RUST_LOG=pg_exporter=debug,sqlx=warn ./pg_exporter run   # fine-grained control
```

## CLI

```
pg_exporter [OPTIONS] [COMMAND]

Options:
  -c, --config <PATH>   Path to config file [default: pg_exporter.yml]

Commands:
  run               Start the exporter
    -l, --listen-addr <ADDR>   Override listen address from config
    -e, --endpoint <PATH>      Override metrics endpoint from config
  configcheck       Validate the config file and exit with status 0 or 1
  list-collectors   Print all available collectors with their minimum PostgreSQL version
```

### list-collectors

Use this command to discover collector names for use in `disable_collectors`:

```
$ pg_exporter list-collectors
COLLECTOR                 MIN_PG_VERSION  NOTES
pg_activity               9.5
pg_archiver               9.5
pg_bgwriter               9.5
pg_conflict               9.5
pg_database               9.5
pg_indexes                9.5
pg_locks                  9.5
pg_postmaster             9.5
pg_replication            9.6
pg_replication_slots      9.6
pg_settings               9.5
pg_stat_io                16
pg_stat_slru              13
pg_statements             9.5             requires pg_stat_statements extension
pg_storage                10
pg_tables                 9.5
pg_wal                    9.5
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

Grafana comes with pre-provisioned dashboards including **pg_exporter — Self Monitoring** which shows per-collector scrape duration and error rates.

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
| `pg_stat_slru` | SLRU cache block hits/reads/writes/flushes by cache name | PG 13+ |
| `pg_archiver` | archived/failed WAL segment counts, archiving lag | — |
| `pg_conflict` | recovery conflicts by type (tablespace, lock, snapshot, bufferpin, deadlock, logical slot) | Standby only |
| `pg_indexes` | index size, scans, tuples fetched | — |
| `pg_statements` | top-N queries: calls, rows, execution time, block I/O | Requires `pg_stat_statements` |
| `pg_tables` | table size, sequential/index scans, dead tuples | — |
| `pg_storage` | data directory disk usage | — |
| `pg_replication` | replication lag by slot | — |
| `pg_replication_slots` | slot retained WAL bytes | — |
| `pg_settings` | all GUC settings as labeled metrics (`name`, `setting`, `unit`, `vartype`); numeric value in base units (bytes/seconds) for `integer`/`real` types | — |

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

### Health check

```bash
curl http://127.0.0.1:61488/health
# {"status":"ok"}
```

Returns `200 {"status":"ok"}` when all connection pools are open, `503 {"status":"degraded"}` otherwise.

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

## Self-monitoring metrics

The exporter exposes its own health metrics so you can monitor the exporter itself:

| Metric | Type | Description |
|---|---|---|
| `pg_exporter_scrape_duration_seconds{collector}` | Histogram | Duration of each collector's `update()` call. Buckets: 10 ms … 30 s |
| `pg_exporter_scrape_errors_total{collector}` | Counter | Number of failed `update()` calls per collector |

Both metrics are pre-initialized to zero for every registered collector so they appear in `/metrics` output from the very first scrape, even before any error occurs.

Useful PromQL queries:

```bash
# Collectors with errors in the last 5 minutes
count(increase(pg_exporter_scrape_errors_total[5m]) > 0)

# p99 scrape duration per collector
histogram_quantile(0.99, sum by (le, collector) (rate(pg_exporter_scrape_duration_seconds_bucket[5m])))
```

## Kubernetes probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 61488
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health
    port: 61488
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Running integration tests

Integration tests start a real PostgreSQL container via Docker:

```bash
cargo test --test integration
```

## License

BSD 3-Clause License. See [LICENSE](LICENSE) for details.

## Thanks to

### pgSCV
- [collects](https://github.com/cherts/pgscv/wiki/Collectors) a lot of stats about PostgreSQL environment.

### postgres_exporter
- [collects](https://github.com/prometheus-community/postgres_exporter) a Prometheus exporter for PostgreSQL server metrics.

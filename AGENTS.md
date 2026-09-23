# AGENTS.md — pg_exporter

PostgreSQL → Prometheus exporter in Rust (edition 2024). One process scrapes many PG
instances; collectors are version-aware for PG 9.5–18.

Deeper references, do not duplicate them here:
- `README.md` — config reference, collector table, CLI.
- `docs/architecture.md` — C4 diagrams, key design decisions.
- Rust/collector coding rules are available to agents as the `rust` skill.

## Commands

```bash
cargo fmt --all -- --check      # CI gate 1
cargo clippy -- -D warnings     # CI gate 2 (warnings are errors)
cargo test --lib                # unit tests, no Docker
cargo test --bins               # adds main.rs /health tests
cargo test --test integration   # needs a running Docker daemon
cargo test --test integration test_pg_stat_slru_collector   # single test
```

- Plain `cargo test` also runs the Docker-backed integration suite. Use `--lib` when Docker is
  unavailable. CI runs `--lib` and `--test integration` as separate steps, in that order.
- No Makefile/justfile. There is no `rust-toolchain.toml`; CI uses `dtolnay/rust-toolchain@stable`
  while the Dockerfile pins `rust:1.93-slim`, so the two can drift.
- Run `cargo fmt` yourself after edits. The auto-format hook in `.claude/settings.json` is
  Claude-Code-specific and does not fire in other harnesses.

## Repo layout quirks

- Single crate, both a lib (`src/lib.rs`) and a bin (`src/main.rs`).
  `main.rs` re-declares `mod app; mod collectors; mod config; mod error; mod instance;`, so those
  modules are compiled twice (once per target). `src/error.rs` is bin-only — it is not in `lib.rs`.
- Integration tests live under `src/tests/`, wired up via `[[test]] path = "src/tests/integration.rs"`
  in `Cargo.toml`, not in a top-level `tests/` dir.
- `build.rs` injects `GIT_HASH` via `env!`; it degrades to `"unknown"` outside a git checkout.

## Collector architecture

Each collector implements **two** traits:
- `collectors::PG` — `async fn update()`: `dbi.ensure_ready().await?`, run `sqlx::query_as`, write
  the rows into `Arc<RwLock<Vec<Row>>>`.
- `prometheus::core::Collector` — sync `collect()`: read the lock, set gauges, emit metric families.
  `reset()` the vec-metrics first when labels carry mutable data (query text, setting values).

Do not do DB work in `collect()`; Prometheus calls it synchronously.

Shared helpers to reuse instead of rewriting:
- `collectors::RwLockExt` — `read_or_log(ctx)` / `write_or_bail(ctx)` for poisoned-lock handling.
- `collector_new!` macro (`src/collectors/mod.rs`) — generates the `pub fn new(...) -> Option<T>`
  wrapper, optionally with a version/capability condition and a log message.

`instance::PostgresDB` uses `connect_lazy`, so startup succeeds with PG down. `ensure_ready()`
fetches and caches `PGConfig` (version, block size, WAL segment size, `pg_stat_statements`
availability + schema) on first use; `current_cfg()` is the non-blocking read and returns `None`
before the first successful connection — that is why version gates use
`current_cfg().map(|c| c.pg_version).unwrap_or(<MIN>)`.

Version gating belongs in `new()` (returns `None` → collector silently absent), not in `update()`.
Version constants (`POSTGRES_V95` … `POSTGRES_V18`) are in `src/collectors/mod.rs`.

Metric names get the `pg` prefix from `collectors::NAMESPACE` via `Opts::namespace()`, so a metric
declared as `"up"` is exported as `pg_up`. `pg_up` itself is owned by `pg_activity`, which must stay
constructible with the DB down and set it to 0 on a failed `update()`.

## Adding a collector — all five places

1. `src/collectors/pg_<name>.rs` — template: `pg_stat_slru.rs` or `pg_stat_io.rs`
   (`pg_activity.rs`, `pg_statements.rs` and `pg_tables.rs` are 800+ lines, poor templates).
2. `pub mod pg_<name>;` in `src/collectors/mod.rs`.
3. Add entries to **both** `COLLECTOR_INFO` and `COLLECTOR_NAMES` in `src/collectors/mod.rs`.
   `COLLECTOR_NAMES` is what `PGEConfig::validate()` checks `disable_collectors` against, so a
   missing entry makes the collector impossible to disable via config.
4. `reg!("pg_<name>", collectors::pg_<name>::new);` inside `pgexporter()` in `src/main.rs`
   (the `reg!` macro applies the per-instance `disable_collectors` filter).
5. Integration test in `src/tests/integration.rs`, plus a row in the README collector table and a
   Grafana panel under `monitoring/grafana/provisioning/dashboards/`.

## Testing notes

- Helpers in `src/tests/common/mod.rs` spin up `postgres:17` via testcontainers:
  `create_test_instance()`, `create_test_instance_with_exclusions()`,
  `create_test_instance_with_statement_timeout()`,
  `create_test_instance_with_pg_stat_statements[_opts]()`, `create_second_database()`.
- Keep the returned `ContainerAsync` bound (`let (_container, pgi) = ...`); dropping it kills the DB.
- `pg_stat_statements` needs `shared_preload_libraries` at container start, hence the separate helper.
- Each test starts its own container — the suite is slow and Docker-bound; prefer running one test
  by name while iterating.
- `#![warn(clippy::unwrap_used)]` is set in `src/main.rs`. Test modules opt out with
  `#[allow(clippy::unwrap_used)]`; production code should use `?` / `ok_or` / `bail!`.
- `instance::reset_cfg()` exists only to test lazy reconnect.

## Config & runtime

- `pg_exporter.yml` is **gitignored**; `pg_exporter.yml.dist` is the template. The Dockerfile
  `COPY`s `pg_exporter.yml`, so an image build needs `cp pg_exporter.yml.dist pg_exporter.yml`
  first (CI does this in the docker job).
- Config keys can be overridden by env vars with the `PGE` prefix (`config` crate `Environment`).
  Logging honours `RUST_LOG` first, then `log_level` from the YAML.
- Pool and `statement_timeout_ms` settings cascade: per-instance value → global default →
  constant in `src/instance/mod.rs` (`merge_pool_defaults`). `statement_timeout_ms` falls back to
  `scrape_timeout_ms`, so no single query outlives the scrape that triggered it.
- Every pool connection runs `after_connect`: sets `application_name=pg_exporter` (see
  `instance::APPLICATION_NAME`) and `statement_timeout`. `connect_lazy` means this only runs when a
  connection is actually established.
- `/metrics` goes through `PGEApp::refresh`: collector updates are serialized by a single-flight
  lock, so concurrent scrapes reuse one pass. With `min_scrape_interval_ms > 0` (default 0) the last
  snapshot is reused within that window. `update_collectors` spawns one task per collector, bounded
  by `scrape_timeout_ms` (default 30 s); on timeout it logs a warning and returns partial metrics.
  HTTP `shutdown_timeout` is deliberately `scrape_timeout_ms/1000 + 5`.
- Exporter self-metrics (`pg_exporter_scrape_duration_seconds`, `..._scrape_errors_total`,
  `..._scrape_cached_total`, `..._pool_size`, `..._pool_idle`) live in a dedicated `Registry` on
  `PGEApp`; process metrics come from the global `prometheus::gather()`. Both are encoded into the
  same response.
- `/health` returns 503 when any instance pool `is_closed()`.
- `docker-compose` bind-mounts `monitoring/postgresql/data{,17}` (gitignored). Stale data dirs from
  an older PG major version will break startup; remove them rather than debugging the container.

## Conventions

- `anyhow` everywhere in collectors and instance code. `thiserror` is used only for `MetricsError`
  in `src/error.rs`; do not add new `thiserror` enums without a reason.
- `tracing` macros, never `println!` (except deliberate CLI output in `list-collectors`).
- SQL: `const QUERY: &str` with `COALESCE` on nullable columns instead of `Option<T>` fields; plain
  newlines in string literals, no `\` continuations.
- Per-instance `const_labels` are attached via `Opts::const_labels(dbi.labels.clone())`.
- Branch is `master`; work through PRs. Commit messages are short and lowercase, no co-author
  trailers. Pushes to `master`/tags are mirrored to Gitverse by CI, and release + docker jobs only
  run on `v*` tags.

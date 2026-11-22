# Recordflow

Change data capture from **PostgreSQL** to **ClickHouse**. Recordflow copies one PostgreSQL table into ClickHouse and keeps it up to date: it takes a consistent snapshot of the table while it streams the table's changes from a logical replication slot, and writes both into a single ClickHouse table that always converges to the current state of the source.

## Table of Contents

1. [How it works](#how-it-works)
2. [Data model in ClickHouse](#data-model-in-clickhouse)
3. [Requirements](#requirements)
4. [Access rights](#access-rights)
5. [Configuration](#configuration)
6. [Running](#running)
7. [Exit codes](#exit-codes)
8. [Operator procedures](#operator-procedures)
9. [Known limitations of v1](#known-limitations-of-v1)
10. [Tests](#tests)
11. [Glossary](#glossary)

---

## How it works

```
 PostgreSQL                         Recordflow                              ClickHouse
┌──────────────┐  pgoutput stream  ┌──────────────────────────────┐  native  ┌──────────────────────────┐
│ slot ────────┼──────────────────►│ decoder → transactions ─┐    │  (9000)  │ recordflow.<schema>__<t> │
│ publication  │                   │                         ├─►sink──────►│  ReplacingMergeTree      │
│ table ───────┼──────────────────►│ snapshot chunks (DBLog)─┘    │          │ <schema>__<t>_current    │
└──────────────┘  SELECT by PK     │ state file in data/          │          │  view                    │
                                   └──────────────────────────────┘          └──────────────────────────┘
```

- **The source database is not yours.** Recordflow creates nothing in PostgreSQL. You create the publication and the replication slot; Recordflow only reads, emits watermark messages into the WAL (`pg_logical_emit_message`) and advances its own slot.
- **Stream and snapshot in one table.** Every row carries a version derived from the WAL position (LSN), so a change from the stream and a row read by the snapshot can arrive in any order and the newest one wins. The snapshot follows the DBLog algorithm: the table is read in primary-key chunks between two watermarks while the stream keeps flowing, so the stream is never paused.
- **Sweep.** After a complete snapshot pass, rows that exist in ClickHouse but no longer in the source (deleted while Recordflow was not running) are marked deleted.
- **At-least-once, idempotent.** Changes are buffered per transaction and written in batches; the slot is confirmed only after a batch is in ClickHouse. After a crash some changes are written again, which changes nothing, because the same version replaces itself.
- **One thread, one table.** One process captures one table, with one replication connection and one query connection.

## Data model in ClickHouse

For the source table `public.orders` Recordflow creates, in the database `recordflow`:

```sql
CREATE TABLE recordflow.public__orders (
    -- the source columns, mapped as below
    id              Int64,
    customer        String,
    amount          Nullable(Decimal(12, 2)),
    created_at      DateTime64(6, 'UTC'),
    -- metadata, always prefixed with _rf_
    _rf_version     UInt128,                                   -- from the LSN; the newest wins
    _rf_is_deleted  UInt8,
    _rf_op          Enum8('c' = 1, 'u' = 2, 'd' = 3, 'r' = 4), -- create, update, delete, read (snapshot)
    _rf_source      Enum8('log' = 1, 'snapshot' = 2),
    _rf_commit_ts   DateTime64(6, 'UTC'),
    _rf_ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_rf_version, _rf_is_deleted)
ORDER BY (id);                                                 -- the source primary key

CREATE VIEW recordflow.public__orders_current AS
SELECT * EXCEPT (_rf_is_deleted) FROM recordflow.public__orders FINAL WHERE _rf_is_deleted = 0;
```

**Read the `_current` view.** It shows exactly one row per primary key, without deleted rows; readers do not need to know about `FINAL` or `_rf_is_deleted`. The base table also holds older versions until ClickHouse merges them away. The table name is `<schema>__<table>` by default (`clickhouse.table` changes it).

| PostgreSQL | ClickHouse |
|---|---|
| `smallint`, `integer`, `bigint` | `Int16`, `Int32`, `Int64` |
| `real`, `double precision` | `Float32`, `Float64` |
| `boolean` | `Bool` |
| `numeric(p, s)` with p ≤ 38 | `Decimal(p, s)` |
| `numeric` without precision, or p > 38 | `String` |
| `date` | `Date32` |
| `timestamp` / `timestamptz` | `DateTime64(6)` / `DateTime64(6, 'UTC')` |
| `uuid` | `UUID` |
| enum types | `LowCardinality(String)` (`String` when nullable) |
| `bytea` | `String` (raw bytes) |
| everything else (`text`, `varchar`, `json(b)`, arrays, `interval`, `time`, …) | `String` (PostgreSQL text form) |

Columns that allow `NULL` become `Nullable(...)`; primary-key columns never do. `infinity`/`-infinity` dates and timestamps become `NULL`, or the smallest or largest ClickHouse value when the column is not nullable (with a warning).

---

## Requirements

**Source PostgreSQL**

- **PostgreSQL 14 or newer** (the `messages` option of `pgoutput`).
- `wal_level = logical`, and enough `max_replication_slots` and `max_wal_senders` for one more slot and one more connection.
- The table has a **primary key**.
- A publication that contains the table and a logical replication slot on `pgoutput`, both created by you (see [Access rights](#access-rights)).

**ClickHouse**

- A current ClickHouse server (tested with 26.8), reachable on the native port **9000**.
- Recordflow and ClickHouse must be on a **private network** (the same Docker network or VPC): the connection to ClickHouse is not encrypted in v1.

**Build** (only when not using the Docker image)

| | |
|---|---|
| Platform | Linux (on Windows: **WSL2**, e.g. Ubuntu 24.04) |
| Toolchain | GCC with C++20, CMake ≥ 3.20, Ninja |
| Dependencies | from the **vcpkg** manifest (`vcpkg.json`): libpq, clickhouse-cpp, nlohmann-json, tomlplusplus, GoogleTest |
| Local services | Docker with Compose (Docker Desktop with WSL integration on Windows) |

```bash
sudo apt install -y build-essential cmake ninja-build pkg-config zip unzip curl tar git \
    autoconf autoconf-archive automake libtool bison flex python3-venv
git clone https://github.com/microsoft/vcpkg ~/vcpkg && ~/vcpkg/bootstrap-vcpkg.sh
echo 'export VCPKG_ROOT=$HOME/vcpkg' >> ~/.bashrc
```

---

## Access rights

Recordflow needs no superuser on either side. The local `docker compose` setup creates exactly these roles and users, so permission problems show up in development, not at the first deployment.

### PostgreSQL

```sql
-- administrator
CREATE ROLE recordflow WITH LOGIN REPLICATION PASSWORD '...';
GRANT CONNECT ON DATABASE mydb TO recordflow;
GRANT USAGE ON SCHEMA public TO recordflow;
GRANT SELECT ON public.orders TO recordflow;
-- only if Recordflow reports at startup that it is missing:
-- GRANT EXECUTE ON FUNCTION pg_logical_emit_message(boolean, text, text) TO recordflow;  -- PostgreSQL 14–16
-- optional, so that the "slot busy" message can name the other client:
-- GRANT pg_read_all_stats TO recordflow;

-- table owner / administrator
CREATE PUBLICATION recordflow_pub FOR TABLE public.orders;
SELECT pg_create_logical_replication_slot('recordflow_slot', 'pgoutput');
```

- **`pg_logical_emit_message` has a different signature per version.** PostgreSQL 17 added a fourth parameter, `flush`. Recordflow looks the function up by name and checks `EXECUTE` at startup; if it is missing, the error message contains the exact `GRANT` for your server version.
- **`pg_hba.conf`:** a logical replication connection (`replication=database`) matches the **ordinary** lines for the database (for example `host mydb recordflow 10.0.0.0/8 scram-sha-256`), not the `replication` lines, which are for physical replication only.
- **Row-level security:** the role must see every row of the table. If the table uses RLS, give the role `BYPASSRLS`; otherwise the snapshot misses hidden rows and the sweep deletes them in ClickHouse. Recordflow does not check this.
- The role needs no ownership of the table and no `CREATE`, `INSERT`, `UPDATE` or `DELETE`.

### ClickHouse

```sql
-- administrator
CREATE DATABASE recordflow;
CREATE USER recordflow IDENTIFIED WITH sha256_password BY '...';
GRANT CREATE TABLE, CREATE VIEW, INSERT, SELECT, TRUNCATE ON recordflow.* TO recordflow;

-- readers (analysts, dashboards)
CREATE USER analyst IDENTIFIED WITH sha256_password BY '...';
GRANT SELECT ON recordflow.* TO analyst;
```

Recordflow works only inside the `recordflow` database and has no `DROP` or `ALTER`; destructive recovery steps stay with the operator. If the database does not exist, Recordflow stops with exit code 4 and prints the SQL above.

---

## Configuration

Recordflow reads a TOML file: the path given with `--config`, otherwise `./recordflow.toml` if it exists. Only the four `[source]` keys are required:

```toml
[source]
conninfo    = "host=localhost port=5432 dbname=mydb user=recordflow"
slot        = "recordflow_slot"
publication = "recordflow_pub"
table       = "public.orders"
```

`recordflow.toml.example` lists every key with its default and a short explanation (ClickHouse connection, `data_dir`, batch limits, snapshot pacing, retries, shutdown timeout).

- **Precedence:** defaults < TOML file < environment < command line.
- **Environment:** every key can be set as `RECORDFLOW_<SECTION>_<KEY>` (for example `RECORDFLOW_SNAPSHOT_ON_START=if_incomplete`), or as `RECORDFLOW_<SECTION>_<KEY>_FILE` naming a file that holds the value (Docker secrets). The whole configuration can come from the environment, without a file.
- **Secrets:** the PostgreSQL password goes through libpq (`PGPASSWORD`, `PGPASSFILE` or `~/.pgpass`), never into `conninfo`. The ClickHouse password goes only through `RECORDFLOW_CLICKHOUSE_PASSWORD` (or `_FILE`); putting it in the file is an error. Secrets are masked in every log line.
- **TLS to PostgreSQL:** through `conninfo` (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`). Use `sslmode=verify-full` in production.
- **Strict:** unknown sections or keys, wrong types and out-of-range values stop the start (exit code 2) with the file and line.
- `recordflow --check-config` loads and validates the configuration, prints the effective values with the source of each (secrets masked) and exits.
- **ClickHouse compression:** set `clickhouse.compression = "none"`. The vcpkg build of clickhouse-cpp computes wrong checksums for compressed blocks, so `lz4` and `zstd` currently fail with `Checksum doesn't match` (code 271).

### State

Recordflow keeps its progress in `data_dir` (default `data`, `/var/lib/recordflow` in the Docker image): `recordflow.state.json` (snapshot progress, last written LSN, identity of the source cluster and of the ClickHouse table) and `recordflow.lock`, which keeps a second process away from the same directory. The directory is created with mode `0700` and the state file with `0600`. The state file holds no secrets, but it identifies the source.

At startup the state is checked against reality: a ClickHouse table that was dropped or recreated, or a slot that jumped past the last written position, starts a new snapshot; a state file that belongs to another slot, table or PostgreSQL cluster stops the start (exit code 4).

---

## Running

```
recordflow [--config PATH] [--check-config]
```

Log messages go to **stderr**. With `output.sink = "jsonl"` the row changes go to **stdout** as JSON Lines instead of ClickHouse (for debugging). Stop with `SIGINT` or `SIGTERM`: Recordflow drops the open transaction and snapshot chunk, writes the complete transactions it holds, confirms the slot and exits within `shutdown.timeout_s` (default 8 s).

### With Docker Compose (whole system)

```bash
cp .env.example .env            # local credentials; .env is git-ignored
docker compose up -d --build --wait
docker compose logs -f recordflow
```

This starts PostgreSQL 18 (`localhost:5432`), ClickHouse (`localhost:9000`, HTTP on `8123`) and Recordflow built from the `Dockerfile`. On the first start the init scripts in `docker/` create the roles and users from [Access rights](#access-rights), a test table `public.orders`, the publication `recordflow_pub` and the slot `recordflow_slot`. The scripts run only when a data volume is empty; `docker compose down -v` removes the volumes (including Recordflow's state) so they run again.

Try it:

```bash
docker exec -i recordflow-source-db psql -U postgres -d mydb \
    -c "INSERT INTO orders (id, customer, amount) VALUES (1, 'alice', 9.99)"
docker exec -i recordflow-clickhouse clickhouse-client --user analyst --password changeme \
    --query "SELECT * FROM recordflow.public__orders_current"
```

The `recordflow` service is configured from `.env` through `RECORDFLOW_*` variables, keeps its state in the named volume `recordflow_data`, gets 10 s to stop (more than `shutdown.timeout_s`) and is restarted on failure at most 5 times.

### With the Docker image

```bash
docker build -t recordflow .
docker run -d --name recordflow \
    -e RECORDFLOW_SOURCE_CONNINFO="host=pg.internal dbname=mydb user=recordflow sslmode=verify-full" \
    -e RECORDFLOW_SOURCE_SLOT=recordflow_slot \
    -e RECORDFLOW_SOURCE_PUBLICATION=recordflow_pub \
    -e RECORDFLOW_SOURCE_TABLE=public.orders \
    -e PGPASSFILE=/run/secrets/pgpass \
    -e RECORDFLOW_CLICKHOUSE_HOST=clickhouse.internal \
    -e RECORDFLOW_CLICKHOUSE_PASSWORD_FILE=/run/secrets/ch_password \
    -e RECORDFLOW_CLICKHOUSE_COMPRESSION=none \
    -v recordflow_data:/var/lib/recordflow \
    --stop-timeout 10 \
    recordflow
```

The image runs as the unprivileged user `recordflow` (UID 10001), writes only to the volume `/var/lib/recordflow` and always starts with `--config /etc/recordflow/recordflow.toml`, which only sets `data_dir`. Arguments given to `docker run` are appended: `docker run ... recordflow --check-config` shows the effective configuration, and `--config /path/in/container.toml` replaces the default file. Or configure everything through the environment, as above. libpq reads the PostgreSQL password from `PGPASSWORD`, or from a file in `.pgpass` format (`host:port:db:user:password`) named by `PGPASSFILE`; that file must be readable only by its owner.

### Locally

```bash
cmake --preset linux
cmake --build --preset linux
cp recordflow.toml.example recordflow.toml     # set clickhouse.compression = "none"
docker compose up -d --wait postgres clickhouse
PGPASSWORD=changeme RECORDFLOW_CLICKHOUSE_PASSWORD=changeme ./build/linux/recordflow
```

The `linux` preset uses the vcpkg toolchain from `$VCPKG_ROOT`; the first configure builds the dependencies (several minutes, once). Start only the two databases, as above: the `recordflow` container would hold the same slot.

---

## Exit codes

| Code | Meaning | What to do |
|---|---|---|
| 0 | Normal stop after `SIGINT`/`SIGTERM` | Nothing. |
| 1 | Unexpected error: a ClickHouse error that retrying will not fix (e.g. someone changed the table by hand), full disk, state file not writable, a bug | Read the last `ERROR` line and fix the cause; a restart is safe (nothing unwritten was confirmed) and may run a new snapshot. |
| 2 | Configuration or command-line error | Fix the key named in the message; check with `--check-config`. |
| 3 | The source table's schema changed | Follow [Schema change](#schema-change); the message contains the exact commands. |
| 4 | Intervention needed: the slot or publication is missing or invalid, a permission is missing, the slot stayed busy for `retry.slot_busy_timeout_s`, the state file belongs to another slot, table or cluster, or is unreadable | Do what the message says (it names the missing object or the `GRANT`); see the procedures below. |

Connection losses, PostgreSQL restarts and temporary ClickHouse errors (`TOO_MANY_PARTS`, `MEMORY_LIMIT_EXCEEDED`, timeouts, network errors) do not stop the process: Recordflow reconnects or retries the batch with exponential backoff (`[retry]`).

---

## Operator procedures

### Schema change

Recordflow does not apply schema changes in v1. Any change that alters the derived ClickHouse table (a column added, dropped, renamed or moved, a type change that changes the ClickHouse type, a primary key or replica identity change, a renamed table, `DROP NOT NULL` once a `NULL` arrives) stops it with exit code 3 and a message that lists the differences. A change that keeps the ClickHouse type (e.g. `varchar(50)` → `varchar(100)`) is accepted. To recover:

1. Stop Recordflow.
2. Drop the target table and view in ClickHouse:
   ```sql
   DROP VIEW  IF EXISTS recordflow.public__orders_current;
   DROP TABLE IF EXISTS recordflow.public__orders;
   ```
3. Move the slot past the change, so that events in the old schema are not replayed:
   ```sql
   SELECT pg_replication_slot_advance('recordflow_slot', pg_current_wal_lsn());
   ```
   Skipping them is safe: the following snapshot reads the whole current table.
4. Start Recordflow. It creates the table from the new schema and runs a full snapshot.

### New slot after invalidation

A slot that falls too far behind can be invalidated by PostgreSQL (`max_slot_wal_keep_size`); Recordflow then stops with exit code 4. Recreate the slot under the same name and start again:

```sql
SELECT pg_drop_replication_slot('recordflow_slot');
SELECT pg_create_logical_replication_slot('recordflow_slot', 'pgoutput');
```

Recordflow notices that the slot is ahead of its last written position (a gap in the changes) and runs a full snapshot, after which the sweep removes rows deleted in the meantime. The same happens if the slot was recreated for any other reason.

### New source cluster

If the source becomes a different PostgreSQL cluster (for example a new server loaded from a dump), its LSNs start over, so old versions in ClickHouse would win over new changes. Recordflow detects the new system identifier and stops with exit code 4. To start over:

1. Stop Recordflow.
2. Drop the target table and view in ClickHouse (as in [Schema change](#schema-change), step 2).
3. Delete `recordflow.state.json` from `data_dir`.
4. Create the publication and slot on the new cluster and start Recordflow; it runs a full snapshot.

To point Recordflow at a different slot or table, use a new `data_dir` (or delete the state file); the old state is refused with exit code 4.

---

## Known limitations of v1

- **One table per process.** Run one Recordflow (with its own slot and `data_dir`) per table.
- **Unchanged TOAST values.** If an `UPDATE` does not touch a large out-of-line (TOASTed) column, PostgreSQL does not resend that value. Recordflow then writes `NULL` (or the type's default, e.g. `''`, when the column is not nullable) and logs a warning with the key and column. The next snapshot (next start) writes the full row and corrects it.
- **Row-level security** is not checked; the role must see all rows (see [Access rights](#access-rights)).
- **No TLS to ClickHouse**; keep both on a private network. TLS to PostgreSQL works through `conninfo`.
- **No compression to ClickHouse** with the current vcpkg build (see [Configuration](#configuration)).
- **Schema changes are not handled**: Recordflow stops and the operator recovers by hand.
- **A snapshot on every start** by default (`snapshot.on_start = "always"`), which corrects anything missed while stopped. With `if_incomplete` only an unfinished snapshot is continued.
- Deleted rows stay in the ClickHouse table (marked with `_rf_is_deleted`) until a merge; the `_current` view hides them.
- No metrics endpoint; observe Recordflow through its log and `pg_replication_slots`.

---

## Tests

```bash
ctest --preset linux -L unit                          # no services needed
docker compose up -d --wait postgres clickhouse
RECORDFLOW_IT=1 ctest --preset linux -L integration   # against the compose services
```

Unit tests cover pure logic: the `pgoutput` decoder, configuration and `conninfo`, value conversion, transaction buffering, schema comparison, the state file and startup decisions, and the snapshot window and chunk visibility. Integration tests start the real binary against PostgreSQL and ClickHouse: streaming, a snapshot under concurrent writes, snapshot resume and convergence after a hard kill, the sweep, schema change detection and graceful shutdown. They read credentials from the environment or `.env`, create their own tables, publications and slots, and are skipped unless `RECORDFLOW_IT=1`.

---

## Glossary

| Term | Description |
|------|-------------|
| CDC | Change Data Capture — tracking and emitting row-level database changes as they happen |
| WAL | Write-Ahead Log — PostgreSQL's append-only log of every data change |
| LSN | Log Sequence Number — a position in the WAL; Recordflow derives row versions from it |
| Replication slot | A server-side object that stores a consumer's confirmed LSN and keeps the WAL it still needs |
| Publication | A server-side object defining which tables are included in a logical replication stream |
| pgoutput | PostgreSQL's built-in logical decoding plugin |
| Watermark | A message Recordflow writes into the WAL to mark where a snapshot chunk starts and ends in the stream |
| DBLog | The snapshot algorithm (Netflix): chunks read between watermarks, merged with the live stream |
| Sweep | The step after a full snapshot that marks rows missing from the source as deleted |
| TOAST | PostgreSQL's out-of-line storage for large values; unmodified TOASTed columns are not resent |
| ReplacingMergeTree | ClickHouse engine that keeps, per sort key, the row with the highest version |

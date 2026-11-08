# Recordflow

A change-data-capture reader for PostgreSQL. Recordflow streams logical replication from a slot and decodes the `pgoutput` protocol into readable row-level events.

## Table of Contents

1. [Introduction](#introduction)
2. [Requirements](#requirements)
3. [Configuration](#configuration)
4. [How to Run](#how-to-run)
5. [pgoutput Messages](#pgoutput-messages)
6. [Glossary](#glossary)

---

## Introduction

Recordflow connects to a PostgreSQL source database in replication mode, issues a single `START_REPLICATION` command against a slot you created yourself, and decodes every `pgoutput` message it receives.

Two properties define the design:

**The application creates zero database objects.** You create the publication and replication slot yourself; Recordflow only issues `START_REPLICATION`, which makes it safe to point at a database you don't own.

**One connection, print-only.** A single `replication=database` connection carries the stream. Decoded events are printed to stdout, and the slot is advanced as they are, so each run resumes where the last one stopped.

---

## Requirements

**Build**

| | |
|---|---|
| Platform | Linux (on Windows: **WSL2**, e.g. Ubuntu 24.04) |
| Toolchain | GCC with C++20 |
| Build system | CMake ≥ 3.20 + Ninja |
| Dependencies | from the **vcpkg** manifest (`vcpkg.json`): libpq, clickhouse-cpp, nlohmann-json, tomlplusplus, GoogleTest |
| Local services | Docker with Compose (Docker Desktop with WSL integration on Windows) |

On Ubuntu, install the build tools once:

```bash
sudo apt install -y build-essential cmake ninja-build pkg-config zip unzip curl tar git \
    autoconf autoconf-archive automake libtool bison flex python3-venv
```

and vcpkg:

```bash
git clone https://github.com/microsoft/vcpkg ~/vcpkg && ~/vcpkg/bootstrap-vcpkg.sh
echo 'export VCPKG_ROOT=$HOME/vcpkg' >> ~/.bashrc
```

**Source database**

- PostgreSQL 10 or newer with `wal_level = logical`
- A role with the `REPLICATION` attribute and `SELECT` on the published tables
- A logical replication slot bound to the `pgoutput` plugin, and a publication — both pre-created (see below)

---

## Configuration

Recordflow reads a TOML file, `./recordflow.toml` by default (or the path given with `--config`). Only the four `[source]` keys are required:

```toml
[source]
conninfo    = "host=localhost port=5432 dbname=mydb user=recordflow"
slot        = "recordflow_slot"
publication = "recordflow_pub"
table       = "public.orders"
```

`recordflow.toml.example` lists every key with its default. Copy it to `recordflow.toml` (git-ignored) and adjust.

- **Precedence:** defaults < TOML file < environment < command line.
- **Environment:** every key can be set as `RECORDFLOW_<SECTION>_<KEY>` (for example `RECORDFLOW_SNAPSHOT_ON_START=if_incomplete`), or as `RECORDFLOW_<SECTION>_<KEY>_FILE` naming a file that holds the value (for Docker secrets). Without a TOML file, the whole configuration can come from the environment.
- **Secrets:** the PostgreSQL password goes through libpq (`PGPASSWORD` or `~/.pgpass`); the ClickHouse password only through `RECORDFLOW_CLICKHOUSE_PASSWORD` (or `_FILE`) — putting it in the file is an error.
- **Strict:** unknown sections or keys, wrong types and out-of-range values stop the start with the file and line.
- `recordflow --check-config` loads and validates the configuration, prints the effective values (with the source of each, secrets masked) and exits.

---

## How to Run

### 1. Prepare the source database

Recordflow never changes the source database, so you set it up manually before the first run: a publication for the tables to capture, a logical replication slot bound to `pgoutput`, and a role with the `REPLICATION` attribute and `SELECT` on those tables.

For local development, Docker Compose starts PostgreSQL (`wal_level=logical`, `localhost:5432`) and ClickHouse (`localhost:9000`, HTTP on `8123`):

```bash
cp .env.example .env      # local credentials; .env is git-ignored
docker compose up -d --wait
```

On the first start, the init scripts in `docker/` create the same roles and users as a production setup: the PostgreSQL role `recordflow` with a test table `public.orders`, the publication `recordflow_pub` and the slot `recordflow_slot`, and the ClickHouse database `recordflow` with the users `recordflow` and `analyst`. The scripts run only when a data volume is empty; to rerun them, remove the volumes with `docker compose down -v`.

### 2. Build

```bash
cmake --preset linux
cmake --build --preset linux
```

The `linux` preset uses the vcpkg toolchain from `$VCPKG_ROOT`; the first configure builds the dependencies from `vcpkg.json` (this takes several minutes once, then it is cached). The binary is `build/linux/recordflow`.

Tests:

```bash
ctest --preset linux -L unit                        # no services needed
RECORDFLOW_IT=1 ctest --preset linux -L integration # needs docker compose up
```

Without `RECORDFLOW_IT=1`, integration tests are skipped.

### 3. Run

```
recordflow [--config PATH] [--check-config]
```

```bash
cp recordflow.toml.example recordflow.toml
PGPASSWORD=changeme ./build/linux/recordflow
```

Status messages ("Connected…", "Streaming…") go to **stderr**; decoded events go to **stdout**, so the two are easy to separate.

Stop with Ctrl+C. Verify the slot moved: `SELECT confirmed_flush_lsn FROM pg_replication_slots;` should have advanced to the last commit's end LSN, and a re-run resumes rather than replaying.

---

## pgoutput Messages

Recordflow requests `proto_version '1'` in text format. That is the stable message set — nine message types, all values delivered as strings — and it is chosen deliberately.

### Transport framing

The stream arrives as libpq `CopyBoth` data. Each frame starts with one byte identifying its kind:

| Frame | Handling |
|-------|----------|
| `w` XLogData | Carries one pgoutput message, which is decoded and printed |
| `k` Keepalive | A server heartbeat. When it asks for a reply, Recordflow sends a Standby Status Update: a message reporting how far it has processed the stream, so the slot can release older WAL |

Other frames are ignored. A malformed frame is reported to stderr and skipped.

### Message types

| Byte | Message | Carries |
|------|---------|---------|
| `B` | Begin | Final LSN of the transaction, commit timestamp, xid |
| `R` | Relation | Relation OID, schema, table name, replica identity, and per column: name, type OID, type modifier, part-of-key flag |
| `I` | Insert | Relation OID + the new tuple |
| `U` | Update | Relation OID, optional old tuple, then the new tuple |
| `D` | Delete | Relation OID + the old tuple |
| `T` | Truncate | Relation count, option flags, the affected relation OIDs |
| `C` | Commit | Flags, commit LSN, transaction end LSN, commit timestamp |
| `O` | Origin | Origin of a cascaded/bidirectional replication stream |
| `Y` | Type | Definition of a custom (non-built-in) column type |

Origin and Type messages are not decoded yet. An unrecognised message type is printed as such rather than aborting the stream.

### Rules

**Relation must precede DML — this is the central rule.** `I`, `U` and `D` identify their table only by OID; they carry no column names. The names arrive earlier, in the `R` message, which the parser caches by OID.

**When a Relation is emitted.** pgoutput sends an `R` before the first DML on a table in each session, and again after its schema changes. **DDL is never streamed**: an `ALTER TABLE` shows up only as a new `R` on the next change to that table.

**Tuple encoding.** A tuple is an Int16 column count followed by one entry per column, each starting with a kind byte:

| Kind | Meaning | Printed |
|------|---------|---------|
| `t` | Text value — Int32 byte length, then the bytes | the value |
| `n` | SQL NULL | `NULL` |
| `u` | Unchanged TOAST — the column was not modified, so the server did not resend it | `<unchanged>` |

`u` is **not** NULL: the value still exists in the row but was not resent. Values are positional and map to the cached Relation's columns in order.

---

## Glossary

| Term | Description |
|------|-------------|
| CDC | Change Data Capture — tracking and emitting row-level database changes as they happen |
| WAL | Write-Ahead Log — PostgreSQL's append-only log of every data change, written before the table files are touched |
| LSN | Log Sequence Number — a position in the WAL; used to resume and to confirm progress |
| Replication slot | A server-side object that stores a consumer's confirmed LSN and prevents PostgreSQL from discarding WAL past it |
| Publication | A server-side object defining which tables are included in a logical replication stream |
| pgoutput | PostgreSQL's built-in logical decoding plugin; formats WAL changes into a structured binary protocol |
| Replica identity | Per-table setting controlling how much of the old row appears in UPDATE and DELETE WAL records |
| TOAST | PostgreSQL's out-of-line storage for large values; unmodified TOASTed columns are omitted from the wire (`u`) |
| OID | Object Identifier — PostgreSQL's internal numeric ID for database objects |
| DML / DDL | INSERT/UPDATE/DELETE versus ALTER/DROP/RENAME |

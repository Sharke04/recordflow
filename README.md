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
| Toolchain | MSYS2 **UCRT64** GCC (C++20) |
| Build system | CMake ≥ 3.20 + Ninja |
| Dependency | libpq (`mingw-w64-ucrt-x86_64-postgresql`) |a

**Source database**

- PostgreSQL 10 or newer with `wal_level = logical`
- A role with the `REPLICATION` attribute and `SELECT` on the published tables
- A logical replication slot bound to the `pgoutput` plugin, and a publication — both pre-created (see below)

---

## Configuration

`recordflow.conf`, read from the working directory, holds **libpq connection keywords only**.

```ini
host=localhost
port=5432
dbname=mydb
user=cdc
password=changeme
```

Copy `recordflow.conf.example` to `recordflow.conf` and fill in real values.

---

## How to Run

### 1. Prepare the source database

Recordflow never changes the source database, so you set it up manually before the first run: a publication for the tables to capture, a logical replication slot bound to `pgoutput`, and a role with the `REPLICATION` attribute and `SELECT` on those tables.

For local development, `docker compose up -d` starts a PostgreSQL with `wal_level=logical` on `localhost:5432`.

### 2. Build

```bash
cmake --preset ucrt64
cmake --build --preset ucrt64
```

The `ucrt64` preset pins the UCRT64 compilers and `CMAKE_PREFIX_PATH`, and produces `build/recordflow.exe`.

### 3. Run

```
recordflow <slot> <publication>
```

```bash
./build/recordflow.exe recordflow_slot recordflow_pub
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

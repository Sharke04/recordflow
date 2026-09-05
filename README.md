# Recordflow

A small C++20 / libpq change-data-capture reader for PostgreSQL. It opens a logical replication stream against an **already existing** replication slot, decodes the `pgoutput` binary protocol into readable row-level events, prints them to stdout, and confirms the slot forward so WAL is released. No Kafka, no downstream sink, and **no modifications to the source database**.

## Table of Contents

1. [Introduction](#introduction)
2. [Requirements](#requirements)
3. [Configuration](#configuration)
4. [How to Run](#how-to-run)
5. [pgoutput Messages](#pgoutput-messages)
6. [Consistency Guarantees](#consistency-guarantees)
7. [Glossary](#glossary)

---

## Introduction

Recordflow connects to a PostgreSQL source database in replication mode, issues a single `START_REPLICATION` command against a slot you created yourself, and decodes every `pgoutput` message it receives.

```
Source DB (not owned)
      │
      │  logical replication, one slot, pgoutput, proto_version 1
      ▼
  recordflow
      ├── replication_stream   connect, START_REPLICATION, CopyBoth receive loop,
      │                        keepalive replies + Standby Status Updates
      └── pgoutput_parser      big-endian byte cursor + relation cache,
                               message decode → one readable line per message
      │
      └──► stdout              BEGIN / RELATION / INSERT / UPDATE / DELETE / TRUNCATE / COMMIT
```

Two properties define the design:

**The application creates zero database objects.** The publication, the replication slot, and the replica-identity settings are created out-of-band by you, via SQL. The only replication command Recordflow ever issues is `START_REPLICATION`; it never runs `CREATE_REPLICATION_SLOT` or `CREATE PUBLICATION`. That is what makes it safe to point at a database this service does not own. If the slot or publication is missing, it says so and exits rather than fixing the database for you.

**One connection, print-only.** A single `replication=database` connection carries the whole stream. There is no second connection for management queries, because there is nothing to manage. The current milestone ends at stdout — decoded events are printed, not published anywhere — but the slot is advanced for real, so re-runs resume where the previous run stopped instead of replaying.

---

## Requirements

**Build**

| | |
|---|---|
| Toolchain | MSYS2 **UCRT64** GCC (C++20) |
| Build system | CMake ≥ 3.20 + Ninja |
| Dependency | libpq (`mingw-w64-ucrt-x86_64-postgresql`) |

**Source database**

- PostgreSQL 10 or newer with `wal_level = logical`
- A role with the `REPLICATION` attribute and `SELECT` on the published tables
- A logical replication slot bound to the `pgoutput` plugin, and a publication — both pre-created (see below)

---

## Configuration

`recordflow.conf`, read from the working directory, holds **libpq connection keywords only** — one `key=value` per line. Blank lines and lines starting with `#` are ignored; a line without `=`, or with an empty key, aborts startup with a clear error. Every key is forwarded verbatim to `PQconnectdbParams`, so any libpq keyword works (`sslmode`, `connect_timeout`, …). Recordflow appends `replication=database` itself.

```ini
host=localhost
port=5432
dbname=mydb
user=cdc
password=changeme
```

Copy [recordflow.conf.example](recordflow.conf.example) to `recordflow.conf` and fill in real values. The file is gitignored, since it holds a password.

The slot and the publication are **not** libpq keywords, so they are not in this file — they are command-line arguments. The set of watched tables is implied by the publication, so there is no table argument either.

---

## How to Run

### 1. Prepare the source database (once, out-of-band)

```sql
-- which tables pgoutput decodes
CREATE PUBLICATION recordflow_pub FOR TABLE public.orders;

-- persistent logical slot bound to the pgoutput plugin
SELECT pg_create_logical_replication_slot('recordflow_slot', 'pgoutput');

-- UPDATE/DELETE before-image carries the PK columns
ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;
```

A slot holds WAL until it is confirmed, so an unused slot grows the source database's disk usage. Drop what you create when you are done:

```sql
SELECT pg_drop_replication_slot('recordflow_slot');
DROP PUBLICATION recordflow_pub;
```

### 2. Build

```bash
cmake --preset ucrt64
cmake --build --preset ucrt64
```

The `ucrt64` preset pins the UCRT64 compilers and `CMAKE_PREFIX_PATH`, and produces `build/recordflow.exe`.

> **MSYS2 PATH note.** Build and run with a **ucrt64-first PATH that contains no mingw64 directories**:
> ```bash
> export PATH="/c/msys64/ucrt64/bin:/c/Windows/System32:/c/Windows:/usr/bin" && hash -r
> ```
> A mixed PATH loads mingw64 (msvcrt) DLLs into the UCRT64 binary. The failures are silent rather than loud: `g++ -c` exits 1 with *no* diagnostic text at all (it forks `as.exe`, which fails DLL init), and at runtime `std::ifstream` can fail to set failbit on a missing file — so a missing `recordflow.conf` reads as empty instead of throwing, and libpq quietly falls back to its own defaults.

### 3. Run

```
recordflow <slot> <publication>
```

```bash
./build/recordflow.exe recordflow_slot recordflow_pub
```

Status messages ("Connected…", "Streaming…") go to **stderr**; decoded events go to **stdout**, so the two are easy to separate. Drive some DML from `psql` and each statement appears wrapped in a `BEGIN` / `COMMIT` pair:

```
BEGIN    xid=768 lsn=0/15E5E08 ts=2026-09-05 18:22:41.103928Z
RELATION public.orders oid=16389 identity=default cols=[*id, note]
INSERT   public.orders (id=1, note=hello)
COMMIT   lsn=0/15E5DB8 end=0/15E5E08 ts=2026-09-05 18:22:41.103928Z
BEGIN    xid=769 lsn=0/15E5F20 ts=2026-09-05 18:22:58.441002Z
UPDATE   public.orders new=(id=1, note=world)
COMMIT   lsn=0/15E5ED0 end=0/15E5F20 ts=2026-09-05 18:22:58.441002Z
```

Stop with Ctrl+C. Verify the slot moved: `SELECT confirmed_flush_lsn FROM pg_replication_slots;` should have advanced to the last commit's end LSN, and a re-run resumes rather than replaying.

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | The server ended the stream cleanly |
| `1` | Config error, connection failure, `START_REPLICATION` rejected (slot or publication missing, no `REPLICATION` privilege), or a read failure mid-stream |
| `2` | Wrong argument count — usage is printed to stderr |

A **missing slot** fails loudly: `START_REPLICATION` errors and the process exits `1`. A **missing publication** does not — PostgreSQL logs a warning and sends an empty stream, so Recordflow connects, prints nothing, and keeps waiting.

---

## pgoutput Messages

Recordflow requests `proto_version '1'` in text format. That is the stable message set — nine message types, all values delivered as strings — and it is chosen deliberately (see [Consistency Guarantees](#consistency-guarantees)).

### Transport framing

The stream arrives as libpq `CopyBoth` data. Each frame starts with one byte identifying its kind:

| Frame | Layout | Handling |
|-------|--------|----------|
| `w` XLogData | 25-byte header (`'w'` + Int64 dataStart + Int64 walEnd + Int64 sendTime), then one pgoutput message | Header is skipped; the payload goes to the parser |
| `k` Keepalive | Int64 walEnd + Int64 sendTime + Byte replyRequested | Feedback position is updated (see rules below); a Standby Status Update is sent if a reply was requested |

Anything else is ignored. A truncated or malformed frame raises inside the byte cursor, is reported to stderr as a warning, and the stream continues with the next frame — one bad message does not kill the reader.

### Message types

| Byte | Message | Carries | Printed as |
|------|---------|---------|------------|
| `B` | Begin | Final LSN of the transaction, commit timestamp, xid | `BEGIN    xid=… lsn=… ts=…` |
| `R` | Relation | Relation OID, schema, table name, replica identity, and per column: name, type OID, type modifier, part-of-key flag | `RELATION schema.table oid=… identity=… cols=[*key, other]` — and **cached** |
| `I` | Insert | Relation OID + the new (`N`) tuple | `INSERT   schema.table (col=value, …)` |
| `U` | Update | Relation OID, optional old tuple (`K` or `O`), then the new (`N`) tuple | `UPDATE   schema.table [old=(…)] new=(…)` |
| `D` | Delete | Relation OID + the old tuple (`K` or `O`) | `DELETE   schema.table old=(…)` |
| `T` | Truncate | Relation count, option flags, the affected relation OIDs | `TRUNCATE schema.table, …` |
| `C` | Commit | Flags, commit LSN, transaction end LSN, commit timestamp | `COMMIT   lsn=… end=… ts=…` — then a Standby Status Update is sent |
| `O` | Origin | Origin of a cascaded/bidirectional replication stream | `ORIGIN   (not decoded)` — placeholder |
| `Y` | Type | Definition of a custom (non-built-in) column type | `TYPE     (not decoded)` — placeholder |

An unrecognised first byte prints `?        message type 'x' (0xNN)` rather than aborting.

### Rules

**Relation must precede DML — this is the central rule.** `I`, `U` and `D` identify their table only by OID; they carry no column names. The names arrive earlier, in the `R` message, which the parser caches by OID for the life of the process. Every DML decode looks its relation up in that cache. If an OID is unknown (a `R` was missed, e.g. by attaching mid-transaction after a restart), decoding still succeeds — the table prints as `oid=16389` and columns as `col_0`, `col_1`, … — but the labels are useless, so a `R` should always be seen first in practice.

**When a Relation is emitted.** pgoutput keeps a per-session cache of the descriptors it has already sent, and emits an `R` only when the table is not yet in that cache (first DML on it since the stream opened) or when the table's schema has changed since the last `R` and new DML has arrived. Consequently **DDL is never streamed**: an `ALTER TABLE` produces no message of its own and stays invisible until the next INSERT/UPDATE/DELETE on that table forces a fresh `R` with the new column list.

**Tuple encoding.** A tuple is an Int16 column count followed by one entry per column, each starting with a kind byte:

| Kind | Meaning | Printed |
|------|---------|---------|
| `t` | Text value — Int32 byte length, then the bytes | the value |
| `n` | SQL NULL | `NULL` |
| `u` | Unchanged TOAST — the column was not modified, so the server did not resend it | `<unchanged>` |

The three stay distinct on purpose. `u` is **not** a null and **not** an empty string: it is a value that still exists in the row and was simply omitted from the wire. A consumer that treats it as NULL would erase data. Column values are positional and are zipped against the cached Relation's column list in order.

**Old-tuple rules.** `U` and `D` prefix their old tuple with a kind byte: `K` = key tuple (replica identity `DEFAULT` or `INDEX` — key columns only) or `O` = full old row (replica identity `FULL`). For `U`, the old tuple is optional; under `REPLICA IDENTITY DEFAULT` an update emits one **only if the key columns actually changed**. An ordinary column update sends the new tuple alone, which is why `UPDATE` lines often print without an `old=` part.

**Replica identity** is reported per table in the `R` message and decides what the before-image contains:

| Value | Printed | Old tuple contains |
|-------|---------|--------------------|
| `d` | `default` | Primary key columns only |
| `i` | `index` | Columns of a chosen unique index |
| `f` | `full` | Every column of the old row |
| `n` | `nothing` | Nothing — PostgreSQL refuses UPDATE/DELETE on a published table set this way |

**Encoding details.** All integers are big-endian; strings are nul-terminated. LSNs are Int64 on the wire and printed in PostgreSQL's `%X/%X` form. Timestamps are microseconds since 2000-01-01 UTC and are converted to a readable UTC string. In text format every value is a string, including numerics, booleans and timestamps.

---

## Consistency Guarantees

### Only committed transactions are streamed

**The stream carries committed transactions only, in commit order.** The walsender decodes WAL as it is written, but buffers each transaction's changes in its reorder buffer and releases them only on reaching that transaction's commit record. Three consequences follow:

- An **aborted** transaction never appears. Its buffered changes are discarded at the abort record and nothing reaches the wire — there is no rollback message to handle and no compensating logic to write downstream.
- An **in-flight** transaction never appears early. A long-running writer's rows stay invisible until it commits, then arrive together as a single `B` … `C` group.
- Changes arrive in **commit order**, not write order. Two concurrent transactions never interleave on the wire, even though their WAL records do.

This is what makes a `B`/`C` pair a safe unit of work: the reader needs no transaction state beyond "am I between a Begin and a Commit", and never has to undo anything it has already printed.

The guarantee comes from PostgreSQL, not from application logic — it is obtained by asking for `proto_version '1'`, and it is the reason to stay there. Protocol v2 and later add opt-in *streaming* of in-progress transactions, with explicit stream start/stop/abort messages that must be handled; useful for very large transactions, but it forfeits exactly this property.

### What gets confirmed back to the slot

Recordflow advances the slot, so WAL is released and re-runs resume forward. The rule: **only ever confirm WAL that has actually been decoded and printed.**

- After each `Commit`, the transaction's **end LSN, taken from the Commit message itself**, is confirmed. The XLogData header's `walEnd` is deliberately not used — it is the sender's current flush position and runs ahead of the message it accompanies, so confirming it would let a restart resume past transactions that were never printed.
- On a **keepalive**, its `walEnd` is adopted **only between transactions**. A keepalive means the sender has drained everything up to that point; taking it mid-transaction could strand the half of a transaction not yet seen. This is what releases WAL while the publication is quiet but the database is not.
- The confirmed position only ever moves forward (`max` of the old and new value).
- A **Standby Status Update** (`'r'` with written / flushed / applied all set to the confirmed LSN) is sent after every `Commit`, and whenever a keepalive requests a reply — which also keeps `wal_sender_timeout` from dropping the connection.

Because this is a print-only milestone with no durable downstream, "flushed" means "printed to stdout". The delivery guarantee is therefore **at-least-once**: confirmation follows printing, so a crash in the window between the two replays the last transaction on the next run. Duplicates are possible; gaps are not.

### What is *not* guaranteed

Scope of the current milestone, stated plainly so nothing is assumed:

- **No durable sink.** Events go to stdout. Nothing is persisted, queued, or acknowledged by a consumer.
- **No reconnection.** A dropped connection or read failure ends the process with exit code `1`; there is no backoff-and-retry loop.
- **No slot-loss recovery.** Slots are local to a PostgreSQL instance and are not replicated to standbys, so a failover destroys the slot. Recordflow reports the failure and exits; it does not recreate the slot, and there is no reconciliation or snapshot mechanism to close the resulting gap.
- **No schema-drift detection.** A changed column list is decoded from the new `R` message and printed; nothing compares it against a previous version or halts on drift.
- **No initial snapshot.** The stream delivers changes going forward only. Rows written before the slot existed are never seen.
- **Only published tables.** Changes to tables outside the publication are invisible — that is the publication's job, not the reader's.
- **`O` and `Y` are not decoded.** Origin and custom-type messages print a placeholder line. Neither carries row data, so decoded events are unaffected, but a stream using custom types or cascaded replication is not fully interpreted.

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

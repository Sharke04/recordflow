#pragma once

#include <optional>
#include <string>

#include "core/change_event.hpp"
#include "core/schema_guard.hpp"
#include "core/table_schema.hpp"
#include "core/version.hpp"
#include "runtime/errors.hpp"
#include "sink/tx_buffer.hpp"

// The target of the event stream (decision 9.4): ClickHouse in production,
// JSON Lines on stdout for development and tests.
namespace recordflow::sink {

struct PrepareResult {
    std::string target;          // e.g. "recordflow.public__orders", for the state file and messages
    std::string table_identity;  // e.g. the ClickHouse table UUID; empty if the sink has none
    bool created = false;        // the target did not exist and was created
    core::SchemaDiff diff;       // differences between source and existing target
};

// An event that cannot be written because the source schema changed
// (decision 7.1, found by the value converter). flush() first writes the
// complete transactions before the one holding the event; `stored` is the
// commit_end of the last of them. The rest of the drained events is dropped.
class FlushSchemaChangeError : public SchemaChangeError {
public:
    FlushSchemaChangeError(const SchemaChangeError& cause, std::optional<core::Lsn> stored, core::Version at)
        : SchemaChangeError(cause.what(), cause.diff()), stored_(stored), at_(at) {}
    const std::optional<core::Lsn>& stored() const { return stored_; }
    core::Version at() const { return at_; }

private:
    std::optional<core::Lsn> stored_;
    core::Version at_;
};

class Sink {
public:
    virtual ~Sink() = default;

    // On start: create or check the target for the source table.
    virtual PrepareResult prepare(const core::TableSchema& source) = 0;

    // Transaction boundaries; snapshot rows use a pseudo-transaction.
    virtual void begin() = 0;
    virtual void append(const core::ChangeEvent& event) = 0;
    virtual void commit(core::Lsn commit_end) = 0;
    virtual void abort() = 0;  // drop the open transaction (lost connection, shutdown)

    virtual BufferStats stats() const = 0;
    virtual bool in_transaction() const = 0;

    // Writes the complete transactions (and the open one's events so far if
    // `include_open`). Returns the commit_end of the last complete transaction
    // written, if any. Truncate events are applied at their position. Throws
    // FlushSchemaChangeError for an event the target cannot take.
    virtual std::optional<core::Lsn> flush(bool include_open = false) = 0;

    // Marks every row whose latest version is below `below` as deleted with
    // version `tombstone` and commit time `commit_ts` (decision 5.5).
    // Idempotent: rows already deleted are not touched again.
    virtual void sweep(core::Version below, core::Version tombstone, std::int64_t commit_ts) = 0;
};

}  // namespace recordflow::sink

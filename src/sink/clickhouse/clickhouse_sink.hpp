#pragma once

#include <optional>
#include <string>

#include "config/config.hpp"
#include "core/schema_guard.hpp"
#include "core/table_schema.hpp"
#include "runtime/backoff.hpp"
#include "runtime/errors.hpp"
#include "sink/clickhouse/client.hpp"
#include "sink/clickhouse/table_sink.hpp"
#include "sink/sink.hpp"
#include "sink/tx_buffer.hpp"

// The production sink (decisions 2.3, 2.5, 3 and 4.5): buffered transactions
// are written to one ReplacingMergeTree table, idempotently through the
// versions of decision 2.2.
namespace recordflow::sink::clickhouse {

class ClickHouseSink : public Sink {
public:
    ClickHouseSink(const config::ClickHouseSection& section, const config::RetrySection& retry);

    // Checks the database, creates the table and its _current view if the
    // table does not exist, and compares an existing table with the schema
    // derived from `source`. Throws InterventionError if the database is
    // missing or access is denied.
    PrepareResult prepare(const core::TableSchema& source) override;

    void begin() override { buffer_.begin(); }
    void append(const core::ChangeEvent& event) override { buffer_.append(event); }
    void commit(core::Lsn commit_end) override { buffer_.commit(commit_end); }
    void abort() override { buffer_.abort(); }

    BufferStats stats() const override { return buffer_.stats(); }
    bool in_transaction() const override { return buffer_.in_transaction(); }

    // One INSERT of one Block per flush. Transient errors reset the connection
    // and retry the same Block with backoff, without a limit (decision 8.3);
    // the stream is not read meanwhile (decision 4.6).
    std::optional<core::Lsn> flush(bool include_open = false) override;
    void sweep(core::Version below, core::Version tombstone) override;

    // "`recordflow`.`public__orders`"
    std::string target() const;

private:
    struct ExistingTable {
        std::string uuid;
        core::TargetSchema schema;
        core::SchemaDiff meta_diff;  // Recordflow's own columns, if changed by hand
    };

    void insert(const ::clickhouse::Block& block);
    bool database_exists();
    InterventionError missing_database() const;
    std::optional<ExistingTable> read_table();

    config::ClickHouseSection section_;
    Client client_;
    runtime::Backoff backoff_;
    std::string table_;
    core::TableSchema source_;
    std::optional<TableSink> rows_;  // set by prepare()
    TxBuffer buffer_;
};

}  // namespace recordflow::sink::clickhouse

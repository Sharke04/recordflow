#include "app/engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <format>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "core/transaction_assembler.hpp"
#include "pg/catalog.hpp"
#include "pg/conninfo.hpp"
#include "pg/query_connection.hpp"
#include "pg/replication_connection.hpp"
#include "pgoutput/decoder.hpp"
#include "pgoutput/wire.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"
#include "sink/clickhouse/clickhouse_sink.hpp"
#include "sink/flush_policy.hpp"
#include "sink/jsonl/jsonl_sink.hpp"
#include "sink/sink.hpp"

namespace recordflow::app {

namespace {

template <typename... Handlers>
struct Overloaded : Handlers... {
    using Handlers::operator()...;
};

struct StreamState {
    core::TransactionAssembler assembler;
    sink::Sink& sink;
    sink::FlushPolicy policy;
};

void handle_message(const pgoutput::Message& message, StreamState& state) {
    std::visit(Overloaded{
                   [&](const pgoutput::Begin& begin) {
                       state.assembler.begin(begin);
                       state.sink.begin();
                   },
                   [&](const pgoutput::Commit& commit) {
                       state.assembler.commit(commit);
                       state.sink.commit(commit.end_lsn);
                   },
                   [&](const pgoutput::Relation& relation) {
                       log::debug("Relation {}.{} (oid {}, {} columns)", relation.table->schema,
                                  relation.table->name, relation.table->oid, relation.table->columns.size());
                   },
                   [&](const pgoutput::Change& change) { state.sink.append(state.assembler.change(change)); },
                   [&](const pgoutput::Truncate& truncate) {
                       for (const auto& event : state.assembler.truncate(truncate)) state.sink.append(event);
                   },
                   [&](const pgoutput::LogicalMessage& message) {
                       log::debug("Logical message '{}' at {}", message.prefix, core::format_lsn(message.lsn));
                   },
                   [&](const pgoutput::Other& other) { log::debug("Ignoring message type '{}'", other.type); },
               },
               message);
}

// A difference between the source and an existing target found at startup
// stops before anything is written (decisions 7.3 and 7.4).
void check_prepared(const sink::PrepareResult& prepared, const core::TableSchema& table,
                    const std::string& target, const std::string& slot) {
    if (prepared.diff.empty()) return;
    const std::string view = target.substr(0, target.size() - 1) + "_current`";
    throw SchemaChangeError(
        std::format("schema change detected on {} (target {})\n"
                    "  detected at: startup\n"
                    "  differences:\n{}\n"
                    "  Recordflow does not handle schema changes in v1. To recover:\n"
                    "    1. stop Recordflow\n"
                    "    2. in ClickHouse: DROP VIEW {}; DROP TABLE {};\n"
                    "    3. in PostgreSQL: SELECT pg_replication_slot_advance('{}', pg_current_wal_lsn());\n"
                    "    4. start Recordflow (it recreates the table and runs a full snapshot)",
                    table.qualified(), target, prepared.diff.text(), view, target, slot),
        prepared.diff.text());
}

// Writes the buffer and confirms what is stored: the slot moves only after a
// flush (decision 4.5), never on a Commit alone.
void flush_and_confirm(pg::ReplicationConnection& conn, StreamState& state, bool include_open) {
    const std::optional<core::Lsn> stored = state.sink.flush(include_open);
    if (stored) conn.confirm(*stored);
}

void maybe_flush(pg::ReplicationConnection& conn, StreamState& state) {
    switch (state.policy.decide(state.sink.stats(), state.sink.in_transaction(), std::chrono::steady_clock::now())) {
        case sink::FlushDecision::None:
            break;
        case sink::FlushDecision::Complete:
            flush_and_confirm(conn, state, false);
            break;
        case sink::FlushDecision::Oversized:
            flush_and_confirm(conn, state, true);
            break;
    }
}

void handle_keepalive(pg::ReplicationConnection& conn, pgoutput::ByteReader& frame, StreamState& state) {
    const auto wal_end = static_cast<core::Lsn>(frame.getLong());
    frame.getLong();
    const bool reply_requested = frame.get() != 0;
    // With nothing buffered, everything up to the server's WAL end is stored
    // (decision 4.5 rule 6).
    if (!state.sink.in_transaction() && state.sink.stats().rows == 0) conn.confirm(wal_end);
    conn.maybe_send_status(std::chrono::steady_clock::now(), reply_requested);
}

int run_stream(pg::ReplicationConnection& conn, const core::TableSchema& table, StreamState& state) {
    pgoutput::PgoutputDecoder decoder;
    state.assembler.set_primary_key(table.qualified(), table.primary_key);

    constexpr std::chrono::milliseconds kPollInterval{200};
    while (!conn.ended()) {
        const auto copy = conn.poll(kPollInterval);
        if (copy) {
            pgoutput::ByteReader frame(copy->data(), copy->size());
            switch (frame.get()) {
                case 'w': {
                    frame.getLong();  // start of WAL data
                    frame.getLong();  // current end of WAL
                    frame.getLong();  // send time
                    std::optional<pgoutput::Message> message;
                    try {
                        message = decoder.decode(frame.rest(), frame.remaining());
                    } catch (const std::exception& e) {
                        log::warn("skipping malformed frame: {}", e.what());
                    }
                    if (message) handle_message(*message, state);
                    break;
                }
                case 'k':
                    handle_keepalive(conn, frame, state);
                    break;
            }
        }
        maybe_flush(conn, state);
        conn.maybe_send_status(std::chrono::steady_clock::now());
    }
    if (state.sink.in_transaction()) state.sink.abort();
    flush_and_confirm(conn, state, false);
    conn.maybe_send_status(std::chrono::steady_clock::now(), true);
    log::info("Stream ended.");
    return 0;
}

}  // namespace

Engine::Engine(config::Config config) : config_(std::move(config)) {}

int Engine::run() {
    const std::string& slot = config_.source.slot.value;
    const std::string& publication = config_.source.publication.value;

    pg::QueryConnection query(pg::build_connection_params(config_.source.conninfo.value,
                                                          pg::ConnectionKind::Query));
    const pg::QueryResult who = query.exec("SELECT current_user, current_setting('server_version')");
    log::info("Query connection open as '{}' (PostgreSQL {}).", who.rows[0][0].value_or("?"),
              who.rows[0][1].value_or("?"));

    const core::TableSchema table = pg::read_table_schema(query, config_.source.table.value);
    std::string columns;
    for (const auto& column : table.columns) {
        columns += std::format("{}{} {}{}", columns.empty() ? "" : ", ", column.name, column.type_name,
                               column.not_null ? " not null" : "");
    }
    std::string key;
    for (const auto& name : table.primary_key) key += (key.empty() ? "" : ", ") + name;
    log::info("Source table {}: primary key ({}); columns: {}", table.qualified(), key, columns);
    pg::check_publication(query, publication, table);
    pg::check_watermark_privilege(query);

    if (config_.output.sink.value == config::SinkKind::ClickHouse) {
        sink::clickhouse::ClickHouseSink clickhouse(config_.clickhouse, config_.retry);
        const sink::PrepareResult prepared = clickhouse.prepare(table);
        check_prepared(prepared, table, clickhouse.target(), slot);
        log::info("ClickHouse target {} (uuid {}){}.", clickhouse.target(), prepared.table_identity,
                  prepared.created ? ", created" : "");
        log::warn("Writes to ClickHouse are not available yet; writing JSON Lines to stdout.");
    }

    pg::ReplicationConnection conn(pg::build_connection_params(config_.source.conninfo.value,
                                                               pg::ConnectionKind::Replication));
    const pg::SystemIdentity identity = conn.identify_system();
    log::info("Connected to system {} (timeline {}, WAL at {}); streaming from slot '{}' via publication '{}'.",
              identity.system_id, identity.timeline, core::format_lsn(identity.xlogpos), slot, publication);

    sink::jsonl::JsonlSink sink;
    sink.prepare(table);
    StreamState state{core::TransactionAssembler{}, sink,
                      sink::FlushPolicy(sink::FlushLimits::from(config_.batch))};

    conn.start(slot, publication, /*start_lsn=*/0);
    log::info("Streaming (Ctrl+C to stop)...");
    return run_stream(conn, table, state);
}

}  // namespace recordflow::app

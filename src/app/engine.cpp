#include "app/engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <format>
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
#include "stream/printer/change_event_printer.hpp"

namespace recordflow::app {

namespace {

template <typename... Handlers>
struct Overloaded : Handlers... {
    using Handlers::operator()...;
};

struct StreamState {
    core::TransactionAssembler assembler;
};

void handle_message(pg::ReplicationConnection& conn, const pgoutput::Message& message,
                    StreamState& state) {
    std::visit(Overloaded{
                   [&](const pgoutput::Begin& begin) {
                       state.assembler.begin(begin);
                       print_begin(begin);
                   },
                   [&](const pgoutput::Commit& commit) {
                       state.assembler.commit(commit);
                       print_commit(commit);
                       conn.confirm(commit.end_lsn);
                   },
                   [&](const pgoutput::Relation& relation) { print_relation(*relation.table); },
                   [&](const pgoutput::Change& change) {
                       print_change_event(state.assembler.change(change));
                   },
                   [&](const pgoutput::Truncate& truncate) {
                       for (const auto& event : state.assembler.truncate(truncate)) {
                           print_change_event(event);
                       }
                   },
                   [&](const pgoutput::LogicalMessage& message) { print_logical_message(message); },
                   [&](const pgoutput::Other& other) { print_other(other); },
               },
               message);
}

void handle_keepalive(pg::ReplicationConnection& conn, pgoutput::ByteReader& frame,
                      StreamState& state) {
    const auto wal_end = static_cast<core::Lsn>(frame.getLong());
    frame.getLong();
    const bool reply_requested = frame.get() != 0;
    if (!state.assembler.in_transaction()) {
        conn.confirm(wal_end);
    }
    conn.maybe_send_status(std::chrono::steady_clock::now(), reply_requested);
}

int run_stream(pg::ReplicationConnection& conn, const core::TableSchema& table) {
    pgoutput::PgoutputDecoder decoder;
    StreamState state;
    state.assembler.set_primary_key(table.qualified(), table.primary_key);

    constexpr std::chrono::milliseconds kPollInterval{200};
    while (!conn.ended()) {
        const auto copy = conn.poll(kPollInterval);
        conn.maybe_send_status(std::chrono::steady_clock::now());
        if (!copy) continue;  // nothing arrived; timers and signals are checked here later
        try {
            pgoutput::ByteReader frame(copy->data(), copy->size());
            switch (frame.get()) {
                case 'w':
                    frame.getLong();  // start of WAL data
                    frame.getLong();  // current end of WAL
                    frame.getLong();  // send time
                    handle_message(conn, decoder.decode(frame.rest(), frame.remaining()), state);
                    break;
                case 'k':
                    handle_keepalive(conn, frame, state);
                    break;
            }
        } catch (const std::exception& e) {
            log::warn("skipping malformed frame: {}", e.what());
        }
    }
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

    pg::ReplicationConnection conn(pg::build_connection_params(config_.source.conninfo.value,
                                                               pg::ConnectionKind::Replication));
    const pg::SystemIdentity identity = conn.identify_system();
    log::info("Connected to system {} (timeline {}, WAL at {}); streaming from slot '{}' via publication '{}'.",
              identity.system_id, identity.timeline, core::format_lsn(identity.xlogpos), slot, publication);

    conn.start(slot, publication, /*start_lsn=*/0);
    log::info("Streaming (Ctrl+C to stop)...");
    return run_stream(conn, table);
}

}  // namespace recordflow::app

#include "app/engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>
#include <variant>

#include "core/transaction_assembler.hpp"
#include "pg/conninfo.hpp"
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
    core::Lsn confirmed = 0;
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
                       state.confirmed = std::max(state.confirmed, commit.end_lsn);
                       conn.send_standby_status(state.confirmed);
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
        state.confirmed = std::max(state.confirmed, wal_end);
    }
    if (reply_requested) {
        conn.send_standby_status(state.confirmed);
    }
}

int run_stream(pg::ReplicationConnection& conn) {
    pgoutput::PgoutputDecoder decoder;
    StreamState state;

    constexpr std::chrono::milliseconds kPollInterval{200};
    while (!conn.ended()) {
        const auto copy = conn.poll(kPollInterval);
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

    pg::ReplicationConnection conn(pg::build_connection_params(config_.source.conninfo.value,
                                                               pg::ConnectionKind::Replication));
    log::info("Connected; streaming from slot '{}' via publication '{}'.", slot, publication);

    conn.start(slot, publication);
    log::info("Streaming (Ctrl+C to stop)...");
    return run_stream(conn);
}

}  // namespace recordflow::app

#include "app/engine.hpp"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>

#include "config.hpp"
#include "pg/replication_connection.hpp"
#include "pgoutput/decoder.hpp"
#include "pgoutput/wire.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"
#include "stream/printer/change_event_printer.hpp"

namespace recordflow::app {

namespace {

struct StreamState {
    std::uint64_t confirmed = 0;
    bool in_transaction = false;
};

void handle_xlog_data(pg::ReplicationConnection& conn, pgoutput::ByteReader& frame,
                      pgoutput::PgoutputDecoder& decoder, StreamState& state) {
    frame.getLong();
    frame.getLong();
    frame.getLong();
    const pgoutput::ParsedMessage msg =
        decoder.handle_message(frame.rest(), frame.remaining());

    if (msg.relation) {
        print_relation(*msg.relation);
    }
    for (const core::ChangeEvent& event : msg.events) {
        print_change_event(event);
    }

    if (msg.type == 'B') {
        state.in_transaction = true;
    } else if (msg.type == 'C') {
        state.in_transaction = false;
        state.confirmed = std::max(state.confirmed, msg.commit_end_lsn);
        conn.send_standby_status(state.confirmed);
    }
}

void handle_keepalive(pg::ReplicationConnection& conn, pgoutput::ByteReader& frame,
                      StreamState& state) {
    const auto wal_end = static_cast<std::uint64_t>(frame.getLong());
    frame.getLong();
    const bool reply_requested = frame.get() != 0;
    if (!state.in_transaction) {
        state.confirmed = std::max(state.confirmed, wal_end);
    }
    if (reply_requested) {
        conn.send_standby_status(state.confirmed);
    }
}

int run_stream(pg::ReplicationConnection& conn) {
    pgoutput::PgoutputDecoder decoder;
    StreamState state;

    while (auto copy = conn.read()) {
        try {
            pgoutput::ByteReader frame(copy->data(), copy->size());
            switch (frame.get()) {
                case 'w':
                    handle_xlog_data(conn, frame, decoder, state);
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
    // Connection parameters still come from the legacy recordflow.conf until
    // the [source] conninfo is wired in.
    Config legacy;
    try {
        legacy = load_config();
    } catch (const std::exception& e) {
        throw ConfigError(e.what());
    }

    const std::string& slot = config_.source.slot.value;
    const std::string& publication = config_.source.publication.value;

    pg::ReplicationConnection conn(std::move(legacy.params));
    log::info("Connected; streaming from slot '{}' via publication '{}'.", slot, publication);

    conn.start(slot, publication);
    log::info("Streaming (Ctrl+C to stop)...");
    return run_stream(conn);
}

}  // namespace recordflow::app

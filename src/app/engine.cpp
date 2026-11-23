#include "app/engine.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "app/startup_decision.hpp"
#include "core/transaction_assembler.hpp"
#include "pg/catalog.hpp"
#include "pg/conninfo.hpp"
#include "pg/query_connection.hpp"
#include "pg/replication_connection.hpp"
#include "pgoutput/decoder.hpp"
#include "pgoutput/wire.hpp"
#include "runtime/backoff.hpp"
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
void check_prepared(const sink::PrepareResult& prepared, const core::TableSchema& table, const std::string& slot) {
    if (prepared.diff.empty()) return;
    throw SchemaChangeError(
        std::format("schema change detected on {} (target {})\n"
                    "  detected at: startup\n"
                    "  differences:\n{}\n"
                    "  Recordflow does not handle schema changes in v1. To recover:\n"
                    "    1. stop Recordflow\n"
                    "    2. in ClickHouse: DROP VIEW {}_current; DROP TABLE {};\n"
                    "    3. in PostgreSQL: SELECT pg_replication_slot_advance('{}', pg_current_wal_lsn());\n"
                    "    4. start Recordflow (it recreates the table and runs a full snapshot)",
                    table.qualified(), prepared.target, prepared.diff.text(), prepared.target, prepared.target,
                    slot),
        prepared.diff.text());
}

// Writes the buffer and confirms what is stored: the slot moves only after a
// flush (decision 4.5), never on a Commit alone.
void flush_and_confirm(pg::ReplicationConnection& conn, StreamState& state, bool include_open) {
    const std::optional<core::Lsn> stored = state.sink.flush(include_open);
    if (stored) conn.confirm(*stored);
}

void maybe_flush(pg::ReplicationConnection& conn, StreamState& state) {
    const sink::BufferStats stats = state.sink.stats();
    switch (state.policy.decide(stats, state.sink.in_transaction(), std::chrono::steady_clock::now())) {
        case sink::FlushDecision::None:
            break;
        case sink::FlushDecision::Complete:
            flush_and_confirm(conn, state, false);
            break;
        case sink::FlushDecision::Oversized:
            // Decision 4.5 rule 2: the open transaction is written so far to
            // bound memory, but the slot moves only past complete ones; a
            // replay after a crash rewrites it with the same versions.
            log::info("Open transaction passed the hard batch limit ({} rows, {} MB buffered); writing it before "
                      "its Commit.", stats.rows, stats.bytes >> 20);
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

std::string describe_columns(const core::TableSchema& table) {
    std::string columns;
    for (const auto& column : table.columns) {
        columns += std::format("{}{} {}{}", columns.empty() ? "" : ", ", column.name, column.type_name,
                               column.not_null ? " not null" : "");
    }
    std::string key;
    for (const auto& name : table.primary_key) key += (key.empty() ? "" : ", ") + name;
    return std::format("primary key ({}); columns: {}", key, columns);
}

const char* action_name(StartupAction action) {
    switch (action) {
        case StartupAction::Resume:        return "resume";
        case StartupAction::ResetSnapshot: return "reset the snapshot";
        case StartupAction::NewSnapshot:   return "new snapshot";
        case StartupAction::Refuse:        return "refuse";
    }
    return "?";
}

}  // namespace

Engine::Engine(config::Config config)
    : config_(std::move(config)), store_(config_.state.data_dir.value) {}

Engine::~Engine() = default;

void Engine::retry_transient(std::string_view what, const std::function<void()>& step,
                             const std::function<void()>& on_failure) {
    runtime::Backoff backoff{std::string(what), config_.retry};
    while (true) {
        try {
            step();
            backoff.success();
            return;
        } catch (const TransientError& e) {
            if (on_failure) on_failure();
            std::this_thread::sleep_for(backoff.failure(e.what()));
        }
    }
}

void Engine::startup() {
    const std::string& slot = config_.source.slot.value;
    const std::string& publication = config_.source.publication.value;

    // 1. One instance per data directory, then the stored state (decision 4.2).
    store_.prepare_directory();
    lock_.emplace(store_.directory());
    const std::optional<state::State> stored = store_.load();

    // 2. Connections; the databases may still be starting (decision 8.2).
    query_ = std::make_unique<pg::QueryConnection>(
        pg::build_connection_params(config_.source.conninfo.value, pg::ConnectionKind::Query));
    retry_transient("PostgreSQL connection", [&] {
        const pg::QueryResult who = query_->exec("SELECT current_user, current_setting('server_version')");
        log::info("Query connection open as '{}' (PostgreSQL {}).", who.rows[0][0].value_or("?"),
                  who.rows[0][1].value_or("?"));
    });

    // 3. IDENTIFY_SYSTEM on the replication connection.
    pg::SystemIdentity identity;
    retry_transient(
        "PostgreSQL replication connection",
        [&] {
            repl_ = std::make_unique<pg::ReplicationConnection>(pg::build_connection_params(
                config_.source.conninfo.value, pg::ConnectionKind::Replication));
            identity = repl_->identify_system();
        },
        [&] { repl_.reset(); });
    log::info("Connected to system {} (timeline {}, WAL at {}).", identity.system_id, identity.timeline,
              core::format_lsn(identity.xlogpos));

    // 4. Catalog, publication and the watermark privilege (decision 5.1).
    retry_transient("PostgreSQL catalog", [&] {
        table_ = pg::read_table_schema(*query_, config_.source.table.value);
        pg::check_publication(*query_, publication, table_);
        pg::check_watermark_privilege(*query_);
    });
    log::info("Source table {}: {}", table_.qualified(), describe_columns(table_));

    // 5. The target; a schema difference stops before the stream opens (decision 7.3).
    if (config_.output.sink.value == config::SinkKind::ClickHouse) {
        sink_ = std::make_unique<sink::clickhouse::ClickHouseSink>(config_.clickhouse, config_.retry);
    } else {
        sink_ = std::make_unique<sink::jsonl::JsonlSink>();
    }
    sink::PrepareResult prepared;
    retry_transient("ClickHouse", [&] { prepared = sink_->prepare(table_); });
    check_prepared(prepared, table_, slot);
    log::info("Target {}{}{}.", prepared.target,
              prepared.table_identity.empty() ? "" : " (uuid " + prepared.table_identity + ")",
              prepared.created ? ", created" : "");

    // 6. The slot's confirmed position.
    pg::SlotInfo slot_info;
    retry_transient("PostgreSQL slot lookup", [&] { slot_info = pg::read_slot(*query_, slot); });

    // 7. Identity checks and the snapshot decision (decisions 4.3 and 5.1).
    StartupFacts facts;
    facts.stored = stored;
    facts.slot = slot;
    facts.source_table = table_.qualified();
    facts.target_table = prepared.target;
    facts.system_id = identity.system_id;
    facts.target_uuid = prepared.table_identity;
    facts.confirmed_flush_lsn = slot_info.confirmed_flush_lsn;
    facts.snapshot_on_start = config_.snapshot.on_start.value;
    StartupDecision decision = decide_startup(facts);
    if (decision.action == StartupAction::Refuse) {
        throw InterventionError(std::format("cannot start with state file {}: {}", store_.file().string(),
                                            decision.messages.front()));
    }
    for (const auto& message : decision.messages) log::warn("{}", message);
    state_ = std::move(*decision.state);
    store_.save(state_);
    log::info("State {}: {}; last flushed {}, slot confirmed {}, snapshot {}.", store_.file().string(),
              action_name(decision.action), core::format_lsn(state_.last_flushed_lsn),
              core::format_lsn(slot_info.confirmed_flush_lsn), state::to_string(state_.snapshot.status));
    if (state_.snapshot.status != state::SnapshotStatus::Done) {
        log::warn("Snapshots are not implemented yet: only changes from the slot's position are streamed.");
    }

    // 8. START_REPLICATION from the last stored position (decision 8.2).
    retry_transient(
        "PostgreSQL replication start",
        [&] {
            if (!repl_) {
                repl_ = std::make_unique<pg::ReplicationConnection>(pg::build_connection_params(
                    config_.source.conninfo.value, pg::ConnectionKind::Replication));
            }
            repl_->start(slot, publication, state_.last_flushed_lsn);
        },
        [&] { repl_.reset(); });
    log::info("Streaming from slot '{}' via publication '{}' at {}.", slot, publication,
              core::format_lsn(state_.last_flushed_lsn));
}

int Engine::stream() {
    StreamState state{core::TransactionAssembler{}, *sink_, sink::FlushPolicy(sink::FlushLimits::from(config_.batch))};
    pg::ReplicationConnection& conn = *repl_;
    pgoutput::PgoutputDecoder decoder;
    state.assembler.set_primary_key(table_.qualified(), table_.primary_key);

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

int Engine::run() {
    startup();
    return stream();
}

}  // namespace recordflow::app

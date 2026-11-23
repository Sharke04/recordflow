#include "app/engine.hpp"

#include <algorithm>
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
#include "core/schema_guard.hpp"
#include "core/transaction_assembler.hpp"
#include "pg/catalog.hpp"
#include "pg/conninfo.hpp"
#include "pg/errors.hpp"
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

// Indents every line of a schema difference by four spaces.
std::string indented(const std::string& diff) {
    std::string out;
    std::size_t begin = 0;
    while (begin <= diff.size()) {
        std::size_t end = diff.find('\n', begin);
        if (end == std::string::npos) end = diff.size();
        std::string line = diff.substr(begin, end - begin);
        line.erase(0, line.find_first_not_of(' '));
        if (!line.empty()) out += (out.empty() ? "    " : "\n    ") + line;
        begin = end + 1;
    }
    return out;
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
    : config_(std::move(config)),
      store_(config_.state.data_dir.value),
      policy_(sink::FlushLimits::from(config_.batch)) {}

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
    if (!prepared.diff.empty()) {
        // Before the stream opens, nothing is written (decision 7.3).
        throw SchemaChangeError(
            schema_change_report(prepared.target, "startup",
                                 stored ? std::optional(stored->last_flushed_lsn) : std::nullopt, std::nullopt,
                                 prepared.diff.text()),
            prepared.diff.text());
    }
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
    stored_lsn_ = state_.last_flushed_lsn;
    last_checkpoint_ = std::chrono::steady_clock::now();
    assembler_.set_primary_key(table_.qualified(), table_.primary_key);
    log::info("State {}: {}; last flushed {}, slot confirmed {}, snapshot {}.", store_.file().string(),
              action_name(decision.action), core::format_lsn(state_.last_flushed_lsn),
              core::format_lsn(slot_info.confirmed_flush_lsn), state::to_string(state_.snapshot.status));
    if (state_.snapshot.status != state::SnapshotStatus::Done) {
        log::warn("Snapshots are not implemented yet: only changes from the slot's position are streamed.");
    }

    // 8. START_REPLICATION from the last stored position (decision 8.2).
    start_replication();
}

void Engine::start_replication() {
    const std::string& slot = config_.source.slot.value;
    const std::string& publication = config_.source.publication.value;
    const std::chrono::seconds busy_timeout{config_.retry.slot_busy_timeout_s.value};
    std::optional<std::chrono::steady_clock::time_point> busy_since;
    retry_transient(
        "PostgreSQL replication",
        [&] {
            try {
                connect_and_start(slot, publication);
            } catch (const pg::SlotBusyError&) {
                // After our own lost connection the old walsender may hold the
                // slot until wal_sender_timeout; past the limit it is someone
                // else (decision 8.1).
                const auto now = std::chrono::steady_clock::now();
                if (!busy_since) busy_since = now;
                if (now - *busy_since < busy_timeout) throw;
                std::string holder;
                try {
                    holder = pg::describe_slot_holder(*query_, slot);
                } catch (const std::exception& lookup) {
                    holder = std::string("holder unknown: ") + lookup.what();
                }
                throw InterventionError(std::format(
                    "replication slot '{}' is still in use after {} s ({}). Another Recordflow instance or "
                    "another client is streaming from it; stop it or give this instance its own slot",
                    slot, busy_timeout.count(), holder));
            }
        },
        [&] { repl_.reset(); });
    repl_->confirm(state_.last_flushed_lsn);
    log::info("Streaming from slot '{}' via publication '{}' at {}.", slot, publication,
              core::format_lsn(state_.last_flushed_lsn));
}

void Engine::connect_and_start(const std::string& slot, const std::string& publication) {
    if (!repl_) {
        repl_ = std::make_unique<pg::ReplicationConnection>(
            pg::build_connection_params(config_.source.conninfo.value, pg::ConnectionKind::Replication));
        // After a physical failover the system_id stays the same; another one
        // is another cluster (decision 4.3).
        const pg::SystemIdentity identity = repl_->identify_system();
        if (identity.system_id != state_.source.system_id) {
            throw InterventionError(std::format(
                "the source is now another PostgreSQL cluster (system_id {}, the state file has {}); stopping. "
                "Drop the ClickHouse table {} and its _current view and remove the state file to start over "
                "with a full snapshot",
                identity.system_id, state_.source.system_id, state_.target.table));
        }
    }
    try {
        // The server starts at the later of this and the slot's position.
        repl_->start(slot, publication, state_.last_flushed_lsn);
    } catch (const InterventionError& e) {
        // A missing or invalidated slot or a dropped publication (decision 8.6).
        throw InterventionError(std::format(
            "{}. If the slot is gone, create a new one; the next start sees the gap and runs a full snapshot",
            e.what()));
    }
}

void Engine::recover(const TransientError& error) {
    log::warn("{}; reconnecting.", error.what());
    // Decision 8.2: only the open transaction is dropped; the new session
    // resends it, together with its Relation messages.
    if (sink_->in_transaction()) sink_->abort();
    assembler_.reset();
    decoder_.reset();
    repl_.reset();
    // Complete transactions are written first; if ClickHouse was the reason
    // the stream stalled, this waits for it before the stream is reopened.
    flush_and_checkpoint(false);
    checkpoint_if_due(true);
    start_replication();
}

bool Engine::tracked(const core::ChangeEvent& event) const {
    return event.schema_name == table_.schema && event.table == table_.name;
}

void Engine::check_relation(const pgoutput::TableInfo& relation) {
    if (!core::same_table(table_, relation)) {
        log::debug("Relation {}.{} (oid {}) is not the tracked table; ignored.", relation.schema, relation.name,
                   relation.oid);
        return;
    }
    // Types the known schema does not describe are looked up in the catalog.
    const core::SchemaDiff diff = core::compare_relation(table_, relation, [&](core::ColumnSchema& column) {
        const pg::QueryResult type = query_->exec(
            "SELECT format_type($1::oid, $2::int), typtype = 'e' FROM pg_type WHERE oid = $1::oid",
            {std::to_string(column.type_oid), std::to_string(column.type_mod)});
        if (type.empty()) return;
        column.type_name = type.rows[0][0].value_or("");
        column.is_enum = type.rows[0][1].value_or("") == "t";
    });
    if (diff.empty()) {
        log::debug("Relation {}.{} matches the target.", relation.schema, relation.name);
        return;
    }
    stop_on_schema_change("Relation message", transaction_lsn_, diff.text());
}

std::string Engine::schema_change_report(const std::string& target, std::string_view where,
                                         std::optional<core::Lsn> stored, std::optional<core::Lsn> detected,
                                         const std::string& diff) const {
    const std::string& slot = config_.source.slot.value;
    return std::format(
        "schema change detected on {} (target {})\n"
        "  detected at: {}{}\n"
        "  applied and confirmed up to: {}\n"
        "  differences:\n{}\n"
        "  Recordflow does not handle schema changes in v1. To recover:\n"
        "    1. stop Recordflow\n"
        "    2. in ClickHouse: DROP VIEW {}_current; DROP TABLE {};\n"
        "    3. in PostgreSQL: SELECT pg_replication_slot_advance('{}', pg_current_wal_lsn());\n"
        "    4. start Recordflow (it recreates the table and runs a full snapshot)",
        config_.source.table.value, target, where, detected ? ", LSN " + core::format_lsn(*detected) : "",
        stored ? core::format_lsn(*stored) : "nothing stored yet", indented(diff), target, target, slot);
}

void Engine::stop_on_schema_change(std::string_view where, core::Lsn detected, const std::string& diff) {
    // Decision 7.3: the transaction holding the change is dropped, the complete
    // ones before it are stored and confirmed, then exit code 3.
    if (sink_->in_transaction()) sink_->abort();
    flush_and_checkpoint(false);
    confirm_now();
    throw SchemaChangeError(schema_change_report(state_.target.table, where, state_.last_flushed_lsn, detected,
                                                 diff),
                            diff);
}

void Engine::confirm_now() {
    checkpoint_if_due(true);
    if (repl_) repl_->maybe_send_status(std::chrono::steady_clock::now(), true);
}

void Engine::dispatch(const pgoutput::Message& message) {
    std::visit(Overloaded{
                   [&](const pgoutput::Begin& begin) {
                       transaction_lsn_ = begin.final_lsn;
                       assembler_.begin(begin);
                       sink_->begin();
                   },
                   [&](const pgoutput::Commit& commit) {
                       assembler_.commit(commit);
                       sink_->commit(commit.end_lsn);
                   },
                   // Before any change of the same transaction (decision 7.2).
                   [&](const pgoutput::Relation& relation) { check_relation(*relation.table); },
                   [&](const pgoutput::Change& change) {
                       // Other tables of the publication are skipped (decision 4.4).
                       core::ChangeEvent event = assembler_.change(change);
                       if (tracked(event)) sink_->append(event);
                   },
                   [&](const pgoutput::Truncate& truncate) {
                       for (const auto& event : assembler_.truncate(truncate)) {
                           if (tracked(event)) sink_->append(event);
                       }
                   },
                   [&](const pgoutput::LogicalMessage& logical) {
                       log::debug("Logical message '{}' at {}", logical.prefix, core::format_lsn(logical.lsn));
                   },
                   [&](const pgoutput::Other& other) { log::debug("Ignoring message type '{}'", other.type); },
               },
               message);
}

void Engine::handle_frame(const pg::CopyData& copy) {
    pgoutput::ByteReader frame(copy.data(), copy.size());
    switch (frame.get()) {
        case 'w': {
            frame.getLong();  // start of WAL data
            frame.getLong();  // current end of WAL
            frame.getLong();  // send time
            // A malformed message is an unexpected error: skipping it would lose data.
            dispatch(decoder_.decode(frame.rest(), frame.remaining()));
            break;
        }
        case 'k': {
            const auto wal_end = static_cast<core::Lsn>(frame.getLong());
            frame.getLong();  // send time
            const bool reply_requested = frame.get() != 0;
            // Keepalives move the position only with an empty buffer (decision 4.1).
            const sink::BufferStats stats = sink_->stats();
            if (!sink_->in_transaction() && stats.rows == 0 && !stats.commit_pending) advance(wal_end, false);
            if (reply_requested) repl_->maybe_send_status(std::chrono::steady_clock::now(), true);
            break;
        }
    }
}

void Engine::advance(core::Lsn lsn, bool now) {
    stored_lsn_ = std::max(stored_lsn_, lsn);
    checkpoint_if_due(now);
}

void Engine::checkpoint_if_due(bool force) {
    if (stored_lsn_ <= state_.last_flushed_lsn) return;
    const auto now = std::chrono::steady_clock::now();
    if (!force && now - last_checkpoint_ < kPositionCheckpointInterval) return;
    // The state file first, then the slot (decision 4.2): a crash in between
    // only replays what is already stored.
    state_.last_flushed_lsn = stored_lsn_;
    store_.save(state_);
    last_checkpoint_ = now;
    if (repl_) {
        repl_->confirm(stored_lsn_);
        repl_->maybe_send_status(now);
    }
}

void Engine::flush_and_checkpoint(bool include_open) {
    const std::optional<core::Lsn> stored = sink_->flush(include_open);
    if (stored) advance(*stored, true);
}

void Engine::maybe_flush() {
    const sink::BufferStats stats = sink_->stats();
    switch (policy_.decide(stats, sink_->in_transaction(), std::chrono::steady_clock::now())) {
        case sink::FlushDecision::None:
            break;
        case sink::FlushDecision::Complete:
            if (stats.rows == 0) {
                // Only a position (transactions without rows for the table):
                // the state file may wait for the interval.
                if (const auto stored = sink_->flush(false)) advance(*stored, false);
            } else {
                flush_and_checkpoint(false);
            }
            break;
        case sink::FlushDecision::Oversized:
            // Decision 4.5 rule 2: the open transaction is written so far to
            // bound memory, but the slot moves only past complete ones; a
            // replay after a crash rewrites it with the same versions.
            log::info("Open transaction passed the hard batch limit ({} rows, {} MB buffered); writing it before "
                      "its Commit.", stats.rows, stats.bytes >> 20);
            flush_and_checkpoint(true);
            break;
    }
}

void Engine::stream() {
    constexpr std::chrono::milliseconds kPollInterval{200};
    try {
        while (true) {
            try {
                if (const auto copy = repl_->poll(kPollInterval)) handle_frame(*copy);
                if (repl_->ended()) {
                    throw TransientError(Resource::PostgresReplication, "the server ended the replication stream");
                }
                maybe_flush();
                checkpoint_if_due();
                repl_->maybe_send_status(std::chrono::steady_clock::now());
            } catch (const TransientError& e) {
                recover(e);
            }
        }
    } catch (const sink::FlushSchemaChangeError& e) {
        // Found by the converter while writing: the sink stored the complete
        // transactions before the failing event (decision 7.3).
        if (sink_->in_transaction()) sink_->abort();
        if (e.stored()) advance(*e.stored(), true);
        confirm_now();
        throw SchemaChangeError(schema_change_report(state_.target.table, "converter (" + std::string(e.what()) + ")",
                                                     state_.last_flushed_lsn, e.at().commit_lsn, e.diff()),
                                e.diff());
    }
}

int Engine::run() {
    startup();
    stream();
}

}  // namespace recordflow::app

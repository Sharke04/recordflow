#include "sink/clickhouse/table_sink.hpp"

#include <absl/numeric/int128.h>

#include <algorithm>
#include <format>
#include <stdexcept>

#include "runtime/errors.hpp"
#include "runtime/log.hpp"
#include "sink/clickhouse/ddl.hpp"

namespace recordflow::sink::clickhouse {

namespace ch = ::clickhouse;

namespace {

// Values of the Enum8 types in kOpEnum and kSourceEnum.
std::int8_t op_value(core::Op op) {
    switch (op) {
        case core::Op::Insert: return 1;
        case core::Op::Update: return 2;
        case core::Op::Delete: return 3;
        case core::Op::Read:   return 4;
        default: break;
    }
    throw std::logic_error("no _rf_op value for this event");
}

ch::TypeRef op_type() {
    return ch::Type::CreateEnum8({{"c", 1}, {"u", 2}, {"d", 3}, {"r", 4}});
}

ch::TypeRef source_type() {
    return ch::Type::CreateEnum8({{"log", 1}, {"snapshot", 2}});
}

}  // namespace

TableSink::TableSink(const core::TableSchema& source) : table_(source.qualified()) {
    for (const auto& column : source.columns) {
        const bool in_key = std::find(source.primary_key.begin(), source.primary_key.end(), column.name) !=
                            source.primary_key.end();
        columns_.emplace_back(column.name, map_type(column, in_key));
    }
    reset_meta();
}

void TableSink::reset_meta() {
    versions_ = std::make_shared<ch::ColumnUInt128>();
    deleted_ = std::make_shared<ch::ColumnUInt8>();
    ops_ = std::make_shared<ch::ColumnEnum8>(op_type());
    sources_ = std::make_shared<ch::ColumnEnum8>(source_type());
    commit_ts_ = std::make_shared<ch::ColumnDateTime64>(6, "UTC");
}

const core::Value* TableSink::find(const core::Row& row, const std::string& name, std::size_t hint) {
    if (hint < row.size() && row[hint].first == name) return &row[hint].second;
    for (const auto& [column, value] : row) {
        if (column == name) return &value;
    }
    return nullptr;
}

void TableSink::add_meta(const core::ChangeEvent& event, core::Op op, bool deleted) {
    versions_->Append(absl::MakeUint128(event.version.commit_lsn, event.version.seq));
    deleted_->Append(deleted ? 1 : 0);
    ops_->Append(op_value(op));
    sources_->Append(event.source == core::EventSource::Log ? 1 : 2);
    commit_ts_->Append(event.commit_ts + core::kPostgresEpochUnixMicros);
    ++rows_;
}

void TableSink::add_upsert(const core::ChangeEvent& event) {
    if (!event.after) throw std::logic_error(std::format("{} event without a new row", table_));
    const core::Row& row = *event.after;
    // Look up every column first: a missing one leaves no partial row. (A
    // conversion error stops the process, so its partial block is discarded.)
    std::vector<const core::Value*> values;
    values.reserve(columns_.size());
    for (std::size_t i = 0; i < columns_.size(); ++i) {
        const core::Value* value = find(row, columns_[i].name(), i);
        if (value == nullptr) {
            throw SchemaChangeError(std::format("column \"{}\" of {} is missing from the change at {}",
                                                columns_[i].name(), table_, event.version.str()),
                                    std::format("    - dropped column \"{}\"", columns_[i].name()));
        }
        values.push_back(value);
    }
    for (std::size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].append(*values[i]) == AppendOutcome::UnchangedToast) ++unchanged_toast_;
    }
    add_meta(event, event.op, false);
}

void TableSink::add_delete(const core::ChangeEvent& event, const std::vector<std::string>& key) {
    if (key.size() != event.key_columns.size() || key.empty()) {
        throw std::logic_error(std::format("{} delete at {} without its key", table_, event.version.str()));
    }
    for (auto& column : columns_) {
        const auto it = std::find(event.key_columns.begin(), event.key_columns.end(), column.name());
        if (it == event.key_columns.end()) {
            column.append_default();
        } else {
            const auto index = static_cast<std::size_t>(it - event.key_columns.begin());
            column.append(core::Value{core::Value::Kind::Text, key[index]});
        }
    }
    add_meta(event, core::Op::Delete, true);
}

void TableSink::add(const core::ChangeEvent& event) {
    switch (event.op) {
        case core::Op::Insert:
        case core::Op::Read:
            add_upsert(event);
            return;
        case core::Op::Update: {
            // With REPLICA IDENTITY DEFAULT pgoutput sends the old key only
            // when it changed; with FULL the old row comes every time.
            if (event.before) {
                std::vector<std::string> old_key;
                for (const auto& name : event.key_columns) {
                    const core::Value* value = find(*event.before, name, 0);
                    if (value == nullptr || value->kind != core::Value::Kind::Text) break;
                    old_key.push_back(value->text);
                }
                if (old_key.size() == event.key_columns.size() && old_key != event.key) add_delete(event, old_key);
            }
            add_upsert(event);
            return;
        }
        case core::Op::Delete:
            add_delete(event, event.key);
            return;
        case core::Op::Truncate:
        case core::Op::Unknown:
            break;
    }
    throw std::logic_error(std::format("{}: event at {} is not a row change", table_, event.version.str()));
}

ch::Block TableSink::take() {
    ch::Block block;
    for (auto& column : columns_) block.AppendColumn(column.name(), column.take());
    block.AppendColumn(std::string(kVersionColumn), versions_);
    block.AppendColumn(std::string(kIsDeletedColumn), deleted_);
    block.AppendColumn(std::string(kOpColumn), ops_);
    block.AppendColumn(std::string(kSourceColumn), sources_);
    block.AppendColumn(std::string(kCommitTsColumn), commit_ts_);
    reset_meta();

    if (unchanged_toast_ > 0) {
        log::warn("{}: {} unchanged TOAST values written as NULL or default; the next snapshot corrects them",
                  table_, unchanged_toast_);
    }
    rows_ = 0;
    unchanged_toast_ = 0;
    return block;
}

}  // namespace recordflow::sink::clickhouse

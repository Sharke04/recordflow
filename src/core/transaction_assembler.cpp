#include "core/transaction_assembler.hpp"

#include <stdexcept>
#include <utility>

namespace recordflow::core {

namespace {

std::vector<std::string> key_values(const std::vector<std::string>& columns, const std::optional<Row>& row) {
    std::vector<std::string> values;
    if (!row) return values;
    for (const auto& column : columns) {
        for (const auto& [name, value] : *row) {
            if (name == column) {
                values.push_back(value.text);
                break;
            }
        }
    }
    return values;
}

}  // namespace

void TransactionAssembler::set_primary_key(const std::string& qualified_table,
                                           std::vector<std::string> columns) {
    primary_keys_[qualified_table] = std::move(columns);
}

void TransactionAssembler::begin(const pgoutput::Begin& begin) {
    current_ = begin;
    in_transaction_ = true;
    next_seq_ = 0;
}

ChangeEvent TransactionAssembler::event_for(Op op, const pgoutput::TableInfo& table) {
    if (!in_transaction_) throw std::runtime_error("change outside of a transaction");

    ChangeEvent event;
    event.op = op;
    event.source = EventSource::Log;
    event.schema_name = table.schema;
    event.table = table.name;
    const auto pk = primary_keys_.find(table.schema + "." + table.name);
    if (pk != primary_keys_.end()) {
        event.key_columns = pk->second;
    } else {
        for (const auto& column : table.columns) {
            if (column.part_of_key) event.key_columns.push_back(column.name);
        }
    }
    event.xid = current_.xid;
    event.commit_ts = current_.commit_ts;
    event.version = Version{current_.final_lsn, next_seq_++};
    return event;
}

ChangeEvent TransactionAssembler::change(const pgoutput::Change& change) {
    ChangeEvent event = event_for(change.op, *change.table);
    event.before = change.before;
    event.after = change.after;
    event.key = key_values(event.key_columns, change.op == Op::Delete ? change.before : change.after);
    return event;
}

std::vector<ChangeEvent> TransactionAssembler::truncate(const pgoutput::Truncate& truncate) {
    std::vector<ChangeEvent> events;
    for (const auto& table : truncate.tables) events.push_back(event_for(Op::Truncate, *table));
    return events;
}

void TransactionAssembler::commit(const pgoutput::Commit&) {
    if (!in_transaction_) throw std::runtime_error("commit outside of a transaction");
    in_transaction_ = false;
    recent_xids_.push_back(current_.xid);
    while (recent_xids_.size() > kMaxRecentXids) recent_xids_.pop_front();
}

void TransactionAssembler::forget_xids_before(Xid xmin) {
    // Commit order is not xid order, so every entry is checked.
    std::erase_if(recent_xids_, [xmin](Xid xid) { return xid_precedes(xid, xmin); });
}

void TransactionAssembler::reset() {
    in_transaction_ = false;
    next_seq_ = 0;
}

}  // namespace recordflow::core

#pragma once

#include <clickhouse/block.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/numeric.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "core/change_event.hpp"
#include "core/table_schema.hpp"
#include "sink/clickhouse/value_converter.hpp"

// Turns the buffered events of one table into a ClickHouse Block: the source
// columns through the value converter (decision 2.4), plus Recordflow's _rf_
// columns (decision 2.3) in the order of the target table.
namespace recordflow::sink::clickhouse {

class TableSink {
public:
    explicit TableSink(const core::TableSchema& source);

    // An insert, update or snapshot row: the event's `after` image, written
    // as the current state of its key.
    void add_upsert(const core::ChangeEvent& event);

    std::size_t rows() const { return rows_; }

    // Returns the block of the rows added so far and starts an empty one.
    // Logs how many unchanged TOAST values were written as NULL or default
    // (decision 5.7).
    ::clickhouse::Block take();

private:
    void add_meta(const core::ChangeEvent& event, core::Op op, bool deleted);
    void reset_meta();
    // Position of `name` in `row`, trying `hint` first (rows usually follow
    // the table's column order).
    static const core::Value* find(const core::Row& row, const std::string& name, std::size_t hint);

    std::string table_;
    std::vector<ColumnBuilder> columns_;
    std::shared_ptr<::clickhouse::ColumnUInt128> versions_;
    std::shared_ptr<::clickhouse::ColumnUInt8> deleted_;
    std::shared_ptr<::clickhouse::ColumnEnum8> ops_;
    std::shared_ptr<::clickhouse::ColumnEnum8> sources_;
    std::shared_ptr<::clickhouse::ColumnDateTime64> commit_ts_;
    std::size_t rows_ = 0;
    std::size_t unchanged_toast_ = 0;
};

}  // namespace recordflow::sink::clickhouse

#pragma once

#include <clickhouse/columns/column.h>
#include <clickhouse/columns/numeric.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "core/change_event.hpp"
#include "sink/clickhouse/type_mapping.hpp"

// Converts PostgreSQL text values (pgoutput and chunk SELECTs, both formatted
// under the fixed GUCs of decision 2.4) into typed ClickHouse columns.
namespace recordflow::sink::clickhouse {

// A text value that cannot be represented in the target type.
class ConversionError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

enum class AppendOutcome { Ok, UnchangedToast };

// Builds one ClickHouse column of a batch.
class ColumnBuilder {
public:
    ColumnBuilder(std::string name, ChType type);

    const std::string& name() const { return name_; }
    const ChType& type() const { return type_; }
    std::size_t size() const { return size_; }

    // Appends one PostgreSQL value. Throws ConversionError for text that does
    // not parse as the column's type, and SchemaChangeError for a NULL in a
    // column that is not Nullable (decision 7.1: NOT NULL was dropped).
    // An unchanged TOAST value is stored as NULL (Nullable) or the type's
    // default and reported as AppendOutcome::UnchangedToast (decision 5.7).
    AppendOutcome append(const core::Value& value);

    // NULL for Nullable columns, otherwise the type's default (zero, empty).
    void append_default();

    // Returns the finished column; the builder starts empty again.
    ::clickhouse::ColumnRef take();

private:
    void append_text(std::string_view text);
    void append_nested_default();
    void append_null();

    std::string name_;
    ChType type_;
    ::clickhouse::ColumnRef nested_;
    std::shared_ptr<::clickhouse::ColumnUInt8> nulls_;
    std::size_t size_ = 0;
};

}  // namespace recordflow::sink::clickhouse

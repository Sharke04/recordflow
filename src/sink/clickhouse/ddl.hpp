#pragma once

#include <array>
#include <string>
#include <string_view>

#include "config/config.hpp"
#include "core/table_schema.hpp"

// DDL of the ClickHouse target (decision 2.3): one ReplacingMergeTree table per
// source table, written by the stream and by snapshots, plus the
// <table>_current view that readers use.
namespace recordflow::sink::clickhouse {

struct MetaColumn {
    std::string_view name;
    std::string_view type;  // as system.columns reports it
};

// Recordflow's columns, after the source columns. _rf_ingested_at is filled
// by its DEFAULT, never by inserts.
inline constexpr std::string_view kVersionColumn = "_rf_version";
inline constexpr std::string_view kIsDeletedColumn = "_rf_is_deleted";
inline constexpr std::string_view kOpColumn = "_rf_op";
inline constexpr std::string_view kSourceColumn = "_rf_source";
inline constexpr std::string_view kCommitTsColumn = "_rf_commit_ts";
inline constexpr std::string_view kIngestedAtColumn = "_rf_ingested_at";
inline constexpr std::string_view kOpEnum = "Enum8('c' = 1, 'u' = 2, 'd' = 3, 'r' = 4)";
inline constexpr std::string_view kSourceEnum = "Enum8('log' = 1, 'snapshot' = 2)";

inline constexpr std::array<MetaColumn, 6> kMetaColumns{{
    {kVersionColumn, "UInt128"},
    {kIsDeletedColumn, "UInt8"},
    {kOpColumn, kOpEnum},
    {kSourceColumn, kSourceEnum},
    {kCommitTsColumn, "DateTime64(6, 'UTC')"},
    {kIngestedAtColumn, "DateTime64(3)"},
}};

inline constexpr std::string_view kMetaPrefix = "_rf_";
inline constexpr std::string_view kCurrentViewSuffix = "_current";

// `name` with backticks, escaping backslashes and backticks.
std::string quote_identifier(std::string_view name);

// clickhouse.table if set, otherwise "<schema>__<table>".
std::string target_table_name(const config::ClickHouseSection& section, const core::TableSchema& source);

// 'text' with backslashes and single quotes escaped.
std::string quote_string(std::string_view text);

// "`db`.`table`"
std::string qualified_name(std::string_view database, std::string_view table);

std::string create_table_sql(std::string_view database, std::string_view table, const core::TableSchema& source);
std::string create_view_sql(std::string_view database, std::string_view table);

}  // namespace recordflow::sink::clickhouse

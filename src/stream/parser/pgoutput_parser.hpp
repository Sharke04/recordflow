#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "stream/wire.hpp"

namespace pgoutput {

struct ColumnInfo {
    std::string name;
    std::uint32_t type_oid;
    std::int32_t type_mod;
    bool part_of_key;
};

struct TableInfo {
    std::uint32_t oid = 0;
    std::string schema;
    std::string name;
    char replica_identity = '\0';
    std::vector<ColumnInfo> columns;
};

struct Value {
    enum class Kind { Null, UnchangedToast, Text };

    Kind kind = Kind::Null;
    std::string text;
};

using Row = std::vector<std::pair<std::string, Value>>;

struct DecodedDml {
    std::optional<Row> before;
    std::optional<Row> after;
};

class WalMessageDecoder {
public:
    char peekType(const pgwire::ByteCursor& buf) const { return buf.typeAt(); }
    std::uint32_t peekOid(const pgwire::ByteCursor& buf) const {
        return static_cast<std::uint32_t>(buf.intAt(1));
    }

    TableInfo decodeRelation(pgwire::ByteCursor& buf) const;
    DecodedDml decodeInsert(pgwire::ByteCursor& buf, const TableInfo& rel) const;
    DecodedDml decodeUpdate(pgwire::ByteCursor& buf, const TableInfo& rel) const;
    DecodedDml decodeDelete(pgwire::ByteCursor& buf, const TableInfo& rel) const;

private:
    Row decodeTuple(pgwire::ByteCursor& buf, const TableInfo& rel) const;
};

struct ParsedMessage {
    char type = 0;
    std::uint64_t commit_end_lsn = 0;
};

}

class PgoutputParser {
public:
    pgoutput::ParsedMessage handle_message(const char* data, std::size_t len);

private:
    std::string relation_label(std::uint32_t oid) const;

    pgoutput::WalMessageDecoder decoder_;
    std::unordered_map<std::uint32_t, pgoutput::TableInfo> relations_;
};

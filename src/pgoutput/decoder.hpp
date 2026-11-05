#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/change_event.hpp"
#include "pgoutput/wire.hpp"

namespace recordflow::pgoutput {

using core::ChangeEvent;
using core::Op;
using core::Row;
using core::Value;

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

class WalMessageDecoder {
public:
    char peekType(const ByteReader& buf) const { return buf.typeAt(); }
    std::uint32_t peekOid(const ByteReader& buf) const {
        return static_cast<std::uint32_t>(buf.intAt(1));
    }

    TableInfo decodeRelation(ByteReader& buf) const;
    ChangeEvent decodeInsert(ByteReader& buf, const TableInfo& rel) const;
    ChangeEvent decodeUpdate(ByteReader& buf, const TableInfo& rel) const;
    ChangeEvent decodeDelete(ByteReader& buf, const TableInfo& rel) const;

private:
    Row decodeTuple(ByteReader& buf, const TableInfo& rel) const;
};

struct TransactionInfo {
    std::uint32_t xid = 0;
    std::uint64_t lsn = 0;
    std::int64_t commit_ts = 0;
};

struct ParsedMessage {
    char type = 0;
    std::uint64_t commit_end_lsn = 0;
    std::vector<ChangeEvent> events;
    std::optional<TableInfo> relation;
};

class PgoutputDecoder {
public:
    ParsedMessage handle_message(const char* data, std::size_t len);

private:
    std::string relation_label(std::uint32_t oid) const;

    WalMessageDecoder decoder_;
    std::unordered_map<std::uint32_t, TableInfo> relations_;
    TransactionInfo transaction_;
};

}  // namespace recordflow::pgoutput

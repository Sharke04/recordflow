#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "stream/publisher/change_event.hpp"
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

class WalMessageDecoder {
public:
    char peekType(const pgwire::ByteCursor& buf) const { return buf.typeAt(); }
    std::uint32_t peekOid(const pgwire::ByteCursor& buf) const {
        return static_cast<std::uint32_t>(buf.intAt(1));
    }

    TableInfo decodeRelation(pgwire::ByteCursor& buf) const;
    ChangeEvent decodeInsert(pgwire::ByteCursor& buf, const TableInfo& rel) const;
    ChangeEvent decodeUpdate(pgwire::ByteCursor& buf, const TableInfo& rel) const;
    ChangeEvent decodeDelete(pgwire::ByteCursor& buf, const TableInfo& rel) const;

private:
    Row decodeTuple(pgwire::ByteCursor& buf, const TableInfo& rel) const;
};

// Transaction context carried by a Begin message. pgoutput sends the xid, the
// commit timestamp and the LSN of the commit record up front, so every event
// in the transaction can be stamped as it is decoded.
struct TransactionInfo {
    std::uint32_t xid = 0;
    std::uint64_t lsn = 0;
    std::int64_t commit_ts = 0;
};

struct ParsedMessage {
    char type = 0;
    std::uint64_t commit_end_lsn = 0;
    std::vector<ChangeEvent> events;
};

}

class PgoutputParser {
public:
    pgoutput::ParsedMessage handle_message(const char* data, std::size_t len);

private:
    std::string relation_label(std::uint32_t oid) const;

    pgoutput::WalMessageDecoder decoder_;
    std::unordered_map<std::uint32_t, pgoutput::TableInfo> relations_;
    pgoutput::TransactionInfo transaction_;
};

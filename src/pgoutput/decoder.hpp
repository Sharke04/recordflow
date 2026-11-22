#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "pgoutput/messages.hpp"
#include "pgoutput/wire.hpp"

namespace recordflow::pgoutput {

// Decodes pgoutput messages and keeps the Relation messages it has seen, since
// row changes refer to their table only by OID.
class PgoutputDecoder {
public:
    // Decodes one message (the payload of an XLogData frame). Throws
    // std::runtime_error if the message is malformed.
    Message decode(const char* data, std::size_t len);

    // Forgets cached relations; a new replication session resends them.
    void reset();

private:
    std::shared_ptr<const TableInfo> relation(std::uint32_t oid) const;
    Relation decode_relation(ByteReader& buf);
    Change decode_change(ByteReader& buf, core::Op op);
    Truncate decode_truncate(ByteReader& buf) const;
    core::Row decode_tuple(ByteReader& buf, const TableInfo& table) const;

    std::unordered_map<std::uint32_t, std::shared_ptr<const TableInfo>> relations_;
};

}  // namespace recordflow::pgoutput

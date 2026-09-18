#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

class ByteCursor {
public:
    ByteCursor(const char* data, std::size_t len) : data_(data), len_(len) {}

    std::uint8_t get() { return byte(advance(1)); }
    std::int16_t getShort() {
        const std::size_t i = advance(2);
        return static_cast<std::int16_t>((byte(i) << 8) | byte(i + 1));
    }
    std::int32_t getInt() { return int32_at(advance(4)); }
    std::int64_t getLong() {
        const std::size_t i = advance(8);
        std::int64_t v = 0;
        for (int k = 0; k < 8; ++k) v = (v << 8) | byte(i + k);
        return v;
    }

    std::string getString() {
        std::string s;
        for (char c; (c = static_cast<char>(get())) != '\0';) s.push_back(c);
        return s;
    }

    std::string getBytes(std::size_t n) {
        const std::size_t i = advance(n);
        return std::string(data_ + i, n);
    }

    std::string getCountedBytes() {
        const std::int32_t n = getInt();
        if (n < 0) throw std::runtime_error("pgoutput value has negative length");
        return getBytes(static_cast<std::size_t>(n));
    }

    const char* rest() const { return data_ + pos_; }
    std::size_t remaining() const { return len_ - pos_; }

    char typeAt() const { return static_cast<char>(data_[pos_]); }
    std::int32_t intAt(std::size_t off) const {
        const std::size_t i = pos_ + off;
        check(i, 4);
        return int32_at(i);
    }

private:
    std::uint32_t byte(std::size_t i) const {
        return static_cast<unsigned char>(data_[i]);
    }
    std::int32_t int32_at(std::size_t i) const {
        return static_cast<std::int32_t>((byte(i) << 24) | (byte(i + 1) << 16) |
                                         (byte(i + 2) << 8) | byte(i + 3));
    }
    std::size_t advance(std::size_t n) {
        check(pos_, n);
        const std::size_t i = pos_;
        pos_ += n;
        return i;
    }
    void check(std::size_t at, std::size_t n) const {
        if (at > len_ || n > len_ - at) {
            throw std::runtime_error("pgoutput message truncated");
        }
    }

    const char* data_;
    std::size_t len_;
    std::size_t pos_ = 0;
};

class ByteWriter {
public:
    ByteWriter& put(std::uint8_t v) {
        buf_.push_back(static_cast<char>(v));
        return *this;
    }
    ByteWriter& putLong(std::uint64_t v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            put(static_cast<std::uint8_t>(v >> shift));
        }
        return *this;
    }

    const char* data() const { return buf_.data(); }
    std::size_t size() const { return buf_.size(); }

private:
    std::string buf_;
};

class WalMessageDecoder {
public:
    char peekType(const ByteCursor& buf) const { return buf.typeAt(); }
    std::uint32_t peekOid(const ByteCursor& buf) const {
        return static_cast<std::uint32_t>(buf.intAt(1));
    }

    TableInfo decodeRelation(ByteCursor& buf) const;
    DecodedDml decodeInsert(ByteCursor& buf, const TableInfo& rel) const;
    DecodedDml decodeUpdate(ByteCursor& buf, const TableInfo& rel) const;
    DecodedDml decodeDelete(ByteCursor& buf, const TableInfo& rel) const;

private:
    Row decodeTuple(ByteCursor& buf, const TableInfo& rel) const;
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

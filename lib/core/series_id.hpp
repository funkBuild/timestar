#pragma once

#include <fmt/format.h>

#include <array>
#include <compare>
#include <cstring>
#include <functional>
#include <map>
#include <string>

/**
 * SeriesId128 - A 128-bit identifier derived from XXH3_128bits hash of SeriesKey
 *
 * This class represents a unique identifier for time series based on the
 * XXH3_128bits hash of the series key string.  The hash produces a full
 * 128-bit digest with no truncation, providing excellent collision resistance
 * at speeds >10x faster than the previous SHA1 implementation.
 *
 * Note: Changing the hash function invalidates all previously-persisted
 * SeriesId128 values (WAL files, NativeIndex entries, TSM file series
 * indexes).  A full data re-ingestion or migration is required after this
 * change.
 */
class SeriesId128 {
private:
    std::array<uint8_t, 16> data;  // 128 bits (16 bytes)

public:
    // Default constructor - creates zero ID
    SeriesId128() { data.fill(0); }

    // Explicit constructor from SeriesKey string
    explicit SeriesId128(const std::string& seriesKey) { *this = fromSeriesKey(seriesKey); }

    // Generate SeriesId128 from SeriesKey using XXH3_128bits hash.
    // This is the single canonical way to produce a SeriesId128.
    // The series key format is: "measurement,tag1=val1,tag2=val2 field"
    // (produced by timestar::buildSeriesKey).
    static SeriesId128 fromSeriesKey(const std::string& seriesKey);

    // Comparison operators (required for index keys and std::map)
    auto operator<=>(const SeriesId128& other) const = default;
    bool operator==(const SeriesId128& other) const = default;

    // Serialization for index storage
    std::string toBytes() const { return std::string(reinterpret_cast<const char*>(data.data()), data.size()); }

    // Zero-copy append: write the 16-byte ID directly into an existing string,
    // avoiding a temporary std::string allocation.
    void appendTo(std::string& out) const { out.append(reinterpret_cast<const char*>(data.data()), data.size()); }

    static SeriesId128 fromBytes(const std::string& bytes) {
        SeriesId128 id;
        if (bytes.size() != 16) {
            throw std::runtime_error("Invalid SeriesId128 byte string length: " + std::to_string(bytes.size()));
        }
        std::memcpy(id.data.data(), bytes.data(), 16);
        return id;
    }

    // Zero-copy overload: construct directly from a raw pointer (e.g. leveldb::Slice::data())
    // without allocating an intermediate std::string.
    static SeriesId128 fromBytes(const char* ptr, size_t len) {
        SeriesId128 id;
        if (len != 16) {
            throw std::runtime_error("Invalid SeriesId128 byte length: " + std::to_string(len));
        }
        std::memcpy(id.data.data(), ptr, 16);
        return id;
    }

    // Debug/logging representation
    std::string toHex() const {
        static constexpr char hex[] = "0123456789abcdef";
        std::string result(32, '\0');
        for (size_t i = 0; i < 16; ++i) {
            result[i * 2] = hex[data[i] >> 4];
            result[i * 2 + 1] = hex[data[i] & 0x0F];
        }
        return result;
    }

    static SeriesId128 fromHex(const std::string& hexStr) {
        SeriesId128 id;
        if (hexStr.length() != 32) {
            throw std::runtime_error("Invalid hex string length for SeriesId128: " + std::to_string(hexStr.length()));
        }

        for (size_t i = 0; i < 16; ++i) {
            char hi = hexStr[i * 2];
            char lo = hexStr[i * 2 + 1];
            auto hexVal = [](char c) -> uint8_t {
                if (c >= '0' && c <= '9')
                    return c - '0';
                if (c >= 'a' && c <= 'f')
                    return 10 + (c - 'a');
                if (c >= 'A' && c <= 'F')
                    return 10 + (c - 'A');
                throw std::runtime_error(std::string("Invalid hex character: '") + c + "'");
            };
            id.data[i] = static_cast<uint8_t>((hexVal(hi) << 4) | hexVal(lo));
        }

        return id;
    }

    // Get raw data for hashing
    const std::array<uint8_t, 16>& getRawData() const { return data; }

    // Mutable access for deserialization (avoids const_cast)
    std::array<uint8_t, 16>& getRawData() { return data; }

    // Hash function for std::unordered_map
    struct Hash {
        size_t operator()(const SeriesId128& id) const {
            // Use first 8 bytes as hash value for good distribution
            size_t hash = 0;
            std::memcpy(&hash, id.data.data(), sizeof(size_t));
            return hash;
        }
    };

    // Check if this is a zero/empty ID
    bool isZero() const {
        static constexpr std::array<uint8_t, 16> zero{};
        return data == zero;
    }
};

// Hash specialization for std namespace
namespace std {
template <>
struct hash<SeriesId128> {
    size_t operator()(const SeriesId128& id) const { return SeriesId128::Hash{}(id); }
};
}  // namespace std

// fmt formatting specialization
template <>
struct fmt::formatter<SeriesId128> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const SeriesId128& id, FormatContext& ctx) const -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{}", id.toHex());
    }
};
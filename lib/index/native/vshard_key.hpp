#pragma once

#include "vshard.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace timestar::index {

// VShard-prefixed NativeIndex keys (ADR 0002 sec 4). One NativeIndex per core
// holds every VShard the core owns; each key is prefixed with the 2-byte VShard
// id in BIG-ENDIAN order so that lexicographic key order groups a VShard's
// entries contiguously. Extracting one VShard for a snapshot is then a prefix
// range-scan [begin, end), and install is a load rather than a rebuild.
//
// The existing per-type prefixes (0x01..0x1x) follow the 2-byte VShard prefix:
//   <vshard:2 BE><type><...>
inline constexpr size_t kVShardKeyPrefixBytes = 2;

// Prepend the VShard prefix to an index key.
inline std::string vshardIndexKey(VShardId vshard, std::string_view key) {
    std::string out;
    out.reserve(kVShardKeyPrefixBytes + key.size());
    out.push_back(static_cast<char>((vshard.value() >> 8) & 0xff));  // big-endian high byte
    out.push_back(static_cast<char>(vshard.value() & 0xff));
    out.append(key);
    return out;
}

// The bare 2-byte prefix for a VShard (the inclusive lower bound of its range).
inline std::string vshardKeyPrefix(VShardId vshard) {
    return vshardIndexKey(vshard, "");
}

// The half-open key range [begin, end) covering all of one VShard's entries.
// end is the next VShard's prefix; for the maximum VShard it is 0x1000 (beyond
// any valid prefix, since ids are < 4096 = 0x1000), a correct exclusive bound.
inline std::pair<std::string, std::string> vshardKeyRange(VShardId vshard) {
    std::string begin = vshardKeyPrefix(vshard);
    const uint32_t next = static_cast<uint32_t>(vshard.value()) + 1;
    std::string end;
    end.push_back(static_cast<char>((next >> 8) & 0xff));
    end.push_back(static_cast<char>(next & 0xff));
    return {std::move(begin), std::move(end)};
}

// Split a VShard-prefixed key into (vshard, remaining key). nullopt if the key is
// shorter than the prefix or carries a reserved (>= 4096) VShard id.
inline std::optional<std::pair<VShardId, std::string_view>> parseVShardIndexKey(std::string_view key) {
    if (key.size() < kVShardKeyPrefixBytes)
        return std::nullopt;
    const uint16_t vs = static_cast<uint16_t>((static_cast<uint8_t>(key[0]) << 8) | static_cast<uint8_t>(key[1]));
    if (vs >= VIRTUAL_SHARD_COUNT)
        return std::nullopt;  // reserved / out of range
    return std::make_pair(VShardId{vs}, key.substr(kVShardKeyPrefixBytes));
}

}  // namespace timestar::index

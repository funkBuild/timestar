#pragma once
#include "crc32.hpp"
#include "rollup.hpp"

#include <zstd.h>

#include <cstring>
#include <span>

namespace rollup_codec {
constexpr size_t recordBytes = 57;
inline std::vector<char> encode(std::span<const RollupState> states) {
    std::vector<char> raw(states.size() * recordBytes);
    char* out = raw.data();
    for (const auto& state : states) {
        auto put = [&](const auto& value) {
            std::memcpy(out, &value, sizeof(value));
            out += sizeof(value);
        };
        put(state.interval);
        put(state.count);
        put(state.latestTimestamp);
        put(state.sum);
        put(state.compensation);
        put(state.integerSum);
        put(state.method);
    }
    std::vector<char> compressed(ZSTD_compressBound(raw.size()));
    auto size = ZSTD_compress(compressed.data(), compressed.size(), raw.data(), raw.size(), 1);
    if (ZSTD_isError(size))
        throw std::runtime_error("Cannot compress rollup metadata");
    compressed.resize(size);
    const uint32_t checksum = CRC32::compute(raw.data(), raw.size());
    compressed.insert(compressed.begin(), 4, '\0');
    std::memcpy(compressed.data(), &checksum, 4);
    return compressed;
}
inline std::vector<RollupState> decode(const void* bytes, size_t size, size_t count) {
    if (count > SIZE_MAX / recordBytes)
        throw std::runtime_error("Rollup count overflow");
    if (size < 4)
        throw std::runtime_error("Truncated rollup metadata");
    uint32_t checksum = 0;
    std::memcpy(&checksum, bytes, 4);
    bytes = static_cast<const char*>(bytes) + 4;
    size -= 4;
    std::vector<char> raw(count * recordBytes);
    auto decoded = ZSTD_decompress(raw.data(), raw.size(), bytes, size);
    if (ZSTD_isError(decoded) || decoded != raw.size())
        throw std::runtime_error("Corrupt rollup metadata");
    if (CRC32::compute(raw.data(), raw.size()) != checksum)
        throw std::runtime_error("Rollup metadata checksum mismatch");
    std::vector<RollupState> states(count);
    const char* in = raw.data();
    for (auto& state : states) {
        auto get = [&](auto& value) {
            std::memcpy(&value, in, sizeof(value));
            in += sizeof(value);
        };
        get(state.interval);
        get(state.count);
        get(state.latestTimestamp);
        get(state.sum);
        get(state.compensation);
        get(state.integerSum);
        get(state.method);
        if (state.folded() && (state.interval == 0 || state.count == 0 || state.method > 5)) {
            throw std::runtime_error("Invalid rollup metadata");
        }
    }
    return states;
}
}  // namespace rollup_codec

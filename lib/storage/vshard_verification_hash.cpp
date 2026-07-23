#include "vshard_verification_hash.hpp"

#include "byte_codec.hpp"

#include <xxhash.h>

#include <bit>
#include <cstdio>
#include <stdexcept>

namespace timestar {

VShardVerificationHash::VShardVerificationHash() : state_(XXH3_createState()) {
    if (state_ == nullptr)
        throw std::runtime_error("VShardVerificationHash: failed to allocate xxHash state");
    XXH3_128bits_reset(state_);
}

VShardVerificationHash::~VShardVerificationHash() {
    XXH3_freeState(state_);
}

void VShardVerificationHash::addRecord(const SeriesId128& series, uint64_t timestamp, uint8_t typeTag,
                                       std::string_view valueBytes) {
    // Enforce the canonical strictly-increasing (series, timestamp) order. The
    // resolved logical view has exactly one point per key, so a repeat or a
    // regression is a caller bug -- fail closed rather than hash an ill-defined
    // stream.
    if (have_) {
        const bool ordered = series > lastSeries_ || (series == lastSeries_ && timestamp > lastTimestamp_);
        if (!ordered)
            throw std::logic_error(
                "VShardVerificationHash: points must be added in strictly increasing "
                "(series, timestamp) order");
    }
    have_ = true;
    lastSeries_ = series;
    lastTimestamp_ = timestamp;

    // Canonical record: series(16) | timestamp(8 BE) | typeTag(1) | value bits.
    std::string record;
    record.reserve(16 + 8 + 1 + valueBytes.size());
    series.appendTo(record);  // 16 raw id bytes
    codec::putU64(record, timestamp);
    codec::putU8(record, typeTag);
    record.append(valueBytes);

    XXH3_128bits_update(state_, record.data(), record.size());
}

void VShardVerificationHash::addFloat(const SeriesId128& series, uint64_t timestamp, double value) {
    // Exact stored bits: preserves -0.0 vs +0.0 and distinct NaN encodings.
    std::string bits;
    codec::putU64(bits, std::bit_cast<uint64_t>(value));
    addRecord(series, timestamp, kFloat, bits);
}

void VShardVerificationHash::addInteger(const SeriesId128& series, uint64_t timestamp, int64_t value) {
    std::string bits;
    codec::putU64(bits, static_cast<uint64_t>(value));  // two's-complement bit pattern
    addRecord(series, timestamp, kInteger, bits);
}

void VShardVerificationHash::addBoolean(const SeriesId128& series, uint64_t timestamp, bool value) {
    std::string bits;
    codec::putU8(bits, value ? 1 : 0);
    addRecord(series, timestamp, kBoolean, bits);
}

void VShardVerificationHash::addString(const SeriesId128& series, uint64_t timestamp, std::string_view value) {
    std::string bits;
    codec::putStr(bits, value);  // length-prefixed so "a","b" and "ab","" cannot collide
    addRecord(series, timestamp, kString, bits);
}

std::string VShardVerificationHash::digestHex() const {
    // XXH3_*_digest does not consume the state -- more points may be added after
    // digesting -- so no state copy is needed.
    const XXH128_hash_t h = XXH3_128bits_digest(state_);

    char buf[33];
    std::snprintf(buf, sizeof(buf), "%016llx%016llx", static_cast<unsigned long long>(h.high64),
                  static_cast<unsigned long long>(h.low64));
    return std::string(buf, 32);
}

}  // namespace timestar

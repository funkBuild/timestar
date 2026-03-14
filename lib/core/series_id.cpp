#include "series_id.hpp"
#include "index/key_encoding_simd.hpp"

#define XXH_STATIC_LINKING_ONLY  // Expose XXH3_state_s for stack allocation
#include <xxhash.h>

#include <cstring>

// Must match the key encoding constants and escape logic in key_encoding.cpp.
// The encoded series key format is:
//   SERIES_INDEX(0x01) + escape(measurement) + [',' + escape(tagKey) + '=' + escape(tagValue)]... + ' ' + escape(field)
// We feed the same bytes to XXH3 via streaming to avoid building the string.

static constexpr char SERIES_INDEX_BYTE = 0x01;

// Feed an escaped component to the XXH3 state (same logic as escapeKeyComponent).
static void feedEscaped(XXH3_state_t* state, const std::string& s) {
    namespace si = timestar::index::simd;

    // SIMD scan for first escape character
    size_t firstEscape = (s.size() >= si::kSimdThreshold)
        ? si::findFirstEscapeChar(s.data(), s.size())
        : [&]() -> size_t {
            for (size_t i = 0; i < s.size(); ++i) {
                char c = s[i];
                if (c == '\\' || c == ',' || c == '=' || c == ' ') return i;
            }
            return s.size();
        }();

    if (firstEscape == s.size()) {
        // No escaping needed — feed entire string in one shot
        XXH3_128bits_update(state, s.data(), s.size());
        return;
    }

    // Feed clean prefix in one call
    if (firstEscape > 0) {
        XXH3_128bits_update(state, s.data(), firstEscape);
    }

    // Escape path: batch consecutive non-escape chars
    const char* p = s.data() + firstEscape;
    const char* end = s.data() + s.size();
    while (p < end) {
        const char* runStart = p;
        while (p < end && *p != '\\' && *p != ',' && *p != '=' && *p != ' ') {
            ++p;
        }
        if (p > runStart) {
            XXH3_128bits_update(state, runStart, static_cast<size_t>(p - runStart));
        }
        if (p < end) {
            switch (*p) {
                case '\\': XXH3_128bits_update(state, "\\\\", 2); break;
                case ',':  XXH3_128bits_update(state, "\\,", 2); break;
                case '=':  XXH3_128bits_update(state, "\\=", 2); break;
                case ' ':  XXH3_128bits_update(state, "\\ ", 2); break;
                default: break;
            }
            ++p;
        }
    }
}

SeriesId128 SeriesId128::fromComponents(const std::string& measurement,
                                         const std::map<std::string, std::string>& tags,
                                         const std::string& field) {
    // Stack-allocated state — no heap allocation per call.
    XXH3_state_t state;
    XXH3_128bits_reset(&state);

    // Feed: SERIES_INDEX prefix byte
    XXH3_128bits_update(&state, &SERIES_INDEX_BYTE, 1);

    // Feed: escaped measurement
    feedEscaped(&state, measurement);

    // Feed: comma-separated escaped tag key=value pairs (map is sorted)
    for (const auto& [k, v] : tags) {
        char comma = ',';
        XXH3_128bits_update(&state, &comma, 1);
        feedEscaped(&state, k);
        char eq = '=';
        XXH3_128bits_update(&state, &eq, 1);
        feedEscaped(&state, v);
    }

    // Feed: space + escaped field
    char space = ' ';
    XXH3_128bits_update(&state, &space, 1);
    feedEscaped(&state, field);

    XXH128_hash_t hash = XXH3_128bits_digest(&state);

    SeriesId128 id;
    std::memcpy(id.data.data(), &hash.low64, 8);
    std::memcpy(id.data.data() + 8, &hash.high64, 8);
    return id;
}

SeriesId128 SeriesId128::fromSeriesKey(const std::string& seriesKey) {
    SeriesId128 id;

    // Empty key produces a zero SeriesId128 -- callers can test with isZero().
    // This avoids hashing an empty buffer and silently producing a non-zero ID
    // that would collide across every empty-key call site.
    if (seriesKey.empty()) [[unlikely]] {
        return id;  // already zero-initialized by default ctor
    }

    // XXH3_128bits is a non-cryptographic hash that produces a full 128-bit
    // digest -- no truncation needed (unlike the previous SHA1 approach which
    // computed 160 bits and discarded 32).  It is allocation-free, has no
    // failure mode, and runs at ~60 GB/s with AVX2.
    //
    // The function is pure (no internal state), so it is safe to call from any
    // Seastar shard without synchronisation.
    XXH128_hash_t hash = XXH3_128bits(seriesKey.data(), seriesKey.size());

    // Copy the two 64-bit halves into the 16-byte data array in native byte
    // order.  This is safe because:
    //   1. SeriesId128 is never serialised to a format shared across machines
    //      with different endianness (WAL and TSM files are local-only).
    //   2. The Hash functor in series_id.hpp reads the first sizeof(size_t)
    //      bytes via memcpy, which will see the low64 half -- this is the
    //      higher-entropy half of XXH3_128bits output.
    //   3. Comparison operators use std::array::operator< which performs a
    //      lexicographic byte comparison -- the ordering is stable on a given
    //      platform even if the byte layout differs from big-endian canonical
    //      form.
    std::memcpy(id.data.data(), &hash.low64, 8);
    std::memcpy(id.data.data() + 8, &hash.high64, 8);

    return id;
}
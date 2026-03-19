#include "series_id.hpp"

#include <xxhash.h>

#include <cstring>

SeriesId128 SeriesId128::fromSeriesKey(std::string_view seriesKey) {
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
#pragma once

#include "../core/series_id.hpp"

#include <cstdint>
#include <string>
#include <string_view>

// Forward-declare the xxHash streaming state so the header stays light.
struct XXH3_state_s;

namespace timestar {

// The whole-snapshot verification hash (parent plan, "Anti-entropy and
// corruption"). It hashes the ordered `(series, timestamp, value bits)` stream
// of the RESOLVED logical view -- tombstones and replicated retention already
// applied, LWW winner materialised -- at a named snapshot index. It is computed
// at snapshot, restore, and migration boundaries to prove two logical views are
// identical.
//
// Revisions are deliberately EXCLUDED: a value-identical re-applied batch (which
// bumps revisions but not values) must not perturb the hash. "value bits" are
// the exact stored bit pattern, so -0.0 vs +0.0 and distinct NaN encodings are
// distinguishable, and the field type participates via a type tag so a float 0.0
// never collides with an integer 0.
//
// Points MUST be added in canonical order: strictly increasing (series bytes
// lexicographically, then timestamp). The resolved view has exactly one point
// per (series, timestamp), so a repeat or an out-of-order add is a caller bug and
// fails closed. This keeps the hash streaming (no buffering of a whole VShard)
// while making it a pure function of the logical content.
class VShardVerificationHash {
public:
    VShardVerificationHash();
    ~VShardVerificationHash();
    VShardVerificationHash(const VShardVerificationHash&) = delete;
    VShardVerificationHash& operator=(const VShardVerificationHash&) = delete;

    void addFloat(const SeriesId128& series, uint64_t timestamp, double value);
    void addInteger(const SeriesId128& series, uint64_t timestamp, int64_t value);
    void addBoolean(const SeriesId128& series, uint64_t timestamp, bool value);
    void addString(const SeriesId128& series, uint64_t timestamp, std::string_view value);

    // The 128-bit digest as 32 lowercase hex chars. Non-destructive: it snapshots
    // the streaming state, so more points can be added afterwards.
    [[nodiscard]] std::string digestHex() const;

private:
    // Field-type tags (stable on-the-wire values; match TSMValueType ordering).
    enum Tag : uint8_t { kFloat = 0, kBoolean = 1, kString = 2, kInteger = 3 };

    void addRecord(const SeriesId128& series, uint64_t timestamp, uint8_t typeTag, std::string_view valueBytes);

    XXH3_state_s* state_;
    bool have_ = false;  // any point added yet
    SeriesId128 lastSeries_{};
    uint64_t lastTimestamp_ = 0;
};

}  // namespace timestar

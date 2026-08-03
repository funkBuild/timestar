#pragma once

#include <cstdint>

namespace timestar {

// The inclusive [minRev, maxRev] range of exact-v1 Raft-index revisions in a
// TSM block or snapshot extent. A default-constructed range is empty.
struct RevisionRange {
    uint64_t minRev = 0;
    uint64_t maxRev = 0;
    bool empty = true;

    // Grow the range to include one point's revision.
    constexpr void extend(uint64_t rev) noexcept {
        if (empty) {
            minRev = maxRev = rev;
            empty = false;
        } else {
            if (rev < minRev)
                minRev = rev;
            if (rev > maxRev)
                maxRev = rev;
        }
    }

    // Union with another range (empty ranges are the identity).
    constexpr void merge(const RevisionRange& other) noexcept {
        if (other.empty)
            return;
        if (empty) {
            *this = other;
            return;
        }
        if (other.minRev < minRev)
            minRev = other.minRev;
        if (other.maxRev > maxRev)
            maxRev = other.maxRev;
    }

    friend constexpr bool operator==(const RevisionRange&, const RevisionRange&) = default;
};

}  // namespace timestar

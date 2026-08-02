#pragma once

#include <cstdint>

namespace timestar {

// Point revisions and the block revision-range primitives that drive
// last-write-wins across VShard-partitioned storage (ADR 0003 sec 1/4/5).
//
// A point revision is a per-VShard monotonic u64 = the vshard_seq of the journal
// record that carried the write (Phase 1); the Raft log index later (Phase 2).
// LWW winner at a given (series, timestamp) is the HIGHEST revision. Revision 0
// is the global floor for current single-node writes that do not track revisions:
// any replicated write (revision >= 1) beats untracked data at the same point.

// Revision used when the current write path is not assigning revisions.
inline constexpr uint64_t kUntrackedRevision = 0;
// The first revision a VShard assigns to an accepted write.
inline constexpr uint64_t kFirstAssignedRevision = 1;

// LWW winner between two revisions for the same point: the higher one wins. A
// revision is assigned to at most one write of a given point, so ties cannot
// occur for distinct writes; equal revisions denote the same write and either
// answer is identical.
[[nodiscard]] inline constexpr uint64_t lwwWinnerRevision(uint64_t a, uint64_t b) noexcept {
    return a >= b ? a : b;
}

// The inclusive [minRev, maxRev] revision range stored per block (ADR 0003 sec
// 4). A default-constructed range is empty; extend() grows it as points are
// added while writing a block.
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

// Two non-empty ranges overlap iff neither lies strictly above the other. Empty
// ranges overlap nothing.
[[nodiscard]] inline constexpr bool overlaps(const RevisionRange& a, const RevisionRange& b) noexcept {
    if (a.empty || b.empty)
        return false;
    return a.minRev <= b.maxRev && b.minRev <= a.maxRev;
}

// Read-path merge decision for two blocks covering the same (series, timestamp)
// region (ADR 0003 sec 5).
enum class RangeWinner {
    A,        // range A lies strictly above B: A wins outright, no per-point read
    B,        // range B lies strictly above A: B wins outright
    Overlap,  // ranges overlap: fall back to per-point revision comparison
};

// Decide the winner between two block ranges. Disjoint ranges resolve without
// touching a per-point revision column; overlapping ranges (only possible when a
// tier-0 block with intra-file duplicates is involved) require per-point compare.
// An empty range never wins: the non-empty side wins outright, and two empty
// ranges are reported as Overlap (a degenerate no-op for callers).
[[nodiscard]] inline constexpr RangeWinner rangeWinner(const RevisionRange& a, const RevisionRange& b) noexcept {
    if (a.empty && b.empty)
        return RangeWinner::Overlap;
    if (b.empty)
        return RangeWinner::A;
    if (a.empty)
        return RangeWinner::B;
    if (a.minRev > b.maxRev)
        return RangeWinner::A;
    if (b.minRev > a.maxRev)
        return RangeWinner::B;
    return RangeWinner::Overlap;
}

}  // namespace timestar

#pragma once

#include "point_revision.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace timestar {

// One resolved point carrying its winning revision (ADR 0003).
template <class T>
struct RevisionPoint {
    uint64_t timestamp = 0;
    T value{};
    uint64_t revision = 0;

    friend bool operator==(const RevisionPoint&, const RevisionPoint&) = default;
};

// The overlap-aware read-path merge (ADR 0003 sec 5). Merges K runs of a single
// series -- each run sorted ascending by timestamp and internally free of
// duplicate timestamps (one point per timestamp, as every TSM block / memory
// store already guarantees) -- into ONE last-write-wins stream: exactly one point
// per distinct timestamp, the value whose revision is HIGHEST winning. The result
// is sorted ascending by timestamp.
//
// This is the correctness core: it always compares per-point revisions, so it is
// right regardless of block layout. The block-range fast path (rangeWinner in
// point_revision.hpp) is a *pre-filter* applied one level up -- when two blocks'
// [minRev,maxRev] ranges are disjoint the higher one wins outright and the lower
// block need not even be decoded -- but when ranges overlap (a tier-0 block with
// intra-file duplicates), the runs land here and are resolved point by point.
//
// Ties (equal revision at one timestamp) cannot occur for distinct writes: a
// revision is assigned to at most one write of a given point. If they do occur
// (e.g. a value-identical re-applied batch), the values are identical so either
// wins; this function keeps the later run's point deterministically.
template <class T>
std::vector<RevisionPoint<T>> mergeByRevision(const std::vector<std::vector<RevisionPoint<T>>>& runs) {
    // K-way merge by timestamp with a per-run cursor. At each step, find the
    // smallest timestamp across all run heads, then among every run whose head is
    // at that timestamp keep the highest-revision point (LWW), advancing all of
    // them past it.
    std::vector<size_t> cursor(runs.size(), 0);

    size_t total = 0;
    for (const auto& run : runs)
        total += run.size();
    std::vector<RevisionPoint<T>> out;
    out.reserve(total);

    while (true) {
        // Find the minimum timestamp among live run heads.
        bool any = false;
        uint64_t minTs = 0;
        for (size_t r = 0; r < runs.size(); ++r) {
            if (cursor[r] >= runs[r].size())
                continue;
            const uint64_t ts = runs[r][cursor[r]].timestamp;
            if (!any || ts < minTs) {
                minTs = ts;
                any = true;
            }
        }
        if (!any)
            break;  // all runs exhausted

        // Resolve LWW across every run whose head is at minTs.
        bool haveWinner = false;
        RevisionPoint<T> winner;
        for (size_t r = 0; r < runs.size(); ++r) {
            if (cursor[r] >= runs[r].size())
                continue;
            const RevisionPoint<T>& p = runs[r][cursor[r]];
            if (p.timestamp != minTs)
                continue;
            // Highest revision wins; on a tie the later-scanned run replaces the
            // earlier (deterministic, and values are identical on a real tie).
            if (!haveWinner || p.revision >= winner.revision) {
                winner = p;
                haveWinner = true;
            }
            ++cursor[r];  // consume this timestamp from every run that carried it
        }
        out.push_back(winner);
    }

    return out;
}

}  // namespace timestar

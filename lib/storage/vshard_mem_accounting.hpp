#pragma once

#include "../core/vshard.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace timestar {

struct VShardMemUsage {
    uint64_t bytes = 0;
    uint64_t points = 0;
    friend constexpr bool operator==(const VShardMemUsage&, const VShardMemUsage&) = default;
};

// Per-VShard memory-store accounting with a rollover trigger (Task 4b). A core
// keeps ONE physical memory store, not 4,096 -- entries are tagged by VShard and
// this tracks each VShard's live byte/point footprint plus the running total, so
// rollover can be decided two ways without per-VShard store objects:
//
//   - GLOBAL: the whole store's footprint crossed the store cap (roll everything).
//   - PER-VSHARD: one hot VShard crossed its own cap (roll just that VShard, so a
//     single busy group cannot force a store-wide flush of cold VShards).
//
// A cap of 0 disables that trigger. This is pure bookkeeping; the actual flush
// I/O lives in the memory store.
class VShardMemoryAccounting {
public:
    // perVShardRolloverBytes: cap that trips a single VShard's rollover (0 = off).
    // globalRolloverBytes: cap on the store's total footprint (0 = off).
    VShardMemoryAccounting(uint64_t perVShardRolloverBytes, uint64_t globalRolloverBytes);

    // Account for an insert of `points` points occupying `bytes` in the store.
    void add(VShardId vshard, uint64_t bytes, uint64_t points = 1);

    // Current footprint of a VShard ({0,0} if untracked).
    [[nodiscard]] VShardMemUsage usage(VShardId vshard) const;
    // Total footprint across all VShards.
    [[nodiscard]] uint64_t totalBytes() const noexcept { return totalBytes_; }
    [[nodiscard]] uint64_t totalPoints() const noexcept { return totalPoints_; }
    // Number of VShards with a live footprint.
    [[nodiscard]] size_t trackedVShards() const noexcept { return byVShard_.size(); }

    // A single VShard is over its per-VShard cap.
    [[nodiscard]] bool vshardNeedsRollover(VShardId vshard) const;
    // The store total is over the global cap.
    [[nodiscard]] bool globalNeedsRollover() const noexcept;
    // Every VShard currently over its per-VShard cap (ascending id), for targeted
    // rollover.
    [[nodiscard]] std::vector<VShardId> overCapVShards() const;

    // Clear a VShard's accounting after its data was rolled over to TSM. Decrements
    // the running totals.
    void reset(VShardId vshard);
    // Clear all accounting after a store-wide rollover.
    void resetAll();

private:
    uint64_t perVShardCap_;
    uint64_t globalCap_;
    uint64_t totalBytes_ = 0;
    uint64_t totalPoints_ = 0;
    std::unordered_map<uint16_t, VShardMemUsage> byVShard_;
};

}  // namespace timestar

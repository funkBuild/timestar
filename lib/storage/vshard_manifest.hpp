#pragma once

#include "../core/vshard.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace timestar {

// Per-VShard durability watermarks (ADR 0001 sec 4).
struct VShardWatermarks {
    // Highest vshard_seq whose effects are fully materialised into TSM/index/catalog.
    uint64_t appliedSeq = 0;
    // Highest vshard_seq no longer needed for recovery or replica catch-up. It can
    // never outrun appliedSeq (releasing un-materialised data would lose it), so
    // the manifest clamps releasedSeq to appliedSeq on read and on encode.
    uint64_t releasedSeq = 0;

    friend constexpr bool operator==(const VShardWatermarks&, const VShardWatermarks&) = default;
};

// Per-core durable manifest of per-VShard watermarks (ADR 0001 sec 4). These
// checkpoints live in the manifest -- NOT in the journal stream -- so segment GC
// never appends to the very log it is trying to reclaim. Serialisation is
// deterministic (entries ordered by VShard id) with a trailing CRC; decode()
// fails closed on any corruption.
class VShardManifest {
public:
    // Advance a VShard's applied watermark (monotonic; a regression is ignored).
    // Advancing applied does NOT retroactively raise a previously clamped
    // released -- the caller re-derives released each cycle -- so the stored state
    // is exactly what persists.
    void setApplied(VShardId vshard, uint64_t appliedSeq);
    // Advance a VShard's released watermark. The effective value is
    // max(current, releasedSeq) clamped to the CURRENT appliedSeq, so releasing
    // past what is materialised is impossible. Because the clamp happens at set
    // time (not at read/encode), the stored watermark is exactly what a later
    // decode observes -- reload is behaviourally identical to the live manifest.
    // (Advance applied before released within a cycle.)
    void setReleased(VShardId vshard, uint64_t releasedSeq);

    // Watermarks for a VShard ({0,0} if untracked).
    [[nodiscard]] VShardWatermarks get(VShardId vshard) const;

    // Number of tracked VShards.
    [[nodiscard]] size_t size() const noexcept { return byVShard_.size(); }

    // Deterministic serialisation (ordered by VShard) with a trailing CRC.
    [[nodiscard]] std::string encode() const;
    // Parse a manifest; nullopt on any corruption (bad magic/version, truncation,
    // CRC mismatch, out-of-range VShard, or duplicate/unordered entries).
    [[nodiscard]] static std::optional<VShardManifest> decode(std::string_view bytes);

private:
    // Both watermarks are stored already-effective: releasedSeq <= appliedSeq at
    // all times (clamped in setReleased), so encode/decode round-trips exactly.
    std::map<uint16_t, VShardWatermarks> byVShard_;  // ordered -> deterministic encode
};

}  // namespace timestar

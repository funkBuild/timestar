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
    void setApplied(VShardId vshard, uint64_t appliedSeq);
    // Advance a VShard's released watermark (monotonic; a regression is ignored).
    // The stored value is clamped to appliedSeq on read/encode, so releasing past
    // what is materialised is impossible even if a caller asks for it.
    void setReleased(VShardId vshard, uint64_t releasedSeq);

    // Watermarks for a VShard ({0,0} if untracked); releasedSeq is clamped to
    // appliedSeq.
    [[nodiscard]] VShardWatermarks get(VShardId vshard) const;

    // Number of tracked VShards.
    [[nodiscard]] size_t size() const noexcept { return byVShard_.size(); }

    // Deterministic serialisation (ordered by VShard) with a trailing CRC.
    [[nodiscard]] std::string encode() const;
    // Parse a manifest; nullopt on any corruption (bad magic/version, truncation,
    // CRC mismatch, out-of-range VShard, or duplicate/unordered entries).
    [[nodiscard]] static std::optional<VShardManifest> decode(std::string_view bytes);

private:
    struct Raw {
        uint64_t appliedSeq = 0;
        uint64_t releasedSeqRaw = 0;  // pre-clamp; get()/encode() clamp to appliedSeq
    };
    std::map<uint16_t, Raw> byVShard_;  // ordered -> deterministic encode
};

}  // namespace timestar

#pragma once

#include <compare>
#include <cstdint>
#include <stdexcept>

namespace timestar {

// Number of virtual shards, frozen at cluster creation (validated, not tunable
// in v1). Storage is addressed by VShard identity; the reactor core that
// executes a VShard is derived at startup from its id and the live core count.
inline constexpr uint16_t VIRTUAL_SHARD_COUNT = 4096;

// A stable virtual-shard identity in [0, VIRTUAL_SHARD_COUNT).
//
// A VShard is the unit of storage ownership and (later) replication. Unlike a
// reactor core number, it never changes when the process core count changes: a
// --smp change is a restart plus a recomputed core assignment (see assignCore),
// with no data movement and no persisted local ownership. It is a distinct type
// so a core number can never be passed where a VShard identity is expected, and
// vice versa.
class VShardId {
public:
    constexpr explicit VShardId(uint16_t value) noexcept : value_(value) {}

    [[nodiscard]] constexpr uint16_t value() const noexcept { return value_; }
    [[nodiscard]] constexpr bool valid() const noexcept { return value_ < VIRTUAL_SHARD_COUNT; }

    constexpr auto operator<=>(const VShardId&) const noexcept = default;

private:
    uint16_t value_;
};

// Derived local execution assignment: THE single definition of which reactor
// core executes a VShard.
//
// It is a deterministic pure function of the VShard identity and the live core
// count, computed exactly once at startup before storage services open --
// nothing recomputes or reassigns it mid-process. The spread is even: with C
// cores, each core owns either floor or ceil of VIRTUAL_SHARD_COUNT / C VShards
// (the first VIRTUAL_SHARD_COUNT % C cores own one extra). The result never
// encodes into any on-disk name, so a VShard directory is opened by its
// assigned core without a core number appearing in the layout.
//
// Changing this formula changes which core replays which VShard's journal after
// a core-count change; it is a startup-behaviour change requiring its own review
// (and a deliberate update of the golden vectors in vshard_test.cpp), NOT a
// storage migration.
[[nodiscard]] constexpr unsigned assignCore(VShardId vshard, unsigned coreCount) {
    if (coreCount == 0)
        throw std::invalid_argument("assignCore: core count must be at least 1");
    return static_cast<unsigned>(vshard.value() % coreCount);
}

}  // namespace timestar

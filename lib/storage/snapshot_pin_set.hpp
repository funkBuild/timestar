#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace timestar {

// One pin holding a snapshot alive against GC (Task 4d, "persistent pin files
// with lease semantics"). A learner/backup pins the snapshot at a revision under
// a LEASE: the pin is live only until `leaseDeadline`, a LOGICAL time (a cluster
// tick / Raft index / epoch supplied by the caller) -- cluster mode bans local
// wall-clock now() in retention decisions, so leases expire against logical time,
// not the wall clock. An abandoned learner's pin therefore self-releases when its
// lease lapses, and GC is never pinned forever by a dead holder.
struct SnapshotPin {
    uint64_t snapshotRevision = 0;
    uint64_t leaseDeadline = 0;  // logical; the pin is live while logicalNow < leaseDeadline

    friend bool operator==(const SnapshotPin&, const SnapshotPin&) = default;
};

// The persistent set of pins for a VShard (each holder holds at most one; a
// re-pin renews the lease). Serialisable so it survives restart. The GC floor is
// the lowest revision any LIVE pin protects; expired pins are ignored.
class SnapshotPinSet {
public:
    // Pin (or renew) a holder's pin. A holder holds at most one pin: re-pinning
    // replaces it (renewed lease / new revision).
    void pin(std::string_view holder, const SnapshotPin& pin);
    // Explicitly drop a holder's pin (returns true if one existed).
    bool release(std::string_view holder);

    // The current pin of a holder, if any.
    [[nodiscard]] std::optional<SnapshotPin> pinOf(std::string_view holder) const;
    [[nodiscard]] size_t size() const noexcept { return pins_.size(); }

    // The lowest snapshotRevision protected by a pin LIVE at `logicalNow` (i.e.
    // logicalNow < leaseDeadline). UINT64_MAX when no live pin exists -- nothing
    // is pinned, so GC may reclaim anything.
    [[nodiscard]] uint64_t gcFloor(uint64_t logicalNow) const;
    // Whether `revision` is protected by some live pin at `logicalNow`.
    [[nodiscard]] bool isPinned(uint64_t revision, uint64_t logicalNow) const;

    // Drop every pin whose lease has lapsed at `logicalNow` (returns how many were
    // removed). Optional housekeeping; gcFloor already ignores expired pins.
    size_t evictExpired(uint64_t logicalNow);

    // Deterministic serialisation (ordered by holder) with a trailing CRC.
    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<SnapshotPinSet> decode(std::string_view bytes);

private:
    std::map<std::string, SnapshotPin> pins_;  // holder -> pin, ordered for deterministic encode
};

}  // namespace timestar

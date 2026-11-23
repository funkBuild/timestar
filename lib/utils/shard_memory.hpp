#pragma once

#include <cstdint>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>

namespace timestar {

// Whether `p` was allocated from the calling shard's memory pool.
//
// Seastar gives every shard its own pool, fixed in size at start-up; nothing
// rebalances them. Memory may be handed to another shard (a write request is
// parsed on the connection's shard and moved to the shard that owns the
// series), and that is fine for as long as the request lives — but a structure
// that KEEPS such memory makes one shard's pool pay for another shard's data.
// With every connection on one shard, that shard would end up holding every
// shard's memory store. So long-lived structures adopt a buffer only when this
// returns true, and copy it otherwise.
//
// Whether this build has per-shard pools at all. Seastar's debug mode uses the
// system allocator, which has none.
inline bool hasShardMemoryPools() noexcept {
    static const bool hasPools = [] {
        try {
            (void)seastar::memory::get_memory_layout();
            return true;
        } catch (...) {
            return false;
        }
    }();
    return hasPools;
}

// Answers true where there are no per-shard pools to protect: off a reactor
// thread (plain tests, offline tools) and in builds using the system allocator.
inline bool isShardLocalMemory(const void* p) noexcept {
    if (!seastar::engine_is_ready() || !hasShardMemoryPools()) {
        return true;
    }
    // Not cached: the pool's extent grows as the shard maps more of its range.
    const auto layout = seastar::memory::get_memory_layout();
    const auto address = reinterpret_cast<uintptr_t>(p);
    return address >= layout.start && address < layout.end;
}

}  // namespace timestar

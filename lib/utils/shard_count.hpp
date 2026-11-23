#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

namespace timestar {

// Number of reactor shards in this process.
//
// Seastar deprecated the process-wide static `smp::count` in favour of
// `this_smp_shard_count()`, which reads the calling reactor thread's smp
// instance and so must not be called off a reactor thread. Some first-party
// code is also constructed where no reactor exists (plain unit tests, offline
// tools); the old static read 0 there, and this keeps that behaviour instead
// of dereferencing a null smp.
inline unsigned shardCount() noexcept {
    return seastar::engine_is_ready() ? seastar::this_smp_shard_count() : 0u;
}

}  // namespace timestar

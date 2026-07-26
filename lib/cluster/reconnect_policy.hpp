#pragma once

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/net/api.hh>  // net::tcp_keepalive_params

// Shared reconnect/retry PACING for the two inter-node transports (write-scaleout 4a/4b).
//
// Both `DataPlaneRpc` and `RaftRpcTransport` have to reconnect by hand: seastar's
// `rpc::client` connects once and latches `error()` on failure -- it never re-dials. Each
// therefore replaces a dead client, but only once per backoff window, so a burst of
// concurrent requests to a down peer does not spawn a connect attempt per request.
//
// The two numbers below are the ONE definition of that window, because they are coupled to
// a third number in a different file: the write router's retry schedule
// (`writeFailureRetryDelay`) must SPAN this window, or a transient reset is retried six
// times against the same dead connection and surfaces to the client as a 5xx -- the [D6]
// collapse. `write_errors.hpp` static_asserts that relationship against these constants.
namespace timestar::cluster {

// How long a transport waits before re-DIALING a peer whose connection died. During the
// window callers are handed the dead client and fail fast, which is deliberate: a peer
// that is genuinely down must not cost one connect() syscall per in-flight write.
inline constexpr std::chrono::milliseconds kReconnectBackoff{200};

// Jitter applied to the window, as a percentage of it, uniformly in [-pct, +pct]
// (write-scaleout 4b-i).
//
// Without jitter every reconnect in the cluster is scheduled on the same grid. A node
// restart drops N_shards x N_peers connections at the same instant, so every one of them
// re-dials at the same instant, 200 ms later, and again 200 ms after that -- a
// thundering herd against a peer that is still opening its listeners, which makes the
// early reconnects fail and re-synchronizes the next round. At `--smp 4` on a 3-node
// cluster that is 8 simultaneous dials per round; at production shard counts it is
// hundreds. +/-50% spreads them over a 200 ms band, which also means a caller arriving
// just after a failed dial has a real chance of finding the window already expired
// instead of always waiting the full 200 ms.
inline constexpr unsigned kReconnectJitterPercent = 50;

// Uniform jitter of +/- `pct` percent around `base`. Never negative, never zero-length
// for a non-zero base (a zero delay would turn a retry loop into a spin).
inline std::chrono::milliseconds jitteredDelay(std::chrono::milliseconds base, unsigned pct) {
    if (base.count() <= 0 || pct == 0)
        return base;
    // One generator per shard (seastar shards are threads), seeded from the shard id AND
    // from per-PROCESS entropy.
    //
    // The shard id alone is not enough and the omission defeated the purpose: every node
    // runs the same binary with the same shard numbering, so node1/shard2 and node2/shard2
    // drew IDENTICAL sequences and re-dialed a restarting peer in perfect lockstep. The
    // herd this jitter exists to break is a CROSS-NODE one -- N nodes x N shards all
    // reconnecting to the peer that just came back -- and only per-process entropy
    // separates the nodes.
    static thread_local std::minstd_rand rng{[] {
        const uint64_t mix = static_cast<uint64_t>(seastar::this_shard_id()) * 2654435761ull ^
                             static_cast<uint64_t>(::getpid()) * 40503ull ^
                             static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        // minstd_rand's modulus is 2^31-1 and seed 0 is degenerate; fold and offset.
        return static_cast<uint32_t>((mix ^ (mix >> 32)) % 2147483646u) + 1u;
    }()};
    const int64_t span = static_cast<int64_t>(base.count()) * static_cast<int64_t>(pct) / 100;
    if (span <= 0)
        return base;
    std::uniform_int_distribution<int64_t> dist(-span, span);
    return std::chrono::milliseconds(std::max<int64_t>(1, base.count() + dist(rng)));
}

// The wall-clock point at which a peer whose connection just died may be re-dialed.
inline seastar::lowres_clock::time_point nextReconnectAt(seastar::lowres_clock::time_point now) {
    return now + jitteredDelay(kReconnectBackoff, kReconnectJitterPercent);
}

// The LONGEST a jittered reconnect window can be. The write retry schedule has to outlast
// THIS, not the nominal window -- asserting against the nominal one would leave the
// pessimal case (a long window against short retry pauses) unproven.
inline constexpr std::chrono::milliseconds worstCaseReconnectBackoff() {
    return kReconnectBackoff * (100 + kReconnectJitterPercent) / 100;
}

// TCP keepalive for every inter-node peer connection (write-scaleout 4b-ii), shared by
// the data plane and the Raft transport so the two cannot drift apart.
//
// A peer connection is idle whenever this shard leads none of the series being written,
// which on 4096 VShards is common and bursty, and a Raft connection to a hibernated
// follower is idle for long stretches. A flow that dies while idle -- peer reboot with no
// FIN, conntrack eviction, a middlebox dropping it -- stays OPEN to us and is discovered
// only when the next write hangs on it to its attempt deadline. Keepalive makes the
// kernel notice: the socket errors, and `clientFor` re-dials on next use.
//
// Deliberately the kernel's mechanism rather than an application ping verb: a ping needs a
// timer per peer per shard, a verb in every wire version, and its own timeout policy, and
// it cannot beat the kernel to a flow the kernel has already given up on.
//
// 5s idle / 2s interval / 3 probes => a dead flow is retired in ~11s: well inside the idle
// periods this targets, and far above the write deadline so a merely-slow peer is never
// killed by it. Keepalive granularity is whole seconds.
inline constexpr std::chrono::seconds kKeepaliveIdle{5};
inline constexpr std::chrono::seconds kKeepaliveInterval{2};
inline constexpr unsigned kKeepaliveCount = 3;

inline seastar::net::tcp_keepalive_params keepaliveParams() {
    return seastar::net::tcp_keepalive_params{kKeepaliveIdle, kKeepaliveInterval, kKeepaliveCount};
}

}  // namespace timestar::cluster

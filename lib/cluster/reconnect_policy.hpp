#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>

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
    // One generator per shard (seastar shards are threads). Seeded off the shard id so two
    // shards never draw the same sequence -- which would defeat the point of jittering a
    // per-shard herd.
    static thread_local std::minstd_rand rng{static_cast<uint32_t>(seastar::this_shard_id() * 2654435761u + 1u)};
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

}  // namespace timestar::cluster

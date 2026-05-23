#pragma once

// Single-server scatter-gather primitives over seastar::sharded<Service>.
//
// Most HTTP handlers fan out to every shard, await all results, and reduce
// them into a single response. This header centralises that pattern so each
// call site reduces to one line of intent (per-shard work + merge), and so
// Phase 6 multi-server can swap a remote-RPC dispatcher in here without
// touching every handler.
//
// Two layers:
//   - scatterAll(sharded, perShard) -> future<vector<R>>      // building block
//   - scatterAndConcat(sharded, perShard) -> future<vector<T>> // common reduce
//   - scatterAndSum   (sharded, perShard, init=T{}) -> future<T>
//
// For non-uniform fan-out (e.g. routing distinct payloads to owning shards),
// keep the explicit invoke_on loop — this template covers the "same work on
// every shard" case, which is the bulk of the duplication.

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

namespace timestar::cluster {

namespace detail {
// Strip `future<T>` -> `T`, leaving non-future types unchanged.
template <class T>
using future_value_t = typename seastar::futurize<T>::value_type;
}  // namespace detail

// Run `perShard` on every shard; return per-shard results in shard order.
// `perShard` may return `R` or `future<R>` — both work via seastar::futurize.
template <class Service, class PerShard>
auto scatterAll(seastar::sharded<Service>& sharded, PerShard perShard)
    -> seastar::future<std::vector<detail::future_value_t<std::invoke_result_t<PerShard, Service&>>>> {
    using R = detail::future_value_t<std::invoke_result_t<PerShard, Service&>>;
    const unsigned shardCount = seastar::smp::count;
    std::vector<seastar::future<R>> futures;
    futures.reserve(shardCount);
    for (unsigned s = 0; s < shardCount; ++s) {
        futures.push_back(sharded.invoke_on(s, perShard));
    }
    return seastar::when_all_succeed(futures.begin(), futures.end());
}

// Scatter, then concatenate per-shard `vector<T>` into a single vector.
template <class Service, class PerShard>
auto scatterAndConcat(seastar::sharded<Service>& sharded, PerShard perShard) {
    using Vec = detail::future_value_t<std::invoke_result_t<PerShard, Service&>>;
    using T = typename Vec::value_type;
    return scatterAll(sharded, std::move(perShard))
        .then([](std::vector<Vec> shardResults) {
            std::vector<T> merged;
            size_t total = 0;
            for (auto& v : shardResults) total += v.size();
            merged.reserve(total);
            for (auto& v : shardResults) {
                merged.insert(merged.end(), std::make_move_iterator(v.begin()),
                              std::make_move_iterator(v.end()));
            }
            return merged;
        });
}

// Scatter, sum a numeric per-shard result.
template <class Service, class PerShard, class T = double>
seastar::future<T> scatterAndSum(seastar::sharded<Service>& sharded, PerShard perShard, T init = T{}) {
    auto results = co_await scatterAll(sharded, std::move(perShard));
    for (const auto& r : results) {
        init += r;
    }
    co_return init;
}

}  // namespace timestar::cluster

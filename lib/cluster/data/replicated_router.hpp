#pragma once

#include "../../core/placement_table.hpp"  // virtualShard
#include "data_plane.hpp"                  // DataPoint, QuerySpec, QueryPartial, QueryIncomplete
#include "replicated_vshard.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <stdexcept>
#include <vector>

namespace timestar::data {

// Resolves the ReplicatedVShard facade that leads `vshard` -- the local group if
// this node leads it, or a forwarder to the leader node in production. Returns
// nullptr if the VShard is unassigned or has no known leader, so the router fails
// the request (fail-closed) rather than dropping data or serving a silent partial.
using LeaderResolver = std::function<VShardLeader*(uint16_t vshard)>;

// The RF>=1 write path: route any-node writes to each series' VShard LEADER and
// replicate through Raft. This is the Phase-4 group-by-owner router with the
// dispatch swapped from a direct local apply to a consensus propose. Same
// contracts: the whole batch is rejected before any dispatch if any point's
// VShard has no leader; all dispatches are awaited (never abandoned); the first
// failure fails the batch. Not a cross-node atomic transaction -- a partial
// failure leaves committed groups committed, which is safe under global LWW and
// the "retry the whole batch" contract.
class ReplicatedWriteRouter {
public:
    explicit ReplicatedWriteRouter(LeaderResolver resolve) : resolve_(std::move(resolve)) {}

    seastar::future<> write(std::vector<DataPoint> points) {
        // Group by VShard, resolving every leader BEFORE dispatch so an
        // unassigned VShard rejects the whole batch with nothing applied.
        std::map<uint16_t, std::vector<DataPoint>> byVShard;
        for (const auto& p : points)
            byVShard[timestar::virtualShard(p.series)].push_back(p);

        std::vector<VShardLeader*> leaders;
        std::vector<std::vector<DataPoint>> groups;
        for (auto& [vs, grp] : byVShard) {
            VShardLeader* leader = resolve_(vs);
            if (!leader)
                throw std::runtime_error("replicated write: VShard has no known leader");
            leaders.push_back(leader);
            groups.push_back(std::move(grp));
        }

        // Dispatch all, then await all. A false result means the resolved facade
        // is not actually the leader (stale routing) -- surface it so the caller
        // retries, never silently drop.
        std::vector<seastar::future<bool>> pending;
        pending.reserve(leaders.size());
        for (size_t i = 0; i < leaders.size(); ++i)
            pending.push_back(leaders[i]->write(DataCommand{WritePoints{std::move(groups[i]), 0}}));

        std::exception_ptr firstErr;
        bool anyNotLeader = false;
        for (auto& f : pending) {
            try {
                if (!co_await std::move(f))
                    anyNotLeader = true;
            } catch (...) {
                if (!firstErr)
                    firstErr = std::current_exception();
            }
        }
        if (firstErr)
            std::rethrow_exception(firstErr);
        if (anyNotLeader)
            throw std::runtime_error("replicated write: stale leader routing, retry the batch");
    }

private:
    LeaderResolver resolve_;
};

// The RF>=1 read path: a linearizable leader read at every VShard the query needs.
// Mirrors the Phase-4 QueryCoordinator (completeness-first, fan-out, merge) but
// each VShard is read at its leader behind a ReadIndex barrier. Unfiltered queries
// need every VShard in `allVShards`; a series filter needs only those series'
// VShards. A missing leader fails the query (QueryIncomplete), never a silent
// partial.
class ReplicatedQueryCoordinator {
public:
    ReplicatedQueryCoordinator(LeaderResolver resolve, std::vector<uint16_t> allVShards)
        : resolve_(std::move(resolve)), allVShards_(std::move(allVShards)) {}

    seastar::future<QueryPartial> query(QuerySpec spec) {
        std::set<uint16_t> needed;
        if (spec.series.empty())
            needed.insert(allVShards_.begin(), allVShards_.end());
        else
            for (const auto& s : spec.series)
                needed.insert(timestar::virtualShard(s));

        // Resolve every leader first: a needed VShard with no leader fails the
        // whole query rather than yielding a silent partial merge.
        std::vector<VShardLeader*> leaders;
        for (uint16_t vs : needed) {
            VShardLeader* leader = resolve_(vs);
            if (!leader)
                throw QueryIncomplete("query needs a VShard with no known leader");
            leaders.push_back(leader);
        }

        std::vector<seastar::future<QueryPartial>> pending;
        pending.reserve(leaders.size());
        for (auto* leader : leaders)
            pending.push_back(leader->linearizableRead(spec));

        QueryPartial merged;
        std::exception_ptr firstErr;
        for (auto& f : pending) {
            try {
                QueryPartial part = co_await std::move(f);
                merged.raw.insert(merged.raw.end(), std::make_move_iterator(part.raw.begin()),
                                  std::make_move_iterator(part.raw.end()));
                for (auto& [series, st] : part.perSeries)
                    merged.perSeries[series].merge(st);
            } catch (...) {
                if (!firstErr)
                    firstErr = std::current_exception();
            }
        }
        if (firstErr)
            std::rethrow_exception(firstErr);

        std::sort(merged.raw.begin(), merged.raw.end(), [](const DataPoint& a, const DataPoint& b) {
            if (a.timestamp != b.timestamp)
                return a.timestamp < b.timestamp;
            if (!(a.series == b.series))
                return a.series < b.series;
            return a.value < b.value;
        });
        co_return merged;
    }

private:
    LeaderResolver resolve_;
    std::vector<uint16_t> allVShards_;
};

}  // namespace timestar::data

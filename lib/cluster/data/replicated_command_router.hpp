#pragma once

#include "node_store.hpp"
#include "replicated_write_router.hpp"
#include "vshard_directory.hpp"

#include <seastar/core/future.hh>

namespace timestar::data {

// Leader-aware routing for one already-VShard-scoped mutation. This deliberately
// shares the write router's failure taxonomy, attempt bounds, and election window:
// a delete proposal has the same quorum/leadership ambiguity as a write proposal,
// but must preserve its exact ReplicatedCommand bytes across retries.
class ReplicatedCommandRouter {
public:
    ReplicatedCommandRouter(const VShardDirectory& dir, ProposeSink& local, NodeTransport& client,
                            const LeaderResolver& leaders)
        : dir_(dir), local_(local), client_(client), leaders_(leaders) {}

    seastar::future<> propose(uint16_t vshard, ReplicatedCommand command);

private:
    const VShardDirectory& dir_;
    ProposeSink& local_;
    NodeTransport& client_;
    const LeaderResolver& leaders_;
};

}  // namespace timestar::data

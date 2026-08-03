#include "dataplane_rpc.hpp"

#include "../../utils/logger.hpp"
#include "../raft/raft_group.hpp"  // kMaxProposalBytes -- the bound a slice must fit (D-31)
#include "../reconnect_policy.hpp"
#include "dataplane_limits.hpp"
#include "node_metadata.hpp"
#include "node_query.hpp"
#include "pattern_series.hpp"
#include "replicated_command.hpp"  // firstUnproposableSlice
#include "write_record.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/rpc/rpc.hh>
#include <set>
#include <stdexcept>

namespace timestar::data {

namespace {

struct DpSerializer {};

template <typename Output>
void write(DpSerializer, Output& out, const seastar::sstring& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(v.data(), v.size());
}
template <typename Input>
seastar::sstring read(DpSerializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    seastar::sstring s = seastar::uninitialized_string(n);
    in.read(s.data(), n);
    return s;
}

constexpr uint64_t kForwardWriteBatch = 3;  // sstring -> sstring (enriched WriteBatch, waited: applied)
constexpr uint64_t kQueryNode = 4;          // sstring -> sstring (enriched NodeQueryRequest, waited: partial)
constexpr uint64_t kQueryMetadata = 5;      // sstring -> sstring (MetadataRequest, waited: MetadataResult)
constexpr uint64_t kLeaderReadIndex = 7;    // sstring(u16 vshard) -> sstring(u64 readIndex); throws if not leader
constexpr uint64_t kLeaderCommitIndex = 8;  // sstring(u16 vshard) -> sstring(u64 commitIndex); throws if not leader
constexpr uint64_t kRequireV1 = 9;          // sstring(u32 version) -> sstring(u32 version); exact v1 only
// The propose reply carries the committed set plus a leader hint per rejected VShard:
//     '1'  -> every slice committed
//     '0' u16 committedCount {u16 vshard}* u16 rejectCount {u16 vshard u64 leader u8 kind}*
constexpr uint64_t kProposeWriteHinted = 10;
// One already-VShard-scoped ReplicatedCommand (currently the production delete
// path). The request is u16 VShard + the checksummed ReplicatedCommand frame; the
// response reuses the committed-set/hint shape above.
constexpr uint64_t kProposeCommandHinted = 11;
// Quorum-fenced, VShard-restricted catalog expansion for pattern deletes.
constexpr uint64_t kFindPatternSeries = 12;
// Lookup/freeze a group-0 pattern-delete plan through its current leader.
constexpr uint64_t kFrozenDeletePlan = 13;
constexpr uint64_t kControlJoin = 14;
constexpr uint64_t kEnsureMoveDestination = 15;
constexpr uint64_t kActuateMove = 16;

// How long before re-attempting a peer connection that died. seastar's rpc::client
// never reconnects itself, so we replace it -- but bounded, so a burst of concurrent
// requests to a down peer does not spawn a connect attempt each. The window (and its
// jitter) is now shared with the Raft transport and, crucially, with the WRITE RETRY
// SCHEDULE that has to outlast it -- see lib/cluster/reconnect_policy.hpp and the
// static_assert in write_errors.hpp (write-scaleout 4a/4b).
using cluster::kReconnectBackoff;

// Peer client options. Keepalive parameters are shared with the Raft transport
// (lib/cluster/reconnect_policy.hpp) so the two cannot drift apart.
seastar::rpc::client_options peerClientOptions() {
    seastar::rpc::client_options opts;
    opts.keepalive = cluster::keepaliveParams();
    return opts;
}

// The frame bounds moved to data/dataplane_limits.hpp in D-31, so the Raft proposal bound
// can be asserted against `kMaxOutboundFrameBytes` from a header that sees both. The
// rationale for both numbers is there.

seastar::rpc::resource_limits inboundLimits() {
    seastar::rpc::resource_limits lim;
    lim.max_memory = kMaxInboundRpcMemory;
    lim.bloat_factor = kInboundBloatFactor;
    return lim;
}

seastar::sstring encU32(uint32_t v) {
    char b[4];
    for (int i = 0; i < 4; ++i)
        b[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    return seastar::sstring(b, 4);
}
std::optional<uint32_t> decU32At(const seastar::sstring& s, size_t off) {
    if (s.size() < off + 4)
        return std::nullopt;
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i)
        v |= static_cast<uint32_t>(static_cast<uint8_t>(s[off + i])) << (8 * i);
    return v;
}

seastar::sstring encU16(uint16_t v) {
    char b[2] = {static_cast<char>(v & 0xff), static_cast<char>((v >> 8) & 0xff)};
    return seastar::sstring(b, 2);
}
std::optional<uint16_t> decU16(const seastar::sstring& s) {
    if (s.size() != 2)
        return std::nullopt;
    return static_cast<uint16_t>(static_cast<uint8_t>(s[0]) | (static_cast<uint16_t>(static_cast<uint8_t>(s[1])) << 8));
}
seastar::sstring encU64(uint64_t v) {
    char b[8];
    for (int i = 0; i < 8; ++i)
        b[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    return seastar::sstring(b, 8);
}
std::optional<uint64_t> decU64(const seastar::sstring& s) {
    if (s.size() != 8)
        return std::nullopt;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(static_cast<uint8_t>(s[i])) << (8 * i);
    return v;
}

// The v1 propose reply keeps the all-committed case to one byte. Otherwise:
//     '0' u16 committedCount {u16 vshard}* u16 rejectCount {u16 vshard u64 leader u8 kind}*
// The COMMITTED list is the authoritative part (see ProposeOutcome); the rejects are
// advisory hints. Carrying the committed set explicitly is what lets the caller treat
// everything it is not told about as uncommitted, instead of inferring it from a reject
// list a sink is under no obligation to make complete.
seastar::sstring encodeProposeOutcome(const ProposeOutcome& out) {
    if (out.committed)
        return seastar::sstring("1", 1);
    std::string s;
    s.reserve(1 + 2 + out.committedVShards.size() * 2 + 2 + out.rejects.size() * 11);
    s.push_back('0');
    auto appendU16 = [&s](uint16_t v) {
        const seastar::sstring b = encU16(v);
        s.append(b.data(), b.size());
    };
    appendU16(static_cast<uint16_t>(out.committedVShards.size()));
    for (uint16_t vs : out.committedVShards)
        appendU16(vs);
    appendU16(static_cast<uint16_t>(out.rejects.size()));
    for (const auto& r : out.rejects) {
        appendU16(r.vshard);
        const seastar::sstring ld = encU64(r.leaderHint);
        s.append(ld.data(), ld.size());
        s.push_back(static_cast<char>(static_cast<uint8_t>(r.kind)));
    }
    return seastar::sstring(s.data(), s.size());
}

// Parse it back. nullopt on ANY malformed shape -- a garbled reply must never read as a
// plausible set of committed slices (which would ack a write that was never replicated)
// or of leader hints (which would send the retry to a node chosen by corruption).
std::optional<ProposeOutcome> decodeProposeOutcome(const seastar::sstring& s) {
    if (s.empty())
        return std::nullopt;
    ProposeOutcome out;
    if (s[0] == '1')
        return s.size() == 1 ? std::optional<ProposeOutcome>(ProposeOutcome{true, {}, {}}) : std::nullopt;
    if (s[0] != '0' || s.size() < 3)
        return std::nullopt;
    auto u16At = [&s](size_t off) {
        return static_cast<uint16_t>(static_cast<uint8_t>(s[off]) |
                                     (static_cast<uint16_t>(static_cast<uint8_t>(s[off + 1])) << 8));
    };
    const size_t nc = u16At(1);
    if (s.size() < 3 + nc * 2 + 2)
        return std::nullopt;
    out.committedVShards.reserve(nc);
    for (size_t i = 0; i < nc; ++i)
        out.committedVShards.push_back(u16At(3 + i * 2));
    const size_t rejOff = 3 + nc * 2;
    const size_t nr = u16At(rejOff);
    if (s.size() != rejOff + 2 + nr * 11)
        return std::nullopt;
    out.rejects.reserve(nr);
    for (size_t i = 0; i < nr; ++i) {
        const size_t off = rejOff + 2 + i * 11;
        SliceReject r;
        r.vshard = u16At(off);
        uint64_t leader = 0;
        for (int b = 0; b < 8; ++b)
            leader |= static_cast<uint64_t>(static_cast<uint8_t>(s[off + 2 + b])) << (8 * b);
        r.leaderHint = leader;
        const uint8_t kind = static_cast<uint8_t>(s[off + 10]);
        if (kind > static_cast<uint8_t>(WriteFailure::Expired))
            return std::nullopt;
        r.kind = static_cast<WriteFailure>(kind);
        out.rejects.push_back(r);
    }
    return out;
}

}  // namespace

struct DataPlaneRpc::Impl {
    using Client = seastar::rpc::protocol<DpSerializer>::client;
    struct PeerEndpoint {
        seastar::socket_address address;
        std::string tlsServerName;
    };
    seastar::rpc::protocol<DpSerializer> proto{DpSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<DpSerializer>::server> server;
    std::map<NodeId, PeerEndpoint> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    NodeStore* nodeSink = nullptr;
    ProposeSink* proposeSink = nullptr;                    // RF=3 Raft propose target (M3)
    ReadIndexSink* readIndexSink = nullptr;                // replica-read leader-reach target (M4)
    FrozenDeletePlanSink* frozenDeletePlanSink = nullptr;  // group-0 request target
    ControlJoinSink* controlJoinSink = nullptr;            // group-0 observer admission target
    MoveDestinationSink* moveDestinationSink = nullptr;    // Group-0-authorized data-group creation
    MoveActuationSink* moveActuationSink = nullptr;        // one fenced data-group movement step
    // Connections that completed the exact-v1 handshake. Dropped when a
    // connection is retired because the peer may return on another binary.
    std::set<NodeId> v1Connections;
    // Mutual TLS (X1b): null unless setTlsCredentials was called. serverCreds requires a
    // client cert; clientCreds presents ours + trusts the CA. Each PeerEndpoint
    // carries the distinct DNS/IP SAN expected for that target.
    seastar::shared_ptr<seastar::tls::server_credentials> serverCreds;
    seastar::shared_ptr<seastar::tls::certificate_credentials> clientCreds;
    bool tlsEnabled = false;
    bool stopping = false;
    // Earliest time a peer whose connection died may be re-attempted (see clientFor).
    std::map<NodeId, seastar::lowres_clock::time_point> nextRetry;
    // Keeps background stops of retired (dead) connections alive across shutdown.
    seastar::gate retireGate;
    // Client stubs are created ONCE (a stub allocated per concurrent call can
    // corrupt reply routing / message-id bookkeeping). Reused for every call.
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> forwardBatchStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryNodeStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryMetadataStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> findPatternSeriesStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeWriteHintedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeCommandHintedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> frozenDeletePlanStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> controlJoinStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> ensureMoveDestinationStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> actuateMoveStub;
    // DEADLINE-CARRYING variants of the three verbs the write path awaits
    // (write-scaleout 3f). seastar's rpc client stub has a time_point overload; without
    // it an awaited call has NO timeout at all, so a peer that accepts the connection and
    // then black-holes it (a half-dead TCP path, a wedged reactor) blocks the caller
    // indefinitely -- the router's deadline is only checked BETWEEN attempts, so a single
    // attempt could hold its WriteAdmission charge for minutes and take the whole shard
    // to 503 behind it. That amplifies exactly the [D6] window Phase 4 is meant to close.
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        proposeWriteHintedTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        proposeCommandHintedTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        requireV1TimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        queryNodeTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        findPatternSeriesTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        frozenDeletePlanTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        controlJoinTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        ensureMoveDestinationTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        actuateMoveTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        leaderReadIndexTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        leaderCommitIndexTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> requireV1Stub;

    // Retire a dead connection without blocking the caller: stop it in the background
    // under a gate, keeping it alive until stop resolves.
    void retire(std::unique_ptr<Client> dead) {
        if (!dead || retireGate.is_closed())
            return;
        auto* raw = dead.get();
        (void)seastar::with_gate(retireGate, [raw, d = std::move(dead)]() mutable {
            return raw->stop().handle_exception([](std::exception_ptr) {}).finally([d = std::move(d)] {});
        });
    }

    // Open (and cache) the one connection to a peer, RECONNECTING a dead one.
    //
    // seastar's rpc::client connects once and latches error() on failure -- it never
    // reconnects. Without this, a peer that goes away (restart, transient network
    // failure) leaves a permanently dead cached client, so THIS node can never reach
    // it again: forwarded writes to VShards it owns fail forever and every scatter-
    // gather query fails, even though the peer is healthy. (Live-verified: killing and
    // restarting one node broke inserts AND queries on its peers indefinitely.)
    //
    // A dead client is retired and replaced. Between attempts we hand back the dead
    // client rather than a fresh one, so a burst of concurrent requests fails fast on
    // the existing connection instead of spawning a connect per request; the retry
    // cadence is bounded by kReconnectBackoff.
    seastar::rpc::protocol<DpSerializer>::client* clientFor(NodeId to) {
        auto it = clients.find(to);
        if (it != clients.end()) {
            if (!it->second->error())
                return it->second.get();
            const auto now = seastar::lowres_clock::now();
            auto rit = nextRetry.find(to);
            if (rit != nextRetry.end() && now < rit->second)
                return it->second.get();  // backing off: fail fast on the dead connection
            // Jittered (4b-i): without it every shard's client to a restarting peer
            // re-dials on the same 200 ms grid, so the whole node hammers the peer in
            // lockstep and re-synchronizes on each failure.
            nextRetry[to] = cluster::nextReconnectAt(now);
            retire(std::move(it->second));
            clients.erase(it);
            // A reconnect may reach a restarted peer running a different binary.
            v1Connections.erase(to);
        }
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        std::unique_ptr<seastar::rpc::protocol<DpSerializer>::client> c;
        const seastar::rpc::client_options copts = peerClientOptions();
        if (tlsEnabled) {
            // Present our cert and verify this exact configured endpoint's SAN.
            seastar::tls::tls_options topts;
            topts.server_name = seastar::sstring(pit->second.tlsServerName);
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(
                proto, copts, seastar::tls::socket(clientCreds, topts), pit->second.address);
        } else {
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(proto, copts, pit->second.address);
        }
        auto* p = c.get();
        clients[to] = std::move(c);
        return p;
    }

    // Create every client stub once.
    void makeStubs() {
        forwardBatchStub = proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWriteBatch);
        queryNodeStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryNode);
        queryMetadataStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryMetadata);
        findPatternSeriesStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFindPatternSeries);
        proposeWriteHintedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWriteHinted);
        proposeCommandHintedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeCommandHinted);
        frozenDeletePlanStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFrozenDeletePlan);
        controlJoinStub = proto.make_client<seastar::sstring(seastar::sstring)>(kControlJoin);
        ensureMoveDestinationStub = proto.make_client<seastar::sstring(seastar::sstring)>(kEnsureMoveDestination);
        actuateMoveStub = proto.make_client<seastar::sstring(seastar::sstring)>(kActuateMove);
        proposeWriteHintedTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWriteHinted);
        proposeCommandHintedTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeCommandHinted);
        requireV1TimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kRequireV1);
        queryNodeTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryNode);
        findPatternSeriesTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFindPatternSeries);
        frozenDeletePlanTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFrozenDeletePlan);
        controlJoinTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kControlJoin);
        ensureMoveDestinationTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kEnsureMoveDestination);
        actuateMoveTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kActuateMove);
        leaderReadIndexTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kLeaderReadIndex);
        leaderCommitIndexTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kLeaderCommitIndex);
        requireV1Stub = proto.make_client<seastar::sstring(seastar::sstring)>(kRequireV1);
    }

    // Where inbound connections are accepted.
    //
    // `perShard == false` (one instance, on one shard) PINS the listener to this shard.
    // seastar's default policy scatters incoming connections across every shard, but a
    // connection accepted on a shard with no server behind it leaves the peer's WAITED
    // call hanging forever (nondeterministically, since the scatter is by shard load).
    // (Raft's transport dodged this only because it is no_wait -- it never blocks on a
    // reply -- so a dropped connection was silently retried instead of hanging.)
    //
    // `perShard == true` means EVERY shard has started an instance on this same
    // address, so any shard can answer and pinning is exactly wrong: it would keep the
    // node's entire inbound data plane on one core. Note this is NOT SO_REUSEPORT --
    // this seastar hardcodes `posix_reuseport_available() { return false; }` ("FIXME:
    // reuseport currently leads to heavy load imbalance"), so the per-shard listen()
    // calls do NOT each get their own socket. Shard 0 owns the one real socket and
    // posix_server_socket_impl::accept() hands each accepted fd to the shard the load
    // balancer picks (posix_ap_server_socket_impl::move_connected_socket), where the
    // per-shard server picks it up. connection_distribution therefore distributes and
    // set_fixed_cpu does not -- which is why shard 0 stayed the profile outlier while
    // every "per-shard SO_REUSEPORT listener" in this tree quietly served nothing.
    void listenServer(seastar::socket_address local, bool perShard) {
        seastar::listen_options lo;
        lo.reuse_address = true;
        if (perShard)
            lo.lba = seastar::server_socket::load_balancing_algorithm::connection_distribution;
        else
            lo.set_fixed_cpu(seastar::this_shard_id());
        seastar::server_socket ss =
            tlsEnabled ? seastar::tls::listen(serverCreds, local, lo) : seastar::listen(local, lo);
        // Bounded inbound admission -- see inboundLimits(). Unbounded here means an
        // unauthenticated peer (mTLS is optional) can spend this node's memory for it.
        server = std::make_unique<seastar::rpc::protocol<DpSerializer>::server>(proto, std::move(ss), inboundLimits());
    }
};

DataPlaneRpc::DataPlaneRpc() : impl_(std::make_unique<Impl>()) {}
DataPlaneRpc::~DataPlaneRpc() = default;

void DataPlaneRpc::addPeer(NodeId id, seastar::socket_address addr, std::string tlsServerName) {
    if (impl_->tlsEnabled && tlsServerName.empty())
        throw std::invalid_argument("DataPlaneRpc: TLS peer server name must not be empty");
    auto existing = impl_->peers.find(id);
    if (existing != impl_->peers.end() && existing->second.address == addr &&
        existing->second.tlsServerName == tlsServerName)
        return;
    if (auto client = impl_->clients.find(id); client != impl_->clients.end()) {
        impl_->retire(std::move(client->second));
        impl_->clients.erase(client);
    }
    impl_->nextRetry.erase(id);
    impl_->v1Connections.erase(id);
    impl_->peers.insert_or_assign(id, Impl::PeerEndpoint{addr, std::move(tlsServerName)});
}

seastar::future<> DataPlaneRpc::start(seastar::socket_address local, NodeStore& sink, bool perShardListener) {
    if (impl_->server)
        throw std::logic_error("DataPlaneRpc::start called more than once");
    impl_->nodeSink = &sink;
    // Enriched forwarded writes: decode the lossless WriteBatch and apply into the
    // node sink, acking only after a durable apply (waited). A malformed frame or
    // an apply failure surfaces as an exception the caller's awaited RPC observes,
    // so a forwarded write never looks accepted when it was not.
    impl_->proto.register_handler(kForwardWriteBatch, [this](seastar::sstring data) {
        auto batch = decodeWriteBatch(std::string(data.data(), data.size()));
        if (!batch)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed write batch"));
        return impl_->nodeSink->applyWrites(std::move(*batch)).then([] { return seastar::sstring("k"); });
    });
    // TEMPORARY: TIMESTAR_QUERY_PROFILE=1 reports encode/decode cost and wire size of
    // query partials. ~375ms of a 476ms grouped cluster query is unaccounted for by
    // the leader-side and merge profilers, leaving exactly this hop.
    impl_->proto.register_handler(kQueryNode, [this](seastar::sstring data) {
        auto req = decodeNodeQueryRequest(std::string(data.data(), data.size()));
        if (!req)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed node query request"));
        return impl_->nodeSink->queryLocal(std::move(*req)).then([](NodeQueryPartial part) {
            const bool prof = [] {
                static const bool on = [] {
                    const char* e = std::getenv("TIMESTAR_QUERY_PROFILE");
                    return e && e[0] == '1';
                }();
                return on;
            }();
            const auto t0 = std::chrono::high_resolution_clock::now();
            std::string enc = encodeNodeQueryPartial(part);
            if (prof) {
                size_t pts = 0, nStates = 0, nRaw = 0, nBucket = 0;
                for (const auto& pr : part.partials) {
                    pts += pr.sortedTimestamps.size() + pr.bucketStates.size();
                    nStates += pr.sortedStates.size();
                    nRaw += pr.sortedValues.size();
                    nBucket += pr.bucketStates.size();
                }
                timestar::http_log.info(
                    "[WIRE_PROFILE] encode partial: {} partials {} pts (states={} rawvals={} buckets={}, "
                    "sizeof(AggregationState)={}) -> {} bytes ({:.1f} B/pt) in {:.1f}ms",
                    part.partials.size(), pts, nStates, nRaw, nBucket, sizeof(timestar::AggregationState), enc.size(),
                    pts ? static_cast<double>(enc.size()) / static_cast<double>(pts) : 0.0,
                    std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - t0).count());
            }
            return seastar::sstring(enc.data(), enc.size());
        });
    });
    impl_->proto.register_handler(kQueryMetadata, [this](seastar::sstring data) {
        auto req = decodeMetadataRequest(std::string(data.data(), data.size()));
        if (!req)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed metadata request"));
        return impl_->nodeSink->queryMetadata(std::move(*req)).then([](MetadataResult res) {
            std::string enc = encodeMetadataResult(res);
            return seastar::sstring(enc.data(), enc.size());
        });
    });
    impl_->proto.register_handler(kFindPatternSeries, [this](seastar::sstring data) {
        auto req = decodePatternSeriesRequest(std::string(data.data(), data.size()));
        if (!req)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed pattern-series request"));
        return impl_->nodeSink->findPatternSeries(std::move(*req)).then([](PatternSeriesResult result) {
            std::string encoded = encodePatternSeriesResult(result);
            return seastar::sstring(encoded.data(), encoded.size());
        });
    });
    impl_->proto.register_handler(kFrozenDeletePlan, [this](seastar::sstring data) {
        if (!impl_->frozenDeletePlanSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no frozen delete-plan sink"));
        auto request = control::decodeFrozenDeletePlanRpcRequest(std::string(data.data(), data.size()));
        if (!request)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed frozen delete-plan request"));
        return impl_->frozenDeletePlanSink->handleFrozenDeletePlan(std::move(*request))
            .then([](control::FreezeDeletePlanResult result) {
                std::string encoded = control::encodeFrozenDeletePlanRpcResult(result);
                return seastar::sstring(encoded.data(), encoded.size());
            });
    });
    impl_->proto.register_handler(kControlJoin, [this](seastar::sstring data) {
        if (!impl_->controlJoinSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no control join sink"));
        // Refuse an oversized untrusted frame before copying it out of the RPC
        // buffer. The decoder repeats the bound for non-RPC callers.
        if (data.size() > control::kMaxControlJoinFrameBytes)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: oversized control join request"));
        auto request = control::decodeControlJoinRequest(std::string(data.data(), data.size()));
        if (!request)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed control join request"));
        return impl_->controlJoinSink->handleControlJoin(std::move(*request))
            .then([](control::ControlJoinResult result) {
                std::string encoded = control::encodeControlJoinResult(result);
                return seastar::sstring(encoded.data(), encoded.size());
            });
    });
    impl_->proto.register_handler(kEnsureMoveDestination, [this](seastar::sstring data) {
        if (!impl_->moveDestinationSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no move-destination sink"));
        if (data.size() > control::kMaxEnsureMoveDestinationFrameBytes)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: oversized ensure-move-destination request"));
        auto request = control::decodeEnsureMoveDestinationRequest(std::string(data.data(), data.size()));
        if (!request)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed ensure-move-destination request"));
        return impl_->moveDestinationSink->handleEnsureMoveDestination(std::move(*request))
            .then([](control::EnsureMoveDestinationResult result) {
                std::string encoded = control::encodeEnsureMoveDestinationResult(result);
                return seastar::sstring(encoded.data(), encoded.size());
            });
    });
    impl_->proto.register_handler(kActuateMove, [this](seastar::sstring data) {
        if (!impl_->moveActuationSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no move-actuation sink"));
        if (data.size() > control::kMaxEnsureMoveDestinationFrameBytes)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: oversized actuate-move request"));
        auto request = control::decodeEnsureMoveDestinationRequest(std::string(data.data(), data.size()));
        if (!request)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed actuate-move request"));
        return impl_->moveActuationSink->handleActuateMove(std::move(*request))
            .then([](control::ActuateMoveResult result) {
                std::string encoded = control::encodeActuateMoveResult(result);
                return seastar::sstring(encoded.data(), encoded.size());
            });
    });
    impl_->proto.register_handler(kProposeWriteHinted, [this](seastar::sstring data) {
        if (!impl_->proposeSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no propose sink (node not RF>1)"));
        auto batch = decodeWriteBatch(std::string(data.data(), data.size()));
        if (!batch)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed propose batch"));
        return impl_->proposeSink->proposeBatchHinted(std::move(*batch)).then([](ProposeOutcome out) {
            return encodeProposeOutcome(out);
        });
    });
    impl_->proto.register_handler(kProposeCommandHinted, [this](seastar::sstring data) {
        if (!impl_->proposeSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no propose sink (node not RF>1)"));
        if (data.size() < 3)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed proposed command"));
        const uint16_t vshard = static_cast<uint16_t>(static_cast<uint8_t>(data[0]) |
                                                      (static_cast<uint16_t>(static_cast<uint8_t>(data[1])) << 8));
        auto command = decodeReplicatedCommand(std::string(data.data() + 2, data.size() - 2));
        if (!command)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed proposed command"));

        // A peer must not use the explicit VShard prefix to steer a series
        // command into a different Raft group. RetentionCutoffCmd is VShard-wide
        // by construction; writes and deletes carry enough identity to verify.
        bool matches = true;
        if (const auto* batch = std::get_if<DeleteRangeBatch>(&*command)) {
            matches = !batch->targets.empty();
            for (const auto& target : batch->targets)
                matches = matches && timestar::virtualShard(SeriesId128::fromSeriesKey(target.seriesKey)) == vshard;
        } else if (auto* w = std::get_if<WriteBatch>(&*command)) {
            matches = !w->series.empty();
            for (auto& s : w->series)
                matches = matches && vshardOf(s) == vshard;
        }
        if (!matches)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: proposed command does not belong to its VShard"));

        return impl_->proposeSink->proposeCommandHinted(vshard, std::move(*command), std::nullopt)
            .then([](ProposeOutcome out) { return encodeProposeOutcome(out); });
    });
    impl_->proto.register_handler(kLeaderReadIndex, [this](seastar::sstring data) {
        if (!impl_->readIndexSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no read-index sink (node not RF>1)"));
        auto vs = decU16(data);
        if (!vs)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed leaderReadIndex request"));
        // futurize_invoke turns a SYNCHRONOUS throw (not-hosted) into an exceptional
        // future, so a reject reaches the client the same way a non-leader readBarrier
        // rejection does.
        return seastar::futurize_invoke([this, vs = *vs] { return impl_->readIndexSink->leaderReadIndex(vs); })
            .then([](raft::LogIndex idx) { return encU64(idx); });
    });
    impl_->proto.register_handler(kLeaderCommitIndex, [this](seastar::sstring data) {
        if (!impl_->readIndexSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no read-index sink (node not RF>1)"));
        auto vs = decU16(data);
        if (!vs)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed leaderCommitIndex request"));
        return seastar::futurize_invoke([this, vs = *vs] { return impl_->readIndexSink->leaderCommitIndex(vs); })
            .then([](raft::LogIndex idx) { return encU64(idx); });
    });
    impl_->proto.register_handler(kRequireV1, [this](seastar::sstring data) {
        auto version = decU32At(data, 0);
        if (!version || data.size() != sizeof(uint32_t))
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed v1 handshake"));
        if (*version != kWriteBatchFormatV1)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: peer does not speak wire v1"));
        return seastar::make_ready_future<seastar::sstring>(encU32(kWriteBatchFormatV1));
    });
    impl_->makeStubs();
    impl_->listenServer(local, perShardListener);
    return seastar::make_ready_future<>();
}

seastar::future<> DataPlaneRpc::startClientOnly() {
    if (impl_->server)
        throw std::logic_error("DataPlaneRpc::startClientOnly after start");
    impl_->makeStubs();
    return seastar::make_ready_future<>();
}

void DataPlaneRpc::setProposeSink(ProposeSink& sink) {
    impl_->proposeSink = &sink;
}

void DataPlaneRpc::setReadIndexSink(ReadIndexSink& sink) {
    impl_->readIndexSink = &sink;
}

void DataPlaneRpc::setFrozenDeletePlanSink(FrozenDeletePlanSink& sink) {
    impl_->frozenDeletePlanSink = &sink;
}

void DataPlaneRpc::setControlJoinSink(ControlJoinSink& sink) {
    impl_->controlJoinSink = &sink;
}

void DataPlaneRpc::setMoveDestinationSink(MoveDestinationSink& sink) {
    impl_->moveDestinationSink = &sink;
}

void DataPlaneRpc::setMoveActuationSink(MoveActuationSink& sink) {
    impl_->moveActuationSink = &sink;
}

void DataPlaneRpc::setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem) {
    if (std::ranges::any_of(impl_->peers, [](const auto& peer) { return peer.second.tlsServerName.empty(); }))
        throw std::logic_error("DataPlaneRpc: TLS requires a server name for every registered peer");
    seastar::tls::credentials_builder b;
    b.set_x509_trust(seastar::tls::blob(caPem.data(), caPem.size()), seastar::tls::x509_crt_format::PEM);
    b.set_x509_key(seastar::tls::blob(certPem.data(), certPem.size()), seastar::tls::blob(keyPem.data(), keyPem.size()),
                   seastar::tls::x509_crt_format::PEM);
    // Mutual TLS: the server REQUIRES a client certificate (a plaintext or
    // wrong-CA peer cannot connect).
    b.set_client_auth(seastar::tls::client_auth::REQUIRE);
    impl_->serverCreds = b.build_server_credentials();
    impl_->clientCreds = b.build_certificate_credentials();
    impl_->tlsEnabled = true;
}

seastar::future<> DataPlaneRpc::ensureV1(NodeId to, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    if (impl_->v1Connections.contains(to))
        co_return;
    const seastar::sstring request = encU32(kWriteBatchFormatV1);
    seastar::sstring reply = deadline ? co_await impl_->requireV1TimedStub(*conn, *deadline, request)
                                      : co_await impl_->requireV1Stub(*conn, request);
    const auto version = decU32At(reply, 0);
    if (!version || reply.size() != sizeof(uint32_t) || *version != kWriteBatchFormatV1)
        throw std::runtime_error("dataplane: malformed v1 handshake reply");
    impl_->v1Connections.insert(to);
}

seastar::future<raft::LogIndex> DataPlaneRpc::leaderReadIndex(NodeId to, uint16_t vshard) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    const auto deadline = seastar::rpc::rpc_clock_type::now() + ReadIndexSink::kAttemptTimeout;
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring reply = co_await impl_->leaderReadIndexTimedStub(*conn, deadline, encU16(vshard));
    auto idx = decU64(reply);
    if (!idx)
        throw std::runtime_error("dataplane: malformed leaderReadIndex reply");
    co_return *idx;
}

seastar::future<raft::LogIndex> DataPlaneRpc::leaderCommitIndex(NodeId to, uint16_t vshard) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    const auto deadline = seastar::rpc::rpc_clock_type::now() + ReadIndexSink::kAttemptTimeout;
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring reply = co_await impl_->leaderCommitIndexTimedStub(*conn, deadline, encU16(vshard));
    auto idx = decU64(reply);
    if (!idx)
        throw std::runtime_error("dataplane: malformed leaderCommitIndex reply");
    co_return *idx;
}

seastar::future<> DataPlaneRpc::forwardWriteBatch(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeWriteBatch(batch);
    co_await impl_->forwardBatchStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req) {
    return queryNode(to, std::move(req), std::nullopt);
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeNodeQueryRequest(req);
    const bool prof = [] {
        static const bool on = [] {
            const char* e = std::getenv("TIMESTAR_QUERY_PROFILE");
            return e && e[0] == '1';
        }();
        return on;
    }();
    const auto tRpc0 = std::chrono::high_resolution_clock::now();
    seastar::sstring frame(bytes.data(), bytes.size());
    seastar::sstring reply = deadline ? co_await impl_->queryNodeTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->queryNodeStub(*conn, frame);
    const double rpcMs =
        std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - tRpc0).count();
    const auto tDec0 = std::chrono::high_resolution_clock::now();
    auto part = decodeNodeQueryPartial(std::string(reply.data(), reply.size()));
    if (prof) {
        timestar::http_log.info(
            "[WIRE_PROFILE] queryNode(node {}): rpc_roundtrip={:.1f}ms reply={} bytes decode={:.1f}ms", to, rpcMs,
            reply.size(),
            std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - tDec0).count());
    }
    if (!part)
        throw std::runtime_error("dataplane: malformed node query partial");
    // The other half of the gate: a peer we did NOT ask to resolve anything must not answer
    // with redirects. It cannot happen from a correct holder (the tail is emitted only in
    // answer to a resolve list), so this catches a peer whose reply shape has drifted, and
    // it refuses rather than letting `applyReadRedirects` hold VShards outstanding on the
    // word of a node that was never asked.
    if (req.resolveVShards.empty() && !part->redirects.empty())
        throw std::runtime_error("dataplane: peer " + std::to_string(to) +
                                 " answered with leader redirects for a read that asked it to resolve nothing");
    co_return std::move(*part);
}

seastar::future<MetadataResult> DataPlaneRpc::queryMetadata(NodeId to, MetadataRequest req) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeMetadataRequest(req);
    seastar::sstring reply = co_await impl_->queryMetadataStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
    auto res = decodeMetadataResult(std::string(reply.data(), reply.size()));
    if (!res)
        throw std::runtime_error("dataplane: malformed metadata result");
    co_return std::move(*res);
}

seastar::future<PatternSeriesResult> DataPlaneRpc::findPatternSeries(NodeId to, PatternSeriesRequest req,
                                                                     OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = encodePatternSeriesRequest(req);
    seastar::sstring frame(encoded.data(), encoded.size());
    seastar::sstring reply = deadline ? co_await impl_->findPatternSeriesTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->findPatternSeriesStub(*conn, frame);
    auto result = decodePatternSeriesResult(std::string(reply.data(), reply.size()));
    if (!result)
        throw std::runtime_error("dataplane: malformed pattern-series result");
    if ((!result->limitExceeded && result->seriesKeys.size() > req.maxSeries) ||
        (result->limitExceeded && !result->seriesKeys.empty()))
        throw std::runtime_error("dataplane: peer returned an invalid pattern-series bound result");
    if (req.resolveVShards.empty() && !result->redirects.empty())
        throw std::runtime_error(
            "dataplane: peer returned pattern-series redirects without being asked to resolve leadership");
    co_return std::move(*result);
}

seastar::future<control::FreezeDeletePlanResult> DataPlaneRpc::frozenDeletePlan(
    NodeId to, control::FrozenDeletePlanRpcRequest request, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = control::encodeFrozenDeletePlanRpcRequest(request);
    seastar::sstring frame(encoded.data(), encoded.size());
    seastar::sstring reply = deadline ? co_await impl_->frozenDeletePlanTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->frozenDeletePlanStub(*conn, frame);
    auto result = control::decodeFrozenDeletePlanRpcResult(std::string(reply.data(), reply.size()));
    if (!result)
        throw std::runtime_error("dataplane: malformed frozen delete-plan result");
    if ((request.operation == control::FrozenDeletePlanRpcOperation::Lookup &&
         result->status == control::FreezeDeletePlanStatus::Capacity) ||
        (request.operation == control::FrozenDeletePlanRpcOperation::Freeze &&
         result->status == control::FreezeDeletePlanStatus::NotFound))
        throw std::runtime_error("dataplane: invalid frozen delete-plan result for requested operation");
    co_return std::move(*result);
}

seastar::future<control::ControlJoinResult> DataPlaneRpc::controlJoin(NodeId to, control::ControlJoinRequest request,
                                                                      OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = control::encodeControlJoinRequest(request);
    seastar::sstring frame(encoded.data(), encoded.size());
    seastar::sstring reply = deadline ? co_await impl_->controlJoinTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->controlJoinStub(*conn, frame);
    auto result = control::decodeControlJoinResult(std::string(reply.data(), reply.size()));
    if (!result)
        throw std::runtime_error("dataplane: malformed control join result");
    co_return *result;
}

seastar::future<control::EnsureMoveDestinationResult> DataPlaneRpc::ensureMoveDestination(
    NodeId to, control::EnsureMoveDestinationRequest request, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = control::encodeEnsureMoveDestinationRequest(request);
    seastar::sstring frame(encoded.data(), encoded.size());
    seastar::sstring reply = deadline ? co_await impl_->ensureMoveDestinationTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->ensureMoveDestinationStub(*conn, frame);
    auto result = control::decodeEnsureMoveDestinationResult(std::string(reply.data(), reply.size()));
    if (!result)
        throw std::runtime_error("dataplane: malformed ensure-move-destination result");
    co_return *result;
}

seastar::future<control::ActuateMoveResult> DataPlaneRpc::actuateMove(NodeId to, control::ActuateMoveRequest request,
                                                                      OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = control::encodeEnsureMoveDestinationRequest(request);
    seastar::sstring frame(encoded.data(), encoded.size());
    seastar::sstring reply = deadline ? co_await impl_->actuateMoveTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->actuateMoveStub(*conn, frame);
    auto result = control::decodeActuateMoveResult(std::string(reply.data(), reply.size()));
    if (!result)
        throw std::runtime_error("dataplane: malformed actuate-move result");
    co_return std::move(*result);
}

seastar::future<ProposeOutcome> DataPlaneRpc::proposeWriteHinted(NodeId to, VShardBatchView view,
                                                                 OptDeadline deadline) {
    // `view` borrows the CALLER's groups (the retry loop keeps them so it can re-dispatch
    // only what failed). It is read across the awaits below; the caller awaits this
    // future before its groups die, so the pointees stay valid throughout -- the read is
    // NOT confined to before the first suspension, and must not be assumed to be.
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    std::vector<uint16_t> vshards;
    vshards.reserve(view.size());
    for (const auto* g : view)
        vshards.push_back(g->first);
    // Bounded by the same deadline as the propose it gates.
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    // TWO REFUSALS, AND NEITHER IMPLIES THE OTHER (debt D-31). This one is per-SLICE: the
    // peer turns each group into ONE Raft entry, so a frame that fits the wire can still
    // hold a slice that cannot be proposed. Refusing here makes it a local, terminal 413 naming the
    // VShard instead of the receiving leader's ProposalTooLargeError arriving as an opaque
    // remote error that the router retries against every other leader.
    if (auto over = firstUnproposableSlice(view, raft::RaftGroup::kMaxProposalBytes))
        throw WriteFrameTooLargeError("dataplane: write slice for vshard " + std::to_string(over->vshard) +
                                      " encodes to as much as " + std::to_string(over->bytes) + " bytes, over the " +
                                      std::to_string(raft::RaftGroup::kMaxProposalBytes) +
                                      "-byte Raft entry limit; split the batch");
    // Encode straight from the borrowed groups -- no mergeVShardBatches allocation.
    std::string bytes = encodeWriteBatch(view);
    if (bytes.size() > kMaxOutboundFrameBytes)
        throw WriteFrameTooLargeError("dataplane: encoded write slice of " + std::to_string(bytes.size()) +
                                      " bytes exceeds the " + std::to_string(kMaxOutboundFrameBytes) +
                                      "-byte inter-node frame limit; split the batch");
    const seastar::sstring frame(bytes.data(), bytes.size());
    seastar::sstring reply = deadline ? co_await impl_->proposeWriteHintedTimedStub(*conn, *deadline, frame)
                                      : co_await impl_->proposeWriteHintedStub(*conn, frame);
    auto out = decodeProposeOutcome(reply);
    if (!out)
        throw std::runtime_error("dataplane: malformed hinted propose reply");
    // A peer answering '1' names no VShards; fill in what we asked it for, so the caller's
    // committed-set arithmetic works uniformly.
    if (out->committed)
        out->committedVShards = std::move(vshards);
    co_return std::move(*out);
}

seastar::future<ProposeOutcome> DataPlaneRpc::proposeCommandHinted(NodeId to, uint16_t vshard,
                                                                   ReplicatedCommand command, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    co_await ensureV1(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string encoded = encodeReplicatedCommand(command);
    if (encoded.size() > raft::RaftGroup::kMaxProposalBytes)
        throw WriteFrameTooLargeError("dataplane: replicated command exceeds the Raft entry limit");
    std::string frame;
    frame.reserve(2 + encoded.size());
    frame.push_back(static_cast<char>(vshard & 0xff));
    frame.push_back(static_cast<char>((vshard >> 8) & 0xff));
    frame += encoded;
    if (frame.size() > kMaxOutboundFrameBytes)
        throw WriteFrameTooLargeError("dataplane: replicated command exceeds the inter-node frame limit");
    auto reply = deadline
                     ? co_await impl_->proposeCommandHintedTimedStub(*conn, *deadline,
                                                                     seastar::sstring(frame.data(), frame.size()))
                     : co_await impl_->proposeCommandHintedStub(*conn, seastar::sstring(frame.data(), frame.size()));
    auto out = decodeProposeOutcome(reply);
    if (!out)
        throw std::runtime_error("dataplane: malformed replicated-command reply");
    if (out->committed)
        out->committedVShards = {vshard};
    co_return std::move(*out);
}

seastar::future<> DataPlaneRpc::stop() {
    if (impl_->stopping)
        co_return;  // failed-start cleanup and an explicit owner stop may both arrive
    // Stop peer clients FIRST (aborts our outbound calls and closes our
    // connections into peers' servers), THEN stop our server. Doing the server
    // first would wait for peers' still-open inbound connections to close -- a
    // deadlock when nodes shut down together, each waiting on the others.
    impl_->stopping = true;
    for (auto& [id, c] : impl_->clients)
        co_await c->stop();
    // Drain any background stops of retired (reconnected-away) connections before the
    // clients they reference are destroyed.
    co_await impl_->retireGate.close();
    impl_->clients.clear();
    if (impl_->server)
        co_await impl_->server->stop();
}

}  // namespace timestar::data

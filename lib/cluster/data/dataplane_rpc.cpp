#include "dataplane_rpc.hpp"

#include "../../utils/logger.hpp"
#include "../raft/raft_group.hpp"  // kMaxProposalBytes -- the bound a slice must fit (D-31)
#include "../reconnect_policy.hpp"
#include "dataplane_codec.hpp"
#include "dataplane_limits.hpp"
#include "journal_format.hpp"
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

constexpr uint64_t kForwardWrites = 1;      // sstring -> sstring (legacy DataPoint, waited: applied)
constexpr uint64_t kQueryRemote = 2;        // sstring -> sstring (legacy DataPoint, waited: partial)
constexpr uint64_t kForwardWriteBatch = 3;  // sstring -> sstring (enriched WriteBatch, waited: applied)
constexpr uint64_t kQueryNode = 4;          // sstring -> sstring (enriched NodeQueryRequest, waited: partial)
constexpr uint64_t kQueryMetadata = 5;      // sstring -> sstring (MetadataRequest, waited: MetadataResult)
constexpr uint64_t kProposeWrite = 6;       // sstring -> sstring (WriteBatch, waited: "1"/"0" committed)
constexpr uint64_t kLeaderReadIndex = 7;    // sstring(u16 vshard) -> sstring(u64 readIndex); throws if not leader
constexpr uint64_t kLeaderCommitIndex = 8;  // sstring(u16 vshard) -> sstring(u64 commitIndex); throws if not leader
constexpr uint64_t kNegotiateVersion = 9;   // sstring(u32 min,u32 max) -> sstring(u32 agreed); throws if incompatible
// write-scaleout 3a. Same request as kProposeWrite (an encoded WriteBatch); the reply
// carries the COMMITTED SET plus a LEADER HINT per rejected VShard:
//     '1'  -> every slice committed
//     '0' u16 committedCount {u16 vshard}* u16 rejectCount {u16 vshard u64 leader u8 kind}*
// THE REPLY SHAPE IS NOW FROZEN UNDER v3. It was changed once after v3 was introduced
// (the committed set was added to what had been a reject-only reply) WITHOUT a version
// bump, and that was safe for exactly one reason: v3 has never been released -- no tag
// contains the commit that introduced it, so no peer anywhere speaks the old shape. A
// mixed-version cluster would NOT have failed closed on it: both shapes start with the
// same '1'/'0' byte and the old decoder read the new reply's committed-count as its reject
// count, which only happens to be rejected by the trailing-length arithmetic. That is luck,
// not a mechanism. ANY further change to this reply requires a new negotiated version.
//
// It is a SEPARATE verb rather than a widened kProposeWrite reply because a reply shape
// is chosen by the SERVER, which does not know which version it negotiated with the
// caller (the handshake is cached client-side, per connection). An old client would
// therefore have read an extended reply as "malformed propose reply" and turned a clean
// not-leader into a 5xx during a rolling upgrade. A new verb inverts the choice: the
// CLIENT picks it, and only once the negotiated version says the peer answers it, so
// both directions of a mixed-version cluster keep working unchanged.
constexpr uint64_t kProposeWriteHinted = 10;
// One already-VShard-scoped ReplicatedCommand (currently the production delete
// path). The request is u16 VShard + the checksummed ReplicatedCommand frame; the
// response reuses the committed-set/hint shape above.
constexpr uint64_t kProposeCommandHinted = 11;
// Quorum-fenced, VShard-restricted catalog expansion for pattern deletes.
// Requires data-plane wire v4; the client checks negotiation before calling it.
constexpr uint64_t kFindPatternSeries = 12;
// Lookup/freeze a version-6 group-0 pattern-delete plan through the node that
// currently leads the control group. This is a separate verb because older
// peers do not register it; the client negotiates v6 before selecting it.
constexpr uint64_t kFrozenDeletePlan = 13;
// Exact identity + full-range capability advertisement. A client selects this
// verb only after negotiating protocol v7, so a pre-v7 peer is never sent an
// unknown request during a rolling upgrade.
constexpr uint64_t kNodeCapability = 14;

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

// The kProposeWriteHinted reply. '1' with no body is byte-identical to the old
// kProposeWrite success reply, deliberately: the two verbs agree on the common case and
// diverge only where there is something extra to say. Otherwise:
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
    seastar::rpc::protocol<DpSerializer> proto{DpSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<DpSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    LocalStore* sink = nullptr;              // legacy DataPoint path
    NodeStore* nodeSink = nullptr;           // enriched WriteBatch path (F.4)
    ProposeSink* proposeSink = nullptr;      // RF=3 Raft propose target (M3)
    ReadIndexSink* readIndexSink = nullptr;  // replica-read leader-reach target (M4)
    FrozenDeletePlanSink* frozenDeletePlanSink = nullptr;  // group-0 request target
    std::optional<std::pair<std::string, control::NodeRecord>> localNodeCapability;
    // Ordered wire/protocol range this node supports (M6/X). Some versions change
    // WriteBatch bytes and others add separate RPC verbs; peers negotiate down to
    // the highest point both know.
    features::VersionRange localVersion{1, kWriteBatchFormatMax};
    // Version agreed with each peer (kNegotiateVersion), cached per connection: a
    // handshake per peer, not per write. Dropped when a connection is retired, because
    // the peer may come back on a different binary.
    std::map<NodeId, uint32_t> agreedVersion;
    // Mutual TLS (X1b): null unless setTlsCredentials was called. serverCreds requires a
    // client cert; clientCreds presents ours + trusts the CA; peerName is the SAN we
    // verify the server against.
    seastar::shared_ptr<seastar::tls::server_credentials> serverCreds;
    seastar::shared_ptr<seastar::tls::certificate_credentials> clientCreds;
    std::string tlsPeerName;
    bool tlsEnabled = false;
    bool stopping = false;
    // Earliest time a peer whose connection died may be re-attempted (see clientFor).
    std::map<NodeId, seastar::lowres_clock::time_point> nextRetry;
    // Keeps background stops of retired (dead) connections alive across shutdown.
    seastar::gate retireGate;
    // Client stubs are created ONCE (a stub allocated per concurrent call can
    // corrupt reply routing / message-id bookkeeping). Reused for every call.
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> forwardStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> forwardBatchStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryNodeStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryMetadataStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> findPatternSeriesStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeWriteStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeWriteHintedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeCommandHintedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> frozenDeletePlanStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> nodeCapabilityStub;
    // DEADLINE-CARRYING variants of the three verbs the write path awaits
    // (write-scaleout 3f). seastar's rpc client stub has a time_point overload; without
    // it an awaited call has NO timeout at all, so a peer that accepts the connection and
    // then black-holes it (a half-dead TCP path, a wedged reactor) blocks the caller
    // indefinitely -- the router's deadline is only checked BETWEEN attempts, so a single
    // attempt could hold its WriteAdmission charge for minutes and take the whole shard
    // to 503 behind it. That amplifies exactly the [D6] window Phase 4 is meant to close.
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        proposeWriteTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        proposeWriteHintedTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        proposeCommandHintedTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::rpc::rpc_clock_type::time_point,
                                                    seastar::sstring)>
        negotiateVersionTimedStub;
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
        nodeCapabilityTimedStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> leaderReadIndexStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> leaderCommitIndexStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> negotiateVersionStub;

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
            // A reconnect may reach a RESTARTED peer running a different binary --
            // re-handshake rather than keep speaking the old connection's version.
            agreedVersion.erase(to);
        }
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        std::unique_ptr<seastar::rpc::protocol<DpSerializer>::client> c;
        const seastar::rpc::client_options copts = peerClientOptions();
        if (tlsEnabled) {
            // Present our cert + verify the peer's against tlsPeerName over TLS.
            seastar::tls::tls_options topts;
            topts.server_name = seastar::sstring(tlsPeerName);
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(
                proto, copts, seastar::tls::socket(clientCreds, topts), pit->second);
        } else {
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(proto, copts, pit->second);
        }
        auto* p = c.get();
        clients[to] = std::move(c);
        return p;
    }

    // Create every client stub ONCE (both command paths). A node registers only
    // one server-side sink, but the client stubs are path-independent -- a node
    // may need to reach peers on either path.
    void makeStubs() {
        forwardStub = proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWrites);
        queryStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryRemote);
        forwardBatchStub = proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWriteBatch);
        queryNodeStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryNode);
        queryMetadataStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryMetadata);
        findPatternSeriesStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFindPatternSeries);
        proposeWriteStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWrite);
        proposeWriteHintedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWriteHinted);
        proposeCommandHintedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeCommandHinted);
        frozenDeletePlanStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFrozenDeletePlan);
        nodeCapabilityStub = proto.make_client<seastar::sstring(seastar::sstring)>(kNodeCapability);
        proposeWriteTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWrite);
        proposeWriteHintedTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWriteHinted);
        proposeCommandHintedTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeCommandHinted);
        negotiateVersionTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kNegotiateVersion);
        queryNodeTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryNode);
        findPatternSeriesTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFindPatternSeries);
        frozenDeletePlanTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kFrozenDeletePlan);
        nodeCapabilityTimedStub = proto.make_client<seastar::sstring(seastar::sstring)>(kNodeCapability);
        leaderReadIndexStub = proto.make_client<seastar::sstring(seastar::sstring)>(kLeaderReadIndex);
        leaderCommitIndexStub = proto.make_client<seastar::sstring(seastar::sstring)>(kLeaderCommitIndex);
        negotiateVersionStub = proto.make_client<seastar::sstring(seastar::sstring)>(kNegotiateVersion);
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

void DataPlaneRpc::addPeer(NodeId id, seastar::socket_address addr) {
    impl_->peers[id] = addr;
}

seastar::future<> DataPlaneRpc::start(seastar::socket_address local, LocalStore& sink) {
    // start() is not idempotent: a second call would re-register verbs (throws) or
    // re-listen on the bound addr (throws) AFTER mutating sink/handler state, leaving
    // half-started state. Fail loudly and early instead, in release builds too.
    if (impl_->server)
        throw std::logic_error("DataPlaneRpc::start called more than once");
    impl_->sink = &sink;
    // Forwarded writes: apply into the local sink and ack (waited), so the sender
    // only considers the write accepted once the owner has stored it. The ack is a
    // one-byte sstring ("k") -- the DpSerializer only carries sstring. Non-coroutine
    // handlers (return the future directly) to keep the rpc reply path simple.
    impl_->proto.register_handler(kForwardWrites, [this](seastar::sstring data) {
        auto pts = decodePoints(std::string(data.data(), data.size()));
        if (!pts)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed write batch"));
        return impl_->sink->applyWrites(std::move(*pts)).then([] { return seastar::sstring("k"); });
    });
    impl_->proto.register_handler(kQueryRemote, [this](seastar::sstring data) {
        auto spec = decodeQuerySpec(std::string(data.data(), data.size()));
        if (!spec)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed query spec"));
        return impl_->sink->queryLocal(std::move(*spec)).then([](QueryPartial part) {
            std::string enc = encodeQueryPartial(part);
            return seastar::sstring(enc.data(), enc.size());
        });
    });
    impl_->makeStubs();
    impl_->listenServer(local, false);
    return seastar::make_ready_future<>();
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
        return impl_->frozenDeletePlanSink->handleFrozenDeletePlan(std::move(*request)).then(
            [](control::FreezeDeletePlanResult result) {
                std::string encoded = control::encodeFrozenDeletePlanRpcResult(result);
                return seastar::sstring(encoded.data(), encoded.size());
            });
    });
    impl_->proto.register_handler(kNodeCapability, [this](seastar::sstring data) {
        const auto expected = decU64(data);
        if (!expected)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed node capability request"));
        // Always disclose the actual configured identity. Rejecting a request
        // whose expected id differs would collapse a permanent topology error
        // into an untyped remote RPC failure; the caller needs the reply so it
        // can classify the mismatch and fail readiness closed.
        if (!impl_->localNodeCapability)
            return seastar::make_ready_future<seastar::sstring>(seastar::sstring{});
        control::NodeCapabilityAdvertisement capability{
            impl_->localNodeCapability->first, impl_->localNodeCapability->second, impl_->localVersion};
        std::string encoded = control::encodeNodeCapabilityAdvertisement(capability);
        return seastar::make_ready_future<seastar::sstring>(seastar::sstring(encoded.data(), encoded.size()));
    });
    impl_->proto.register_handler(kProposeWrite, [this](seastar::sstring data) {
        if (!impl_->proposeSink)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: no propose sink (node not RF>1)"));
        auto batch = decodeWriteBatch(std::string(data.data(), data.size()));
        if (!batch)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed propose batch"));
        return impl_->proposeSink->proposeBatch(std::move(*batch)).then([](bool ok) {
            return seastar::sstring(ok ? "1" : "0");
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
        if (const auto* d = std::get_if<DeleteRangeKey>(&*command)) {
            matches = timestar::virtualShard(SeriesId128::fromSeriesKey(d->seriesKey)) == vshard;
        } else if (const auto* batch = std::get_if<DeleteRangeBatch>(&*command)) {
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
    impl_->proto.register_handler(kNegotiateVersion, [this](seastar::sstring data) {
        auto peerMin = decU32At(data, 0);
        auto peerMax = decU32At(data, 4);
        if (!peerMin || !peerMax || *peerMin > *peerMax)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed negotiateVersion request"));
        auto agreed = features::negotiate(impl_->localVersion, features::VersionRange{*peerMin, *peerMax});
        if (!agreed)
            // No overlapping version: refuse the peer rather than mis-frame a format it
            // cannot read (rolling-upgrade safety, decision 8).
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: incompatible wire versions (no overlap with peer)"));
        return seastar::make_ready_future<seastar::sstring>(encU32(*agreed));
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

void DataPlaneRpc::setLocalNodeCapability(std::string clusterUuid, control::NodeRecord record) {
    if (impl_->server)
        throw std::logic_error("DataPlaneRpc node capability must be configured before start");
    // Validate once through the production codec. This rejects malformed local
    // identity before the listener begins serving rather than on the first probe.
    (void)control::encodeNodeCapabilityAdvertisement(
        control::NodeCapabilityAdvertisement{clusterUuid, record, impl_->localVersion});
    impl_->localNodeCapability = std::make_pair(std::move(clusterUuid), std::move(record));
}

void DataPlaneRpc::setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem,
                                     std::string expectedPeerName) {
    seastar::tls::credentials_builder b;
    b.set_x509_trust(seastar::tls::blob(caPem.data(), caPem.size()), seastar::tls::x509_crt_format::PEM);
    b.set_x509_key(seastar::tls::blob(certPem.data(), certPem.size()), seastar::tls::blob(keyPem.data(), keyPem.size()),
                   seastar::tls::x509_crt_format::PEM);
    // Mutual TLS: the server REQUIRES a client certificate (a plaintext or
    // wrong-CA peer cannot connect).
    b.set_client_auth(seastar::tls::client_auth::REQUIRE);
    impl_->serverCreds = b.build_server_credentials();
    impl_->clientCreds = b.build_certificate_credentials();
    impl_->tlsPeerName = std::move(expectedPeerName);
    impl_->tlsEnabled = true;
}

void DataPlaneRpc::setLocalVersion(features::VersionRange range) {
    impl_->localVersion = range;
}

features::VersionRange DataPlaneRpc::localVersion() const {
    return impl_->localVersion;
}

seastar::future<uint32_t> DataPlaneRpc::negotiateVersion(NodeId to) {
    return negotiateVersion(to, std::nullopt);
}

seastar::future<uint32_t> DataPlaneRpc::negotiateVersion(NodeId to, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring req = encU32(impl_->localVersion.min) + encU32(impl_->localVersion.max);
    // A non-overlapping peer throws server-side; that exception propagates here so the
    // caller refuses the peer (never falls back to a silently-mismatched version).
    //
    // The handshake is bounded by the SAME deadline as the write it precedes: it is an
    // awaited round trip to the same peer, so a black-holed connection would otherwise
    // hang here, before a single byte of the batch was even encoded -- an unbounded
    // suspension in front of a bounded one.
    seastar::sstring reply = deadline ? co_await impl_->negotiateVersionTimedStub(*conn, *deadline, req)
                                      : co_await impl_->negotiateVersionStub(*conn, req);
    auto agreed = decU32At(reply, 0);
    if (!agreed)
        throw std::runtime_error("dataplane: malformed negotiateVersion reply");
    co_return *agreed;
}

seastar::future<raft::LogIndex> DataPlaneRpc::leaderReadIndex(NodeId to, uint16_t vshard) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring reply = co_await impl_->leaderReadIndexStub(*conn, encU16(vshard));
    auto idx = decU64(reply);
    if (!idx)
        throw std::runtime_error("dataplane: malformed leaderReadIndex reply");
    co_return *idx;
}

seastar::future<raft::LogIndex> DataPlaneRpc::leaderCommitIndex(NodeId to, uint16_t vshard) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring reply = co_await impl_->leaderCommitIndexStub(*conn, encU16(vshard));
    auto idx = decU64(reply);
    if (!idx)
        throw std::runtime_error("dataplane: malformed leaderCommitIndex reply");
    co_return *idx;
}

seastar::future<> DataPlaneRpc::forwardWrites(NodeId to, std::vector<DataPoint> points) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodePoints(points);
    co_await impl_->forwardStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<QueryPartial> DataPlaneRpc::queryRemote(NodeId to, QuerySpec spec) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeQuerySpec(spec);
    seastar::sstring reply = co_await impl_->queryStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
    auto part = decodeQueryPartial(std::string(reply.data(), reply.size()));
    if (!part)
        throw std::runtime_error("dataplane: malformed query partial");
    co_return std::move(*part);
}

seastar::future<uint32_t> DataPlaneRpc::versionFor(NodeId to) {
    return versionFor(to, std::nullopt);
}

seastar::future<uint32_t> DataPlaneRpc::versionFor(NodeId to, OptDeadline deadline) {
    // The WriteBatch format to speak with this peer. Handshake once per connection and
    // cache it; a peer that cannot read anything we can write THROWS out of
    // negotiateVersion (no overlapping range), which fails the write closed instead of
    // shipping it a frame it would misparse. Failures are not cached, so the next
    // attempt re-handshakes.
    //
    // Resolve the CONNECTION FIRST, before reading the cache. clientFor is what notices
    // a dead client, retires it and drops the cached version with it -- so reading the
    // cache first would hand back the DEAD connection's version and send exactly one
    // frame at it over the fresh connection. That is the rolling-DOWNGRADE case this
    // handshake exists for: a peer restarting on an older binary would get one v2 frame
    // (a spurious client 5xx rather than corruption, since it fails closed -- but it is
    // the case the gate is for).
    if (!impl_->clientFor(to))
        throw std::runtime_error("dataplane: unknown peer");
    auto it = impl_->agreedVersion.find(to);
    if (it != impl_->agreedVersion.end())
        co_return it->second;
    const uint32_t agreed = co_await negotiateVersion(to, deadline);
    impl_->agreedVersion[to] = agreed;
    co_return agreed;
}

seastar::future<> DataPlaneRpc::forwardWriteBatch(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    const uint32_t version = co_await versionFor(to);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeWriteBatch(batch, version);
    co_await impl_->forwardBatchStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req) {
    return queryNode(to, std::move(req), std::nullopt);
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    // WIRE-VERSION GATE FOR THE RESOLVE/REDIRECT PAIR (debt D-25). Handshaked ONLY when the
    // request actually carries a resolve tail, which is the whole of the cost argument: a
    // read that names nothing to resolve -- every RF == N read, every RF == 1 read, every
    // metadata fan-out -- takes exactly the round trips it took before, and an RF < N read
    // pays the handshake once per (shard, peer) connection in the STEADY STATE, because
    // `versionFor` caches it and drops it only when the connection is retired. (Concurrent
    // COLD reads to the same peer each handshake: there is no in-flight dedup on
    // `versionFor`. Pre-existing on the write path, recorded as a residual on debt D-25.)
    // See node_query.hpp for why absence of a reply tail cannot be read as "no redirects".
    //
    if (!req.resolveVShards.empty()) {
        const uint32_t version = co_await versionFor(to, deadline);
        if (version < kNodeQueryResolveMinVersion)
            throw ReadResolveUnsupportedError(
                "dataplane: peer " + std::to_string(to) + " negotiated wire v" + std::to_string(version) + ", below v" +
                std::to_string(kNodeQueryResolveMinVersion) + " -- it cannot resolve VShard leadership on request");
    }
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
    const uint32_t version = co_await versionFor(to, deadline);
    if (version < kPatternSeriesMinVersion)
        throw PatternSeriesUnsupportedError(
            "dataplane: peer " + std::to_string(to) + " negotiated wire v" + std::to_string(version) + ", below v" +
            std::to_string(kPatternSeriesMinVersion) + " -- it cannot provide quorum-fenced pattern-series discovery");
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
    const uint32_t version = co_await versionFor(to, deadline);
    if (version < kFrozenDeletePlanActivationVersion)
        throw ClusterFormatUnsupportedError(
            "dataplane: peer " + std::to_string(to) + " negotiated wire v" + std::to_string(version) +
            ", below v" + std::to_string(kFrozenDeletePlanActivationVersion) +
            " -- it cannot forward frozen pattern-delete plans");
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

seastar::future<control::NodeCapabilityAdvertisement> DataPlaneRpc::nodeCapability(
    NodeId to, OptDeadline deadline) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    const uint32_t agreed = co_await versionFor(to, deadline);
    if (agreed < kWriteBatchFormatV7)
        throw ClusterFormatUnsupportedError(
            "dataplane: peer " + std::to_string(to) + " negotiated wire v" + std::to_string(agreed) +
            ", below v" + std::to_string(kWriteBatchFormatV7) +
            " -- it cannot provide identity-bound capabilities");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    const seastar::sstring request = encU64(to);
    seastar::sstring reply = deadline ? co_await impl_->nodeCapabilityTimedStub(*conn, *deadline, request)
                                      : co_await impl_->nodeCapabilityStub(*conn, request);
    auto capability = control::decodeNodeCapabilityAdvertisement(std::string(reply.data(), reply.size()));
    if (!capability || capability->record.raftId != to || !capability->formats.supports(kWriteBatchFormatV7) ||
        features::negotiate(impl_->localVersion, capability->formats) != agreed)
        throw NodeCapabilityMismatchError("dataplane: malformed or inconsistent node capability reply");
    co_return std::move(*capability);
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
    // Bounded by the SAME deadline as the propose it gates -- see negotiateVersion.
    const uint32_t version = co_await versionFor(to, deadline);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    // TWO REFUSALS, AND NEITHER IMPLIES THE OTHER (debt D-31). This one is per-SLICE: the
    // peer turns each group into ONE Raft entry, so a frame that fits the wire can still
    // hold a slice that cannot be proposed -- and it is charged at the worst format
    // version, because the version the receiver's journal gate emits is not the version
    // negotiated for this wire. Refusing here makes it a local, terminal 413 naming the
    // VShard instead of the receiving leader's ProposalTooLargeError arriving as an opaque
    // remote error that the router retries against every other leader.
    if (auto over = firstUnproposableSlice(view, raft::RaftGroup::kMaxProposalBytes))
        throw WriteFrameTooLargeError("dataplane: write slice for vshard " + std::to_string(over->vshard) +
                                      " encodes to as much as " + std::to_string(over->bytes) + " bytes, over the " +
                                      std::to_string(raft::RaftGroup::kMaxProposalBytes) +
                                      "-byte Raft entry limit; split the batch");
    // Encode straight from the borrowed groups -- no mergeVShardBatches allocation.
    std::string bytes = encodeWriteBatch(view, version);
    if (bytes.size() > kMaxOutboundFrameBytes)
        throw WriteFrameTooLargeError("dataplane: encoded write slice of " + std::to_string(bytes.size()) +
                                      " bytes exceeds the " + std::to_string(kMaxOutboundFrameBytes) +
                                      "-byte inter-node frame limit; split the batch");
    const seastar::sstring frame(bytes.data(), bytes.size());
    if (version < kWriteBatchFormatV3) {
        // The peer predates the hinted verb: use the v1-shaped one and report NOTHING
        // committed on failure. Correctness is unchanged (the caller still retries the
        // whole view against a re-resolved leader), only the routing is blind -- exactly
        // the v1 behaviour.
        seastar::sstring old = deadline ? co_await impl_->proposeWriteTimedStub(*conn, *deadline, frame)
                                        : co_await impl_->proposeWriteStub(*conn, frame);
        if (old.size() != 1 || (old[0] != '1' && old[0] != '0'))
            throw std::runtime_error("dataplane: malformed propose reply");
        ProposeOutcome out;
        out.committed = old[0] == '1';
        if (out.committed)
            out.committedVShards = std::move(vshards);
        else
            for (uint16_t vs : vshards)
                out.rejects.push_back(SliceReject{vs, kNoNode, WriteFailure::NotLeader});
        co_return out;
    }
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
    // This verb was introduced with the unreleased v3 data plane. Never send it
    // to a negotiated v1/v2 peer: an unknown RPC verb is not an upgrade policy.
    const uint32_t version = co_await versionFor(to, deadline);
    if (version < kWriteBatchFormatV3)
        throw ClusterFormatUnsupportedError("dataplane: peer wire version does not support replicated commands");
    const uint32_t requiredFormat = requiredClusterFormatVersion(command);
    if (requiredFormat >= kBoundedDeleteReceiptActivationVersion && version < kWriteBatchFormatV5)
        throw ClusterFormatUnsupportedError(
            "dataplane: peer wire version does not support bounded delete receipts or Expired outcomes");
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
    if (version < kWriteBatchFormatV5 &&
        std::ranges::any_of(
            out->rejects, [](const SliceReject& reject) { return reject.kind == WriteFailure::Expired; }))
        throw ClusterFormatUnsupportedError(
            "dataplane: peer returned an Expired outcome below the protocol version that defines it");
    if (out->committed)
        out->committedVShards = {vshard};
    co_return std::move(*out);
}

seastar::future<bool> DataPlaneRpc::proposeWrite(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    const uint32_t version = co_await versionFor(to);
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    // The legacy (pre-v3) propose verb sends the WHOLE batch and the receiver splits it,
    // so the per-slice bound cannot be evaluated here -- but the batch bounds every slice
    // of it, which is conservative in the safe direction (debt D-31).
    if (maxEncodedWriteCommandBytes(batch) > raft::RaftGroup::kMaxProposalBytes)
        throw WriteFrameTooLargeError("dataplane: write batch encodes to as much as " +
                                      std::to_string(maxEncodedWriteCommandBytes(batch)) + " bytes, over the " +
                                      std::to_string(raft::RaftGroup::kMaxProposalBytes) +
                                      "-byte Raft entry limit; split the batch");
    std::string bytes = encodeWriteBatch(batch, version);
    if (bytes.size() > kMaxOutboundFrameBytes)
        throw WriteFrameTooLargeError("dataplane: encoded write slice of " + std::to_string(bytes.size()) +
                                      " bytes exceeds the " + std::to_string(kMaxOutboundFrameBytes) +
                                      "-byte inter-node frame limit; split the batch");
    seastar::sstring reply = co_await impl_->proposeWriteStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
    // "1" = committed on the leader, "0" = not-leader (caller redirects). Anything
    // else is a framing/corruption fault -- THROW (like the other verbs) rather than
    // silently reading it as not-leader, which would retry a corrupt path forever.
    if (reply.size() != 1 || (reply[0] != '1' && reply[0] != '0'))
        throw std::runtime_error("dataplane: malformed propose reply");
    co_return reply[0] == '1';
}

seastar::future<> DataPlaneRpc::stop() {
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

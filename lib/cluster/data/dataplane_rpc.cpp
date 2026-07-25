#include "../../utils/logger.hpp"
#include "dataplane_rpc.hpp"

#include "dataplane_codec.hpp"
#include "node_metadata.hpp"
#include "node_query.hpp"
#include "write_record.hpp"

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

// How long before re-attempting a peer connection that died. seastar's rpc::client
// never reconnects itself, so we replace it -- but bounded, so a burst of concurrent
// requests to a down peer does not spawn a connect attempt each.
constexpr std::chrono::milliseconds kReconnectBackoff{200};

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

}  // namespace

struct DataPlaneRpc::Impl {
    using Client = seastar::rpc::protocol<DpSerializer>::client;
    seastar::rpc::protocol<DpSerializer> proto{DpSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<DpSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    LocalStore* sink = nullptr;         // legacy DataPoint path
    NodeStore* nodeSink = nullptr;      // enriched WriteBatch path (F.4)
    ProposeSink* proposeSink = nullptr;      // RF=3 Raft propose target (M3)
    ReadIndexSink* readIndexSink = nullptr;  // replica-read leader-reach target (M4)
    features::VersionRange localVersion{};   // wire-version range this node supports (M6/X)
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
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> proposeWriteStub;
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
            nextRetry[to] = now + kReconnectBackoff;
            retire(std::move(it->second));
            clients.erase(it);
        }
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        std::unique_ptr<seastar::rpc::protocol<DpSerializer>::client> c;
        if (tlsEnabled) {
            // Present our cert + verify the peer's against tlsPeerName over TLS.
            seastar::tls::tls_options topts;
            topts.server_name = seastar::sstring(tlsPeerName);
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(
                proto, seastar::tls::socket(clientCreds, topts), pit->second);
        } else {
            c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(proto, pit->second);
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
        proposeWriteStub = proto.make_client<seastar::sstring(seastar::sstring)>(kProposeWrite);
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
        server =
            std::make_unique<seastar::rpc::protocol<DpSerializer>::server>(proto, std::move(ss));
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
            return seastar::make_exception_future<seastar::sstring>(std::runtime_error(
                "dataplane: incompatible wire versions (no overlap with peer)"));
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

void DataPlaneRpc::setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem,
                                    std::string expectedPeerName) {
    seastar::tls::credentials_builder b;
    b.set_x509_trust(seastar::tls::blob(caPem.data(), caPem.size()), seastar::tls::x509_crt_format::PEM);
    b.set_x509_key(seastar::tls::blob(certPem.data(), certPem.size()),
                   seastar::tls::blob(keyPem.data(), keyPem.size()), seastar::tls::x509_crt_format::PEM);
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

seastar::future<uint32_t> DataPlaneRpc::negotiateVersion(NodeId to) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    seastar::sstring req = encU32(impl_->localVersion.min) + encU32(impl_->localVersion.max);
    // A non-overlapping peer throws server-side; that exception propagates here so the
    // caller refuses the peer (never falls back to a silently-mismatched version).
    seastar::sstring reply = co_await impl_->negotiateVersionStub(*conn, req);
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

seastar::future<> DataPlaneRpc::forwardWriteBatch(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeWriteBatch(batch);
    co_await impl_->forwardBatchStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
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
    seastar::sstring reply = co_await impl_->queryNodeStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
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

seastar::future<bool> DataPlaneRpc::proposeWrite(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeWriteBatch(batch);
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

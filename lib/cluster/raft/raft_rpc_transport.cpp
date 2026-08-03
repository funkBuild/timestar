#include "raft_rpc_transport.hpp"

#include "../../utils/logger.hpp"
#include "../reconnect_policy.hpp"
#include "raft_codec.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/tls.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/later.hh>
#include <utility>
#include <vector>

namespace timestar::raft {

namespace {

// Minimal seastar-rpc serializer: we only ever ship one already-serialized blob
// (the envelope bytes) as an sstring. write/read are found by ADL on this type.
struct RaftSerializer {};

template <typename Output>
void write(RaftSerializer, Output& out, const seastar::sstring& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(v.data(), v.size());
}

template <typename Input>
seastar::sstring read(RaftSerializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    seastar::sstring s = seastar::uninitialized_string(n);
    in.read(s.data(), n);
    return s;
}

// The one v1 delivery verb carries a concatenation of [u32 len LE][TSR1 envelope]
// records. A one-envelope send uses the same framing; there is no retired scalar
// verb or negotiation path in this greenfield protocol.
constexpr uint64_t kDeliverVerb = 1;
// Flush thresholds for the per-peer batch buffer. The normal flush is one reactor
// task-queue round later (`seastar::yield()`), which is what lets one tick's worth of
// group messages accumulate; these bound the buffer when a single round produces an
// unusual amount (e.g. a catch-up burst) so a frame never approaches the rpc frame limit.
constexpr size_t kMaxBatchBytes = 256 * 1024;
constexpr size_t kMaxBatchEnvelopes = 512;

// One envelope inside a received batch frame: where its bytes are (into the frame,
// which outlives the handler's awaits) and which group they address.
struct BatchRecord {
    const char* bytes;
    uint32_t len;
    uint16_t gid;
};

// How many per-group delivery chains a single batch frame may have in flight (debt
// D-15). Chains fan out to at most `smp::count` distinct shards, so the ceiling that
// buys anything is the shard count -- 16 covers every box this runs on while keeping
// the peak cross-shard tasks a single frame can spawn a fixed, small number rather
// than kMaxBatchEnvelopes. Beyond the shard count the extra concurrency only pipelines
// submit_to round trips into a shard that processes them serially anyway.
constexpr size_t kMaxConcurrentDeliverChains = 16;

// Fire-and-forget does not mean free: every unresolved seastar-rpc send owns a
// continuation and its encoded frame. A recovering 4096-group peer can otherwise
// enqueue thousands on one reactor before the socket drains and exhaust Seastar's
// shard memory. Raft already retries dropped messages, so bound both concurrent
// frames and their encoded bytes instead of turning network backpressure into an
// unbounded task queue. The byte bound is essential because one legal frame may be
// much larger than a heartbeat.
constexpr size_t kMaxInFlightSendFrames = 256;
constexpr size_t kMaxInFlightSendBytes = 64 * 1024 * 1024;

// The inbound admission bound moved to raft_types.hpp in D-37, next to the rest of the
// size chain it is the last link of -- a shard-level cap on concurrent snapshot transfers
// is derived from it, and that cap lives in a header. Its SEND-SIDE MIRROR stays here,
// because it is a property of this transport: seastar answers an over-limit `no_wait`
// request by dropping it with no reply, so a sender that exceeds the receiver's bound
// produces a silent, permanently-retried black hole. Refusing (and logging) on the send
// side turns that into a visible error naming the group.
constexpr size_t kMaxRaftMessageBytes = kMaxRaftSendBytes;  // refuse to SEND above this

// Raft message-rate instrumentation (write-scaleout 5-pre / 5a). The counters are
// unconditional -- they are a handful of increments on a path that already does an
// allocation and a syscall, and 5a's whole claim ("N envelopes now ride one frame") is
// only checkable if something counts both. TIMESTAR_RAFT_PROFILE=1 additionally LOGS them
// on a 5 s window, alongside the RaftGroup stage profiler.
//
// They live on the Impl, not in a thread_local: production runs one transport per shard,
// but a test runs several in one reactor, and a shared counter would conflate them.
bool transportProfileEnabled() {
    static const bool on = [] {
        const char* e = std::getenv("TIMESTAR_RAFT_PROFILE");
        return e && e[0] == '1';
    }();
    return on;
}

// How long to wait before re-attempting a peer connection that failed. seastar's
// rpc::client never reconnects itself, so we recreate it -- but a node runs
// thousands of Raft groups all ticking, so an unthrottled "recreate on every send"
// would hammer a genuinely-down peer with thousands of connects per second. Raft is
// retry-driven (the next tick re-sends), so dropping messages between attempts is
// safe; this only bounds how fast we retry.
//
// The window and its JITTER are now shared with the data plane
// (lib/cluster/reconnect_policy.hpp, write-scaleout 4b-i): un-jittered, every shard's
// client to a restarting peer re-dials on the same 200 ms grid, so a peer reboot is met
// by N_shards simultaneous connects each round -- and because they all fail together
// they stay in lockstep for the whole outage.
using timestar::cluster::kReconnectBackoff;

// TCP keepalive on Raft peer connections. The PARAMETERS live in reconnect_policy.hpp
// alongside the data plane's, so the two transports cannot drift apart -- the reasoning
// (a hibernated follower's connection is idle for long stretches, and a flow that dies
// while idle is otherwise only noticed when the next append vanishes into it) is
// identical and is recorded there.
seastar::rpc::client_options peerClientOptions() {
    seastar::rpc::client_options opts;
    opts.keepalive = timestar::cluster::keepaliveParams();
    return opts;
}

}  // namespace

struct RaftRpcTransport::Impl {
    using Client = seastar::rpc::protocol<RaftSerializer>::client;
    struct PeerEndpoint {
        seastar::socket_address address;
        std::string tlsServerName;
    };

    seastar::rpc::protocol<RaftSerializer> proto{RaftSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<RaftSerializer>::server> server;
    std::map<NodeId, PeerEndpoint> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    // Earliest time we may re-attempt a peer whose connection died (see kReconnectBackoff).
    std::map<NodeId, seastar::lowres_clock::time_point> nextRetry;
    DeliverFn onDeliver;
    seastar::gate gate;
    bool stopping = false;
    seastar::shared_ptr<seastar::tls::server_credentials> serverCreds;
    seastar::shared_ptr<seastar::tls::certificate_credentials> clientCreds;
    bool tlsEnabled = false;

    // The deliver stub is created ONCE. Allocating one per send (as this used to)
    // burns an allocation on every Raft message -- and a node ticking thousands of
    // groups sends a great many of them.
    RaftRpcTransport::DeliverRawFn onDeliverRaw;
    std::function<seastar::future<>(Client&, seastar::sstring)> deliverStub =
        proto.make_client<seastar::rpc::no_wait_type(seastar::sstring)>(kDeliverVerb);
    // --- write-scaleout 5a: per-peer outbound batching ---
    struct PendingFrames {
        std::vector<seastar::sstring> envelopes;
        size_t bytes = 0;
    };
    std::map<NodeId, PendingFrames> pending;
    // (group, peer) pairs whose oversized-message refusal has already been logged.
    std::set<std::pair<uint16_t, NodeId>> oversizeLogged;
    bool flushScheduled = false;
    RaftRpcTransport::Stats stats;
    size_t sendsInFlight = 0;
    size_t sendBytesInFlight = 0;
    seastar::lowres_clock::time_point windowStart{};
    RaftRpcTransport::Stats windowBase;

    // Log the rate over the last window, when the profile flag is set. Called from the
    // send/deliver paths so no timer is needed and an idle transport prints nothing.
    void maybeReport() {
        if (!transportProfileEnabled())
            return;
        constexpr auto kWindow = std::chrono::seconds(5);
        const auto now = seastar::lowres_clock::now();
        if (windowStart == seastar::lowres_clock::time_point{}) {
            windowStart = now;
            windowBase = stats;
            return;
        }
        if (now - windowStart < kWindow)
            return;
        const double secs = std::chrono::duration<double>(now - windowStart).count();
        const auto env = stats.envelopesSent - windowBase.envelopesSent;
        const auto frames = stats.framesSent - windowBase.framesSent;
        timestar::timestar_log.info(
            "[RAFT_MSGRATE] shard={} window={:.1f}s out_env/s={:.0f} out_frames/s={:.0f} "
            "env_per_frame={:.2f} out_MB/s={:.2f} in_env/s={:.0f} in_frames/s={:.0f} dropped/s={:.0f}",
            seastar::this_shard_id(), secs, env / secs, frames / secs,
            frames ? static_cast<double>(env) / static_cast<double>(frames) : 0.0,
            (stats.bytesSent - windowBase.bytesSent) / secs / 1e6,
            (stats.envelopesRecv - windowBase.envelopesRecv) / secs, (stats.framesRecv - windowBase.framesRecv) / secs,
            (stats.dropped - windowBase.dropped) / secs);
        windowStart = now;
        windowBase = stats;
    }

    void dispatchFrame(Client* conn, seastar::sstring data, size_t envelopeCount) {
        const size_t frameBytes = data.size();
        if (sendsInFlight >= kMaxInFlightSendFrames || frameBytes > kMaxInFlightSendBytes ||
            sendBytesInFlight > kMaxInFlightSendBytes - frameBytes) {
            stats.dropped += envelopeCount;
            stats.backpressured += envelopeCount;
            return;
        }
        ++sendsInFlight;
        sendBytesInFlight += frameBytes;
        stats.envelopesSent += envelopeCount;
        ++stats.framesSent;
        stats.bytesSent += frameBytes;
        maybeReport();
        try {
            (void)seastar::with_gate(gate, [this, conn, data = std::move(data), frameBytes]() mutable {
                return deliverStub(*conn, std::move(data))
                    .handle_exception([](std::exception_ptr) {})
                    .finally([this, frameBytes] {
                        --sendsInFlight;
                        sendBytesInFlight -= frameBytes;
                    });
            });
        } catch (...) {
            --sendsInFlight;
            sendBytesInFlight -= frameBytes;
            stats.dropped += envelopeCount;
        }
    }

    // Queue an encoded envelope for `to`, flushing eagerly if the buffer is already large.
    void enqueue(NodeId to, seastar::sstring data) {
        auto& p = pending[to];
        p.bytes += data.size();
        p.envelopes.push_back(std::move(data));
        if (p.bytes >= kMaxBatchBytes || p.envelopes.size() >= kMaxBatchEnvelopes) {
            flushPeer(to, p);
            return;
        }
        scheduleFlush();
    }

    // Flush at the END of the current task-queue round. `yield()` is what gives one
    // shard's tick -- which drains ~1000 groups -- a chance to fill the buffer before
    // anything goes on the wire, and it bounds the added latency to a single round.
    //
    // The continuation captures only `this` (the Impl, which outlives every task the gate
    // holds open) and is a PLAIN lambda, not a coroutine lambda: a coroutine lambda's
    // captures live in the closure object, which dies at the end of the full expression
    // while the coroutine is still suspended.
    void scheduleFlush() {
        if (flushScheduled || gate.is_closed())
            return;
        flushScheduled = true;
        (void)seastar::with_gate(gate, [this] {
            return seastar::yield().then([this] {
                flushScheduled = false;
                flushAll();
            });
        });
    }

    void flushAll() {
        for (auto& [to, p] : pending) {
            if (!p.envelopes.empty())
                flushPeer(to, p);
        }
    }

    // Dispatch everything buffered for one peer. Does not suspend, so the buffer cannot
    // be mutated underneath it.
    void flushPeer(NodeId to, PendingFrames& p) {
        std::vector<seastar::sstring> envelopes;
        envelopes.swap(p.envelopes);
        p.bytes = 0;
        if (stopping || gate.is_closed())
            return;
        auto* conn = clientFor(to);
        if (!conn) {
            stats.dropped += envelopes.size();
            return;  // unknown/backing-off peer: drop (Raft retries)
        }
        size_t total = 0;
        for (const auto& e : envelopes)
            total += e.size() + sizeof(uint32_t);
        seastar::sstring frame = seastar::uninitialized_string(total);
        char* out = frame.data();
        for (const auto& e : envelopes) {
            const uint32_t n = static_cast<uint32_t>(e.size());
            std::memcpy(out, &n, sizeof(n));
            out += sizeof(n);
            std::memcpy(out, e.data(), e.size());
            out += e.size();
        }
        dispatchFrame(conn, std::move(frame), envelopes.size());
    }

    // Retire a dead connection without blocking the caller: stop it in the background
    // (under the gate) so its resources are released, keeping it alive until stop
    // resolves.
    void retire(std::unique_ptr<Client> dead) {
        if (!dead || gate.is_closed())
            return;  // shutting down: stop() will not run; just drop it
        auto* raw = dead.get();
        (void)seastar::with_gate(gate, [raw, d = std::move(dead)]() mutable {
            return raw->stop().handle_exception([](std::exception_ptr) {}).finally([d = std::move(d)] {});
        });
    }

    // Open (and cache) the one connection to a peer host, RECONNECTING a dead one.
    //
    // seastar's rpc::client connects exactly once, in the background, and latches
    // error() on failure -- it never reconnects. Raft starts sending the instant the
    // groups tick, which during a rolling start is BEFORE the peers are listening, so
    // the first connection attempt to a not-yet-up peer fails and the cached client is
    // permanently dead. Every later heartbeat/vote to that peer is silently discarded,
    // so its groups never elect a leader (the cluster reports every VShard leaderless
    // forever). Detect the dead client, retire it, and build a fresh one -- throttled
    // by kReconnectBackoff so a genuinely-down peer is not hammered.
    Client* clientFor(NodeId to) {
        auto it = clients.find(to);
        if (it != clients.end()) {
            if (!it->second->error())
                return it->second.get();
            const auto now = seastar::lowres_clock::now();
            auto rit = nextRetry.find(to);
            if (rit != nextRetry.end() && now < rit->second)
                return nullptr;  // backing off: drop this message, Raft re-sends
            nextRetry[to] = timestar::cluster::nextReconnectAt(now);
            retire(std::move(it->second));
            clients.erase(it);
        }
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        std::unique_ptr<Client> c;
        const auto opts = peerClientOptions();
        if (tlsEnabled) {
            seastar::tls::tls_options tlsOpts;
            tlsOpts.server_name = seastar::sstring(pit->second.tlsServerName);
            c = std::make_unique<Client>(proto, opts, seastar::tls::socket(clientCreds, tlsOpts),
                                         pit->second.address);
        } else {
            c = std::make_unique<Client>(proto, opts, pit->second.address);
        }
        auto* p = c.get();
        clients[to] = std::move(c);
        return p;
    }
};

RaftRpcTransport::RaftRpcTransport() : impl_(std::make_unique<Impl>()) {}
RaftRpcTransport::~RaftRpcTransport() = default;

void RaftRpcTransport::addPeer(NodeId id, seastar::socket_address addr, std::string tlsServerName) {
    if (impl_->tlsEnabled && tlsServerName.empty())
        throw std::invalid_argument("RaftRpcTransport: TLS peer server name must not be empty");
    auto existing = impl_->peers.find(id);
    if (existing != impl_->peers.end() && existing->second.address == addr &&
        existing->second.tlsServerName == tlsServerName)
        return;
    if (auto client = impl_->clients.find(id); client != impl_->clients.end()) {
        impl_->retire(std::move(client->second));
        impl_->clients.erase(client);
    }
    impl_->nextRetry.erase(id);
    impl_->peers.insert_or_assign(id, Impl::PeerEndpoint{addr, std::move(tlsServerName)});
}

void RaftRpcTransport::setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem) {
    if (impl_->server)
        throw std::logic_error("RaftRpcTransport TLS must be configured before start");
    if (std::ranges::any_of(impl_->peers, [](const auto& peer) { return peer.second.tlsServerName.empty(); }))
        throw std::logic_error("RaftRpcTransport: TLS requires a server name for every registered peer");
    seastar::tls::credentials_builder builder;
    builder.set_x509_trust(seastar::tls::blob(caPem.data(), caPem.size()), seastar::tls::x509_crt_format::PEM);
    builder.set_x509_key(seastar::tls::blob(certPem.data(), certPem.size()),
                         seastar::tls::blob(keyPem.data(), keyPem.size()), seastar::tls::x509_crt_format::PEM);
    builder.set_client_auth(seastar::tls::client_auth::REQUIRE);
    impl_->serverCreds = builder.build_server_credentials();
    impl_->clientCreds = builder.build_certificate_credentials();
    impl_->tlsEnabled = true;
}

seastar::future<> RaftRpcTransport::start(seastar::socket_address local, DeliverFn onDeliver, bool perShardListener) {
    impl_->onDeliver = std::move(onDeliver);
    // A no_wait handler: the sender never awaits a reply (Raft is fire-and-forget
    // and retries via heartbeats), so a send to a slow/dead peer can never block
    // the Ready loop or shutdown. The handler body still runs to completion.
    // The v1 frame is [u32 len][TSR1 envelope] repeated. Each envelope is routed by
    // the group id peeked after its v1 marker and decoded on the owning shard.
    //
    // A malformed frame (truncated length, length past the end) STOPS at the bad record
    // and delivers everything before it. Raft re-sends what a peer did not acknowledge, so
    // dropping a suffix is safe; guessing at the rest is not.
    //
    // DISPATCH IS CONCURRENT ACROSS GROUPS AND SEQUENTIAL WITHIN ONE (debt D-15). The
    // first version of this handler `co_await`ed every envelope in turn, so a full frame
    // was up to kMaxBatchEnvelopes SEQUENTIAL cross-shard hops inside one handler --
    // each one a submit_to round trip whose latency the next envelope paid for. That is
    // the opposite of what batching is for.
    //
    // ORDERING IS THE CONSTRAINT, and it is per GROUP, not per frame. Raft messages
    // within a group are order-sensitive (an AppendEntries and the heartbeat behind it
    // must not swap); messages for DIFFERENT groups are wholly independent -- they land
    // in different RaftNodes, often on different shards. So the frame is partitioned
    // into one chain per group id, each chain delivered strictly in frame order, and the
    // chains run concurrently under a bound. Since group -> shard is a function, a
    // per-group partition is a refinement of a per-shard one: no two chains can
    // interleave deliveries into the same node.
    impl_->proto.register_handler(
        kDeliverVerb, [this](seastar::sstring data) -> seastar::future<seastar::rpc::no_wait_type> {
            ++impl_->stats.framesRecv;
            impl_->maybeReport();

            // Pass 1: walk the frame once, recording each envelope's extent. Pointers
            // are into `data`, which lives in this coroutine's frame and outlives every
            // await below.
            std::vector<BatchRecord> recs;
            size_t off = 0;
            size_t recordsSeen = 0;
            while (recordsSeen < kMaxBatchEnvelopes && off + sizeof(uint32_t) <= data.size()) {
                uint32_t n = 0;
                std::memcpy(&n, data.data() + off, sizeof(n));
                off += sizeof(n);
                if (n > data.size() - off)
                    break;  // truncated record: deliver the prefix, drop the rest
                const char* bytes = data.data() + off;
                off += n;
                ++recordsSeen;
                ++impl_->stats.envelopesRecv;
                if (n < kRaftEnvelopeV1MagicBytes + 2)
                    continue;
                const uint16_t gid =
                    static_cast<uint16_t>(static_cast<unsigned char>(bytes[kRaftEnvelopeV1MagicBytes])) |
                    static_cast<uint16_t>(
                        static_cast<unsigned char>(bytes[kRaftEnvelopeV1MagicBytes + 1]) << 8);
                recs.push_back(BatchRecord{bytes, n, gid});
            }
            if (recs.empty())
                co_return seastar::rpc::no_wait;

            // The decode-here fallback (no raw hook installed) stays SEQUENTIAL. It does
            // no cross-shard hop -- it decodes and delivers on this shard -- so there is
            // no round-trip latency to hide, and keeping it serial keeps whole-frame
            // order for the callers that rely on it.
            if (!impl_->onDeliverRaw) {
                for (const auto& r : recs) {
                    auto env = decodeEnvelope(std::string(r.bytes, r.len));
                    if (env && impl_->onDeliver)
                        co_await impl_->onDeliver(std::move(*env));
                }
                co_return seastar::rpc::no_wait;
            }

            // Pass 2: partition into per-group chains. A STABLE sort by group id makes
            // each group's records contiguous while preserving their frame order within
            // the group -- which is exactly the ordering guarantee owed. O(E log E) on at
            // most kMaxBatchEnvelopes records; a linear group-map probe would be O(E^2)
            // on the common frame where nearly every envelope is a different group.
            std::stable_sort(recs.begin(), recs.end(),
                             [](const BatchRecord& a, const BatchRecord& b) { return a.gid < b.gid; });
            std::vector<std::pair<size_t, size_t>> chains;  // [begin, end) runs of one gid
            for (size_t i = 0; i < recs.size();) {
                size_t j = i + 1;
                while (j < recs.size() && recs[j].gid == recs[i].gid)
                    ++j;
                chains.emplace_back(i, j);
                i = j;
            }

            // `recs` and `chains` are frame locals and the loop is awaited, so both
            // outlive every reference the chain bodies take.
            co_await seastar::max_concurrent_for_each(
                chains, kMaxConcurrentDeliverChains,
                [this, &recs](const std::pair<size_t, size_t>& run) -> seastar::future<> {
                    for (size_t i = run.first; i < run.second; ++i)
                        co_await impl_->onDeliverRaw(recs[i].gid, recs[i].bytes, recs[i].len);
                });
            co_return seastar::rpc::no_wait;
        });
    // Where inbound connections are accepted.
    //
    // `perShardListener == false` (a single instance, on one shard) PINS the listening
    // socket to THIS shard. The Raft server and its group registry live on one shard,
    // but seastar's default accept policy scatters incoming connections across ALL
    // shards -- a connection accepted on a shard with no server behind it has its
    // messages silently discarded forever.
    //
    // This verb is no_wait, so the SENDER never hangs; the failure is invisible and
    // ASYMMETRIC. Observed: node 1 (leader of all 4096 groups) saw node 2 caught up on
    // 4096 and node 3 caught up on ZERO -- node 3's append acks never arrived, so
    // matchIndex_[3] stayed 0 and leadership transfer to node 3 could never fire its
    // TimeoutNow (RaftNode::transferLeadership only sends it to a caught-up target).
    // Which peer is affected is per-connection luck, which is why it looks like one
    // "bad" node.
    //
    // `perShardListener == true` means EVERY shard has started an instance on this
    // address (ShardRaftPlane), so any shard can answer and pinning is exactly wrong:
    // reuseport is disabled in this seastar, so shard 0 owns the one real socket and
    // set_fixed_cpu(this_shard_id()) makes it hand EVERY accepted fd to shard 0 --
    // shards 1..N sit on accept promises that never resolve while shard 0 reads all
    // inbound Raft traffic and runs every peek/route hop. connection_distribution
    // spreads the accepted fds instead. Safe for any handler here because the raw
    // deliver path already routes each envelope to the shard owning its group (the
    // group id is peeked from the frame), and this transport registers exactly one
    // verb, which is no_wait -- there is no reply routing to get wrong.
    seastar::listen_options lo;
    lo.reuse_address = true;
    if (perShardListener)
        lo.lba = seastar::server_socket::load_balancing_algorithm::connection_distribution;
    else
        lo.set_fixed_cpu(seastar::this_shard_id());
    // Bound inbound admission. Without resource_limits seastar admits an unbounded
    // amount of in-flight request memory, so a peer (mTLS is optional on this
    // transport) can spend this node's memory for it.
    //
    // TIGHTENED from 1 GiB in write-scaleout 5.4, and what made that possible was capping
    // the producer that needed the slack: a catch-up AppendEntries used to carry
    // `log_.entriesFrom(nextIndex)`, the WHOLE log tail in one message, so after a big
    // write campaign a returning follower's first append could be arbitrarily large.
    // Since the deliver verb is `no_wait`, an over-limit message is DROPPED WITH NO REPLY
    // -- the follower would retry the same oversized append forever and never catch up,
    // silently -- so the bound had to exceed anything the leader might produce.
    // RaftOptions::maxAppendEntries/maxAppendBytes now bound it (1 MiB of entries), and
    // the batch frames 5a introduces are capped at 256 KB.
    //
    // TIGHTENED AGAIN, from 128 MiB to 64 MiB, by D-5 -- for the same reason and by the
    // same method. InstallSnapshot was the last unbounded producer (a whole VShard
    // snapshot in one message); it is now chunked at `kMaxSnapshotChunkBytes` with
    // receiver-side staging and a resumable offset, so this bound is sized for the biggest
    // APPEND rather than for the biggest snapshot. `kMaxRaftMessageBytes` above is the
    // send-side mirror of it: a payload that would be dropped is logged as an error rather
    // than vanishing.
    //
    // bloat_factor stays 1: entry bytes are persisted roughly 1:1 here. The amplifying
    // WriteBatch decode happens later, at APPLY, which is behind commit -- i.e. behind
    // the Raft protocol itself, not reachable by an unsolicited frame.
    seastar::rpc::resource_limits lim;
    lim.max_memory = kMaxInboundRaftMemory;
    seastar::server_socket listener =
        impl_->tlsEnabled ? seastar::tls::listen(impl_->serverCreds, local, lo) : seastar::listen(local, lo);
    impl_->server = std::make_unique<seastar::rpc::protocol<RaftSerializer>::server>(impl_->proto,
                                                                                    std::move(listener), lim);
    return seastar::make_ready_future<>();
}

seastar::future<> RaftRpcTransport::send(Envelope env) {
    if (impl_->stopping || impl_->gate.is_closed())
        return seastar::make_ready_future<>();
    // Encode now and buffer through the end of this task-queue round.
    // Buffering is what lets one shard's tick -- which drains up to ~1000 groups -- put
    // many groups' messages into one frame to the same peer. Fire-and-forget semantics
    // are identical either way: the Ready loop is never blocked on the network, and a
    // dropped message is a Raft retry.
    std::string bytes = encodeEnvelope(env);
    // Refuse, LOUDLY, what the peer's admission bound would drop silently (5.4). The
    // deliver verb is no_wait: an over-limit frame gets no reply, so the sender would keep
    // re-sending it forever and the group would simply never make progress with that peer.
    // The only producer that can reach this today is InstallSnapshot (appends are capped
    // by RaftOptions); an operator seeing this needs to know a VShard snapshot is too big
    // to replicate, not to watch a follower hang.
    if (bytes.size() > kMaxRaftMessageBytes) {
        ++impl_->stats.dropped;
        // LATCHED PER (group, peer). Raft retries on every heartbeat, so an undeliverable
        // message is not a one-off event -- unlatched, this logs once per round trip for
        // as long as the condition lasts, which buries the node's log in the one message
        // an operator most needs to find. The condition is a property of the pair, not of
        // the moment, so saying it once per pair is saying it exactly as often as it is
        // true (write-scaleout 5 review, F3a).
        if (impl_->oversizeLogged.emplace(env.groupId, env.message.to).second) {
            timestar::timestar_log.error(
                "[RAFT] refusing to send a {} byte message for group {} to node {}: over the {} byte peer admission "
                "bound (an InstallSnapshot this large cannot be delivered; chunked snapshot streaming is the fix). "
                "Further refusals for this group/peer are not logged.",
                bytes.size(), env.groupId, env.message.to, kMaxRaftMessageBytes);
        }
        return seastar::make_ready_future<>();
    }
    impl_->enqueue(env.message.to, seastar::sstring(bytes.data(), bytes.size()));
    return seastar::make_ready_future<>();
}

RaftRpcTransport::Stats RaftRpcTransport::stats() const {
    return impl_->stats;
}

void RaftRpcTransport::setRawDeliver(DeliverRawFn onDeliverRaw) {
    impl_->onDeliverRaw = std::move(onDeliverRaw);
}

seastar::future<> RaftRpcTransport::stop() {
    if (impl_->stopping)
        co_return;
    // Block new sends first, then ABORT in-flight ones by stopping the peer
    // clients -- otherwise a background send stuck reconnecting to an
    // already-stopped peer would keep the gate from ever closing. Only then is it
    // safe to close the gate (no task still references a client) and clear them.
    impl_->stopping = true;
    // Anything still buffered is a Raft message that will simply be re-sent by whoever
    // still needs it; there is nothing to flush on the way out (and flushing would open
    // new background sends just as we are trying to drain them).
    impl_->pending.clear();
    for (auto& [id, c] : impl_->clients)
        co_await c->stop();
    co_await impl_->gate.close();
    if (impl_->server)
        co_await impl_->server->stop();
    impl_->clients.clear();
    co_return;
}

}  // namespace timestar::raft

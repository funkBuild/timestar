#include "raft_rpc_transport.hpp"

#include "../../utils/logger.hpp"
#include "../reconnect_policy.hpp"
#include "raft_codec.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/later.hh>
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

// ...and a bare u32, for the capability probe's reply. Every type crossing this
// transport needs its own write/read here; there is no default.
template <typename Output>
void write(RaftSerializer, Output& out, uint32_t v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template <typename Input>
uint32_t read(RaftSerializer, Input& in, seastar::rpc::type<uint32_t>) {
    uint32_t v = 0;
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    return v;
}

constexpr uint64_t kDeliverVerb = 1;
// MULTI-ENVELOPE FRAME (write-scaleout 5a). Payload is a concatenation of
// [u32 len LE][envelope bytes] records; the receiver routes each by group id exactly as
// the single-envelope path does. A node ticking ~1000 groups per shard sends thousands of
// separate RPCs per second to the same peer, each with its own frame header, its own
// resource permit and its own write; a tick that produces messages for many groups can
// carry them in one.
constexpr uint64_t kDeliverBatchVerb = 2;
// CAPABILITY PROBE. This transport has NO version negotiation -- the single deliver verb
// has been the whole protocol -- and an unknown verb on seastar's rpc is answered with an
// unknown-verb reply that a no_wait sender IGNORES. So an old peer sent a batch frame
// would silently discard every Raft message in it: no error, no reply, just a node whose
// groups never hear from their leader. The sender therefore has to KNOW before it batches.
//
// A waited verb is the mechanism, because only a reply can distinguish "peer understands
// this" from "peer dropped it": a new peer answers with its capability bits, an old peer
// fails the call with unknown_verb_error, and either way the sender learns the truth
// before the first batch frame. Fail-closed: unknown or failed probe == no batching, and
// the probe is redone for every new connection (a peer can be RESTARTED into an older
// binary on the same address).
constexpr uint64_t kCapabilitiesVerb = 3;
constexpr uint32_t kCapBatchedDeliver = 1u << 0;

// Flush thresholds for the per-peer batch buffer. The normal flush is one reactor
// task-queue round later (`seastar::yield()`), which is what lets one tick's worth of
// group messages accumulate; these bound the buffer when a single round produces an
// unusual amount (e.g. a catch-up burst) so a frame never approaches the rpc frame limit.
constexpr size_t kMaxBatchBytes = 256 * 1024;
constexpr size_t kMaxBatchEnvelopes = 512;

// Inbound admission bound for this transport, and its SEND-SIDE MIRROR (write-scaleout
// 5.4). They are one pair of constants on purpose: seastar answers an over-limit `no_wait`
// request by dropping it with no reply, so a sender that exceeds the receiver's bound
// produces a silent, permanently-retried black hole. Refusing (and logging) on the send
// side turns that into a visible error naming the group.
//
// The number is sized for the one producer still unbounded -- InstallSnapshot, which
// carries a whole VShard snapshot -- not for appends, which 5.4 caps at 1 MiB of entries.
constexpr size_t kMaxInboundRaftMemory = size_t{128} << 20;  // 128 MiB in flight
constexpr size_t kMaxRaftMessageBytes = kMaxRaftSendBytes;   // refuse to SEND above this

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

    seastar::rpc::protocol<RaftSerializer> proto{RaftSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<RaftSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    // Earliest time we may re-attempt a peer whose connection died (see kReconnectBackoff).
    std::map<NodeId, seastar::lowres_clock::time_point> nextRetry;
    DeliverFn onDeliver;
    seastar::gate gate;
    bool stopping = false;

    // The deliver stub is created ONCE. Allocating one per send (as this used to)
    // burns an allocation on every Raft message -- and a node ticking thousands of
    // groups sends a great many of them.
    RaftRpcTransport::DeliverRawFn onDeliverRaw;
    std::function<seastar::future<>(Client&, seastar::sstring)> deliverStub =
        proto.make_client<seastar::rpc::no_wait_type(seastar::sstring)>(kDeliverVerb);
    std::function<seastar::future<>(Client&, seastar::sstring)> deliverBatchStub =
        proto.make_client<seastar::rpc::no_wait_type(seastar::sstring)>(kDeliverBatchVerb);
    std::function<seastar::future<uint32_t>(Client&)> capabilitiesStub =
        proto.make_client<uint32_t()>(kCapabilitiesVerb);

    // --- write-scaleout 5a: per-peer outbound batching ---
    struct PeerCaps {
        // Which connection these bits describe. clientFor() bumps the generation when it
        // builds a new client, so a probe reply that lands after a reconnect is ignored
        // rather than applied to a connection it never spoke to (a peer can restart into
        // an older binary on the same address).
        uint64_t generation = 0;
        bool known = false;
        uint32_t bits = 0;
    };
    struct PendingFrames {
        std::vector<seastar::sstring> envelopes;
        size_t bytes = 0;
    };
    std::map<NodeId, PeerCaps> caps;
    std::map<NodeId, PendingFrames> pending;
    // (group, peer) pairs whose oversized-message refusal has already been logged.
    std::set<std::pair<uint16_t, NodeId>> oversizeLogged;
    uint64_t nextGeneration = 1;
    bool flushScheduled = false;
    // OFF BY DEFAULT, and the measurement that made it so is recorded in
    // docs/write-scaleout-plan.md Phase 5. Batching does what it says -- it cut idle RPC
    // frames per shard from 2724/s to ~700/s -- but on this cluster that is not a
    // resource under pressure, and the buffering it costs IS: measured against the
    // pre-Phase-5 binary on the same box, minutes apart, idle CPU went 4% -> 7-8% per
    // node and canonical-bench median throughput 5.16 -> 4.90 M pts/s.
    //
    // That is the Phase-2 lesson restated: removing work from a system that is ~80% CPU
    // idle cannot raise its throughput, but ADDING per-message work (a buffer insert, a
    // gate hold and a `yield()`-scheduled flush task per round) can lower it. The
    // capability, its wire format and its tests stay; the default does not.
    //
    // TIMESTAR_RAFT_BATCH_SENDS=1 turns it on for a deployment where the frame rate is
    // the binding cost (many more peers, or a NIC/syscall-bound node) -- and where it is
    // on, `send()` takes the buffered path; where it is off, `send()` dispatches
    // immediately exactly as it did before 5a, so the default costs nothing at all.
    bool batchingEnabled = [] {
        const char* e = std::getenv("TIMESTAR_RAFT_BATCH_SENDS");
        return e && e[0] == '1';
    }();

    RaftRpcTransport::Stats stats;
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

    bool peerSupportsBatch(NodeId to) const {
        auto it = caps.find(to);
        return it != caps.end() && it->second.known && (it->second.bits & kCapBatchedDeliver) != 0;
    }

    // Ask a freshly-built connection what it understands. Fail-closed on ANY error --
    // unknown verb (an old peer), a dead connection, a timeout -- because "not known to
    // support batching" and "known not to" must behave identically.
    void probeCapabilities(NodeId to, Client* conn, uint64_t generation) {
        if (gate.is_closed())
            return;
        (void)seastar::with_gate(gate, [this, to, conn, generation] {
            return capabilitiesStub(*conn).then_wrapped([this, to, generation](seastar::future<uint32_t> f) {
                uint32_t bits = 0;
                if (f.failed())
                    f.ignore_ready_future();
                else
                    bits = f.get();
                auto it = caps.find(to);
                if (it != caps.end() && it->second.generation == generation) {
                    it->second.known = true;
                    it->second.bits = bits;
                }
            });
        });
    }

    // Dispatch one envelope straight to the peer -- the pre-5a path, and the default.
    void sendNow(NodeId to, seastar::sstring data);

    // Queue an encoded envelope for `to`, flushing eagerly if the buffer is already large.
    void enqueue(NodeId to, seastar::sstring data) {
        if (!batchingEnabled) {
            sendNow(to, std::move(data));  // pre-5a path: no buffer, no deferred flush
            return;
        }
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
        const bool batch = batchingEnabled && envelopes.size() > 1 && peerSupportsBatch(to);
        if (batch) {
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
            stats.envelopesSent += envelopes.size();
            ++stats.framesSent;
            stats.bytesSent += frame.size();
            maybeReport();
            (void)seastar::with_gate(gate, [this, conn, frame = std::move(frame)]() mutable {
                return deliverBatchStub(*conn, std::move(frame)).handle_exception([](std::exception_ptr) {});
            });
            return;
        }
        for (auto& e : envelopes) {
            ++stats.envelopesSent;
            ++stats.framesSent;
            stats.bytesSent += e.size();
            maybeReport();
            (void)seastar::with_gate(gate, [this, conn, data = std::move(e)]() mutable {
                return deliverStub(*conn, std::move(data)).handle_exception([](std::exception_ptr) {});
            });
        }
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
        auto c = std::make_unique<Client>(proto, peerClientOptions(), pit->second);
        auto* p = c.get();
        clients[to] = std::move(c);
        // A NEW connection knows nothing until it answers: reset to "unknown" (which is
        // "no batching") and probe. Never carry the old connection's bits forward.
        auto& cap = caps[to];
        cap.generation = nextGeneration++;
        cap.known = false;
        cap.bits = 0;
        probeCapabilities(to, p, cap.generation);
        return p;
    }
};

RaftRpcTransport::RaftRpcTransport() : impl_(std::make_unique<Impl>()) {}
RaftRpcTransport::~RaftRpcTransport() = default;

void RaftRpcTransport::addPeer(NodeId id, seastar::socket_address addr) {
    impl_->peers[id] = addr;
}

seastar::future<> RaftRpcTransport::start(seastar::socket_address local, DeliverFn onDeliver, bool perShardListener) {
    impl_->onDeliver = std::move(onDeliver);
    // A no_wait handler: the sender never awaits a reply (Raft is fire-and-forget
    // and retries via heartbeats), so a send to a slow/dead peer can never block
    // the Ready loop or shutdown. The handler body still runs to completion.
    impl_->proto.register_handler(
        kDeliverVerb, [this](seastar::sstring data) -> seastar::future<seastar::rpc::no_wait_type> {
            // Fast path: route by group id without decoding, so the (potentially
            // 100s of KB) AppendEntries payload is decoded on the shard that owns
            // the group rather than on this single listening shard. groupId is the
            // first field encodeEnvelope writes: u16, little-endian, at offset 0.
            ++impl_->stats.framesRecv;
            ++impl_->stats.envelopesRecv;
            impl_->maybeReport();
            if (impl_->onDeliverRaw) {
                if (data.size() < 2)
                    co_return seastar::rpc::no_wait;
                const uint16_t gid = static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
                                     static_cast<uint16_t>(static_cast<unsigned char>(data[1]) << 8);
                // `data` outlives the call: we await before it is destroyed.
                co_await impl_->onDeliverRaw(gid, data.data(), data.size());
                co_return seastar::rpc::no_wait;
            }
            auto env = decodeEnvelope(std::string(data.data(), data.size()));
            if (env && impl_->onDeliver)
                co_await impl_->onDeliver(std::move(*env));
            co_return seastar::rpc::no_wait;
        });
    // Multi-envelope frame (write-scaleout 5a): [u32 len][envelope] repeated. Each
    // envelope takes exactly the same route as a single-envelope frame -- routed by the
    // group id peeked from its first two bytes, decoded on the owning shard -- so the two
    // verbs cannot diverge in how a message is handled, only in how it arrived.
    //
    // A malformed frame (truncated length, length past the end) STOPS at the bad record
    // and delivers everything before it. Raft re-sends what a peer did not acknowledge, so
    // dropping a suffix is safe; guessing at the rest is not.
    impl_->proto.register_handler(
        kDeliverBatchVerb, [this](seastar::sstring data) -> seastar::future<seastar::rpc::no_wait_type> {
            ++impl_->stats.framesRecv;
            impl_->maybeReport();
            size_t off = 0;
            while (off + sizeof(uint32_t) <= data.size()) {
                uint32_t n = 0;
                std::memcpy(&n, data.data() + off, sizeof(n));
                off += sizeof(n);
                if (n > data.size() - off)
                    break;  // truncated record: deliver the prefix, drop the rest
                const char* bytes = data.data() + off;
                off += n;
                ++impl_->stats.envelopesRecv;
                if (n < 2)
                    continue;
                if (impl_->onDeliverRaw) {
                    const uint16_t gid = static_cast<uint16_t>(static_cast<unsigned char>(bytes[0])) |
                                         static_cast<uint16_t>(static_cast<unsigned char>(bytes[1]) << 8);
                    // `data` outlives every await here, so the pointer stays valid.
                    co_await impl_->onDeliverRaw(gid, bytes, n);
                } else {
                    auto env = decodeEnvelope(std::string(bytes, n));
                    if (env && impl_->onDeliver)
                        co_await impl_->onDeliver(std::move(*env));
                }
            }
            co_return seastar::rpc::no_wait;
        });
    // The capability probe. Stateless and constant, so it does not matter which shard's
    // server instance answers it under connection_distribution.
    impl_->proto.register_handler(kCapabilitiesVerb,
                                  [] { return seastar::make_ready_future<uint32_t>(kCapBatchedDeliver); });
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
    // What is STILL unbounded is InstallSnapshot: it carries an entire VShard snapshot
    // payload (TSM bytes) in one message, and chunking it is a protocol change (offset +
    // done, receiver-side reassembly, a resumable boundary) rather than a producer cap.
    // So this bound is sized for SNAPSHOTS, not for appends, and `kMaxRaftMessageBytes`
    // below is the send-side mirror of it: a payload that would be dropped is logged as
    // an error rather than vanishing.
    //
    // bloat_factor stays 1: entry bytes are persisted roughly 1:1 here. The amplifying
    // WriteBatch decode happens later, at APPLY, which is behind commit -- i.e. behind
    // the Raft protocol itself, not reachable by an unsolicited frame.
    seastar::rpc::resource_limits lim;
    lim.max_memory = kMaxInboundRaftMemory;
    impl_->server =
        std::make_unique<seastar::rpc::protocol<RaftSerializer>::server>(impl_->proto, seastar::listen(local, lo), lim);
    return seastar::make_ready_future<>();
}

seastar::future<> RaftRpcTransport::send(Envelope env) {
    if (impl_->stopping || impl_->gate.is_closed())
        return seastar::make_ready_future<>();
    // Encode now, and either dispatch immediately (the default) or buffer for the end of
    // this task-queue round (write-scaleout 5a, opt-in -- see `batchingEnabled`).
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

void RaftRpcTransport::Impl::sendNow(NodeId to, seastar::sstring data) {
    auto* conn = clientFor(to);
    if (!conn) {
        ++stats.dropped;
        return;  // unknown/backing-off peer: drop (Raft retries)
    }
    ++stats.envelopesSent;
    ++stats.framesSent;
    stats.bytesSent += data.size();
    maybeReport();
    (void)seastar::with_gate(gate, [this, conn, data = std::move(data)]() mutable {
        return deliverStub(*conn, std::move(data)).handle_exception([](std::exception_ptr) {});
    });
}

RaftRpcTransport::Stats RaftRpcTransport::stats() const {
    return impl_->stats;
}

void RaftRpcTransport::setBatchingEnabled(bool enabled) {
    impl_->batchingEnabled = enabled;
}

void RaftRpcTransport::setRawDeliver(DeliverRawFn onDeliverRaw) {
    impl_->onDeliverRaw = std::move(onDeliverRaw);
}

seastar::future<> RaftRpcTransport::stop() {
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

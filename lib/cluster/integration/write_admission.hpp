#pragma once

#include "../../utils/logger.hpp"
#include "../data/write_errors.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <system_error>

namespace timestar::cluster {

// Bounded in-flight write bytes, PER SHARD, on the cluster data plane
// (write-scaleout 3d).
//
// Why it is needed now: Phase 1a made the scatter concurrent, so every shard slice of
// every in-flight batch is alive at once instead of one at a time, and Phase 3b's retry
// holds a failed slice for the length of its retry window instead of dropping it. Both
// raise peak resident bytes on the write path, and until now nothing bounded them -- the
// only limit was how fast clients could push, which is exactly the shape that ends in an
// OOM kill or in the timeout storms that look like the [D6] collapse.
//
// It is deliberately a REJECTION, not a queue and not a wait. The single-node ingest path
// already settled this argument (IngestBacklogException -> 503 + Retry-After, see
// http_write_handler.cpp): a client that honours Retry-After converges on the sustainable
// rate on its own, whereas silently accumulating past the bound exhausts memory, and
// blocking turns a memory problem into a latency problem that presents as a hang. The
// wire-level analogue on the inbound side is rpc::resource_limits (dataplane_rpc.cpp);
// this is the same idea applied to what THIS node originates.
//
// Scope: the counter is shard-local (each reactor charges the batches it coordinates), so
// there is no cross-core atomic on the write path and the bound scales with core count
// exactly as the work does.
//
// TWO BUDGETS, NOT ONE (debt D-8). A node's write memory arrives through two doors and
// they are bounded SEPARATELY:
//
//   Originated  -- what a client asked THIS node to write (`writeSlicesToOwningShards`),
//                  charged on the request shard.
//   PeerIngress -- what a peer forwarded here because we lead those VShards
//                  (`proposeSlicesToOwningShards{,Hinted}`), charged on the SERVING shard
//                  (the one the connection landed on, which holds the decoded batch and
//                  the fan-out frame). On a balanced 3-node RF=3 cluster this is ~2/3 of
//                  all replication traffic and used to be bounded only by
//                  `rpc::resource_limits`, which caps one frame and total estimated RPC
//                  memory -- not this node's write pipeline.
//
// WHY SEPARATE RATHER THAN SHARED, which was the real decision here. A shared counter
// makes the two roles compete for one number, and they are not interchangeable: a burst
// of peer ingress would 503 a client's own writes on a node that is coordinating almost
// nothing (and the reverse -- a busy coordinator turning away replication it is the
// LEADER for, which its peers can only retry back at it). That is coordinator-vs-replica
// starvation, and which side loses depends on arrival order, i.e. it is unpredictable.
// Separate budgets make each role's headroom a property of that role: an operator can
// reason "this node will hold at most (originated + ingress) x shards", and neither role
// can be starved by the other. The cost is that the node-wide ceiling is the SUM, so the
// default is stated per class rather than pretending 32 MiB is a node bound.
enum class AdmissionClass : uint8_t {
    Originated = 0,  // this node is the coordinator: a client's write
    PeerIngress = 1  // a peer forwarded it: we lead those VShards
};

class WriteAdmission {
public:
    // The per-shard, PER-CLASS budget. Default 32 MiB -- several times what the canonical
    // RF=3 bench holds in flight per shard (~2 MB: 8 connections x one ~10k-point batch,
    // held for one ~90 ms quorum round), so it never trips on healthy load, while still
    // bounding a pathological client to something a node can hold. The same default is
    // used for peer ingress: a node's replication traffic is the same batches arriving
    // from its peers, so the shape and the headroom argument are identical.
    //
    // Overridable per class, so the backpressure gate can drive the ORIGINATED budget
    // deliberately without also squeezing replication (and vice versa):
    //   TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES    -- originated
    //   TIMESTAR_CLUSTER_INGRESS_INFLIGHT_BYTES  -- peer ingress
    static constexpr size_t kDefaultLimitBytes = 32u << 20;

    static size_t limitBytes(AdmissionClass cls = AdmissionClass::Originated) {
        return cls == AdmissionClass::PeerIngress ? ingressLimitBytes() : originatedLimitBytes();
    }

    static WriteAdmission& local(AdmissionClass cls = AdmissionClass::Originated) {
        static thread_local WriteAdmission originated{AdmissionClass::Originated};
        static thread_local WriteAdmission ingress{AdmissionClass::PeerIngress};
        return cls == AdmissionClass::PeerIngress ? ingress : originated;
    }

    // Admit `bytes` or throw WriteOverloadedError. Admission is all-or-nothing per batch
    // and is checked BEFORE the budget is charged, so a single oversized batch on an idle
    // shard is still admitted (it would otherwise be permanently unwritable) while a
    // backlog of ordinary batches is turned away.
    void acquire(size_t bytes) {
        const size_t lim = limitBytes(cls_);
        if (inFlight_ != 0 && inFlight_ + bytes > lim) {
            ++rejected_;
            // The ORIGINATED wording is byte-identical to what shipped before the second
            // budget existed ("cluster: shard write buffer full (N of M bytes in flight)")
            // -- backpressure_gate.sh greps the server log for exactly that, and so may an
            // operator's alert. Only the new class adds a word.
            throw data::WriteOverloadedError(std::string("cluster: shard ") + name(cls_) + " buffer full (" +
                                             std::to_string(inFlight_) + " of " + std::to_string(lim) +
                                             " bytes in flight); retry this write");
        }
        inFlight_ += bytes;
        if (inFlight_ > peak_)
            peak_ = inFlight_;
    }
    void release(size_t bytes) { inFlight_ -= (bytes > inFlight_ ? inFlight_ : bytes); }

    size_t inFlight() const { return inFlight_; }
    size_t peak() const { return peak_; }
    uint64_t rejected() const { return rejected_; }
    void resetStats() {
        peak_ = inFlight_;
        rejected_ = 0;
    }

private:
    explicit WriteAdmission(AdmissionClass cls = AdmissionClass::Originated) : cls_(cls) {}

    static const char* name(AdmissionClass c) {
        return c == AdmissionClass::PeerIngress ? "peer-ingress write" : "write";
    }

    // Parse a byte count from `env`, or the default. STRICT (see below).
    static size_t parseLimit(const char* env, const char* what) {
        const char* e = std::getenv(env);
        if (!e || !*e) {
            timestar::http_log.info("cluster {} in-flight budget: {} bytes/shard (default)", what, kDefaultLimitBytes);
            return kDefaultLimitBytes;
        }
        // STRICT parse. std::stoull stops at the first non-digit, so "32MiB" silently
        // became a 32-BYTE budget -- a node that 503s essentially every write -- and
        // "-1" wrapped to SIZE_MAX, i.e. no bound at all, the exact opposite of what
        // was asked for. Both are configuration mistakes that must be refused loudly
        // rather than obeyed.
        const std::string_view sv(e);
        unsigned long long v = 0;
        const auto res = std::from_chars(sv.data(), sv.data() + sv.size(), v);
        if (res.ec != std::errc{} || res.ptr != sv.data() + sv.size() || v == 0) {
            timestar::http_log.error(
                "{}='{}' is not a positive count of BYTES "
                "(digits only -- no units, no sign); falling back to the {}-byte/shard default",
                env, e, kDefaultLimitBytes);
            return kDefaultLimitBytes;
        }
        timestar::http_log.info("cluster {} in-flight budget: {} bytes/shard ({})", what, static_cast<size_t>(v), env);
        return static_cast<size_t>(v);
    }

    static size_t originatedLimitBytes() {
        static const size_t lim = parseLimit("TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES", "write");
        return lim;
    }
    static size_t ingressLimitBytes() {
        static const size_t lim = parseLimit("TIMESTAR_CLUSTER_INGRESS_INFLIGHT_BYTES", "peer-ingress");
        return lim;
    }

    AdmissionClass cls_ = AdmissionClass::Originated;
    size_t inFlight_ = 0;
    size_t peak_ = 0;
    uint64_t rejected_ = 0;
};

// RAII charge. Constructed in the coordinating coroutine's frame so the budget is
// released on EVERY exit -- success, a retryable failure, or an abandoned batch.
// The class is fixed at construction and used again at release, so a charge can never be
// released against the OTHER budget -- which would let one class drive the other's counter
// negative-by-saturation and quietly stop bounding anything.
class WriteAdmissionGuard {
public:
    explicit WriteAdmissionGuard(size_t bytes, AdmissionClass cls = AdmissionClass::Originated)
        : bytes_(bytes), cls_(cls) {
        WriteAdmission::local(cls_).acquire(bytes);
    }
    ~WriteAdmissionGuard() { WriteAdmission::local(cls_).release(bytes_); }
    WriteAdmissionGuard(const WriteAdmissionGuard&) = delete;
    WriteAdmissionGuard& operator=(const WriteAdmissionGuard&) = delete;

private:
    size_t bytes_;
    AdmissionClass cls_;
};

}  // namespace timestar::cluster

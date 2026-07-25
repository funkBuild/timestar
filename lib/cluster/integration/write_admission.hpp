#pragma once

#include "../data/write_errors.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>

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
class WriteAdmission {
public:
    // The per-shard budget. Default 32 MiB -- several times what the canonical RF=3 bench
    // holds in flight per shard (~2 MB: 8 connections x one ~10k-point batch, held for
    // one ~90 ms quorum round), so it never trips on healthy load, while still bounding a
    // pathological client to something a node can hold. Overridable so the backpressure
    // gate can drive it deliberately.
    static size_t limitBytes() {
        static const size_t lim = [] {
            if (const char* e = std::getenv("TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES")) {
                try {
                    const unsigned long long v = std::stoull(e);
                    if (v > 0)
                        return static_cast<size_t>(v);
                } catch (...) {
                }
            }
            return static_cast<size_t>(32u << 20);
        }();
        return lim;
    }

    static WriteAdmission& local() {
        static thread_local WriteAdmission inst;  // one per reactor
        return inst;
    }

    // Admit `bytes` or throw WriteOverloadedError. Admission is all-or-nothing per batch
    // and is checked BEFORE the budget is charged, so a single oversized batch on an idle
    // shard is still admitted (it would otherwise be permanently unwritable) while a
    // backlog of ordinary batches is turned away.
    void acquire(size_t bytes) {
        if (inFlight_ != 0 && inFlight_ + bytes > limitBytes()) {
            ++rejected_;
            throw data::WriteOverloadedError("cluster: shard write buffer full (" + std::to_string(inFlight_) +
                                             " of " + std::to_string(limitBytes()) +
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
    size_t inFlight_ = 0;
    size_t peak_ = 0;
    uint64_t rejected_ = 0;
};

// RAII charge. Constructed in the coordinating coroutine's frame so the budget is
// released on EVERY exit -- success, a retryable failure, or an abandoned batch.
class WriteAdmissionGuard {
public:
    explicit WriteAdmissionGuard(size_t bytes) : bytes_(bytes) { WriteAdmission::local().acquire(bytes); }
    ~WriteAdmissionGuard() { WriteAdmission::local().release(bytes_); }
    WriteAdmissionGuard(const WriteAdmissionGuard&) = delete;
    WriteAdmissionGuard& operator=(const WriteAdmissionGuard&) = delete;

private:
    size_t bytes_;
};

}  // namespace timestar::cluster

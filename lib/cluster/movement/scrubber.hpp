#pragma once

#include <functional>
#include <set>
#include <string>
#include <vector>

namespace timestar::movement {

// One object to verify: its id and the hash it is expected to have (from the
// manifest / verification hash recorded at snapshot/restore boundaries).
struct ScrubTarget {
    std::string objectId;
    std::string expectedHash;
};

// The verdict for one scrubbed object.
struct ScrubResult {
    std::string objectId;
    bool healthy = true;
    std::string actualHash;
};

// A re-replication request: the movement path must rebuild this VShard's replica
// on this node from a healthy source (full-VShard re-replication through the same
// snapshot/catch-up path a move uses).
struct RepairRequest {
    std::string objectId;  // the corrupt object that triggered the repair
};

// Continuous scrub + corruption quarantine (docs/clustering.md §Phase 7 7b). It
// recomputes each object's hash and compares it to the expected one; a mismatch
// QUARANTINES the object (it is never trusted or served) and emits a
// re-replication request routed through the movement path. The hash function is
// injected (the storage-layer VShardVerificationHash in production).
class Scrubber {
public:
    using HashFn = std::function<std::string(const std::string& objectId)>;
    explicit Scrubber(HashFn hash) : hash_(std::move(hash)) {}

    // Scrub a batch; returns the corrupt objects. Each corrupt object is
    // quarantined and yields a repair request. Healthy objects are untouched.
    std::vector<RepairRequest> scrub(const std::vector<ScrubTarget>& targets) {
        std::vector<RepairRequest> repairs;
        for (const auto& t : targets) {
            // Already quarantined and awaiting re-replication: don't re-emit a
            // repair every scrub cycle (the object stays corrupt until the repair
            // completes and clearQuarantine() is called). Emit only on the
            // quarantine TRANSITION so the movement path isn't spammed.
            if (isQuarantined(t.objectId))
                continue;
            const std::string actual = hash_(t.objectId);
            if (actual != t.expectedHash) {
                quarantined_.insert(t.objectId);
                repairs.push_back(RepairRequest{t.objectId});
            }
        }
        return repairs;
    }

    bool isQuarantined(const std::string& objectId) const { return quarantined_.count(objectId) > 0; }
    const std::set<std::string>& quarantined() const { return quarantined_; }

    // A repaired object (re-replicated and re-verified) leaves quarantine.
    void clearQuarantine(const std::string& objectId) { quarantined_.erase(objectId); }

private:
    HashFn hash_;
    std::set<std::string> quarantined_;
};

}  // namespace timestar::movement

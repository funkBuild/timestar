#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace timestar::features {

// One VShard's backup unit: its whole-state snapshot plus a verification hash.
// This is the per-VShard snapshot export format (reusing the Phase 7 snapshot +
// verification-hash machinery); the snapshot is the backup unit.
struct VShardBackup {
    uint16_t vshard = 0;
    std::string snapshot;          // DataStateMachine::snapshot() bytes
    std::string verificationHash;  // logical hash of the snapshot's state
};

// A backup of a whole cluster: the SOURCE cluster UUID (provenance only), the
// number of VShards the cluster HAS (so restore can detect a truncated backup),
// and every VShard's unit. Membership/config is deliberately NOT part of the
// backup -- restore bootstraps fresh membership.
struct ClusterBackup {
    std::string sourceClusterUuid;
    uint32_t expectedVShards = 0;  // total VShards the source cluster has (completeness check)
    std::vector<VShardBackup> vshards;
};

// The outcome of a restore: the NEW cluster identity and the verified per-VShard
// state ready to load as generation-one state. `ok` is false (with `error`) if the
// restore failed closed.
struct RestoreResult {
    std::string newClusterUuid;
    std::vector<VShardBackup> vshards;
    bool ok = false;
    std::string error;
};

// Backup export + restore (docs/clustering.md §Phase 8). The hash function
// recomputes a snapshot's logical hash to prove integrity.
class BackupRestore {
public:
    using HashFn = std::function<std::string(const std::string& snapshot)>;

    // Export: stamp each (vshard, snapshot) with its verification hash under the
    // source cluster UUID, and record the expected VShard count so a truncated
    // backup is detectable at restore.
    static ClusterBackup exportCluster(const std::string& sourceUuid,
                                       const std::vector<std::pair<uint16_t, std::string>>& snapshots,
                                       const HashFn& hash) {
        ClusterBackup b;
        b.sourceClusterUuid = sourceUuid;
        b.expectedVShards = static_cast<uint32_t>(snapshots.size());
        for (const auto& [vs, snap] : snapshots)
            b.vshards.push_back(VShardBackup{vs, snap, hash(snap)});
        return b;
    }

    // Restore into a NEW cluster UUID as generation-one state. Verifies every
    // unit's hash and fails closed on any mismatch (no partial restore). The new
    // cluster UUID MUST differ from the source: a restore bootstraps a fresh
    // cluster and SCRUBS old membership -- it never resurrects the old identity or
    // reuses its node config. Membership is intentionally absent from the backup,
    // so the restored cluster cannot confuse old membership with the new cluster.
    static RestoreResult restore(const ClusterBackup& backup, const std::string& newClusterUuid, const HashFn& hash) {
        RestoreResult r;
        r.newClusterUuid = newClusterUuid;
        if (newClusterUuid.empty()) {
            r.error = "restore: empty new cluster UUID";
            return r;
        }
        if (newClusterUuid == backup.sourceClusterUuid) {
            // Byte-exact compare: the caller must pass CANONICAL cluster UUIDs
            // (the node-identity binding compares them the same way).
            r.error = "restore: new cluster UUID must differ from the source (scrub old identity)";
            return r;
        }
        // Completeness: an empty or truncated backup must NOT restore as a valid
        // partial cluster. Require every expected VShard unit to be present.
        if (backup.vshards.empty()) {
            r.error = "restore: empty backup";
            return r;
        }
        if (backup.expectedVShards != 0 && backup.vshards.size() != backup.expectedVShards) {
            r.error = "restore: truncated backup (" + std::to_string(backup.vshards.size()) + " of " +
                      std::to_string(backup.expectedVShards) + " VShards)";
            return r;
        }
        for (const auto& u : backup.vshards) {
            if (hash(u.snapshot) != u.verificationHash) {
                r.error = "restore: verification hash mismatch for VShard " + std::to_string(u.vshard);
                return r;  // fail closed: nothing restored
            }
        }
        r.vshards = backup.vshards;  // verified; membership scrubbed (not copied from the backup)
        r.ok = true;
        return r;
    }
};

}  // namespace timestar::features

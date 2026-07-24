#pragma once

#include <filesystem>
#include <string>

namespace timestar::cluster {

// The node's persistent cluster identity (integration plan M3 group-0 bootstrap).
// `node_uuid` is generated ONCE on first start and never changes for the life of the
// data directory; `cluster_uuid` is empty until this node initializes a cluster
// (`timestar cluster init`) or joins one, then fixed. Persisted as node.json under
// data_dir. Per the plan, reinstall-without-identity (a fresh data_dir) is a
// REPLACEMENT, not the same node -- so identity is bound to the data, not the host.
struct NodeIdentity {
    std::string node_uuid;
    std::string cluster_uuid;  // empty until init/join

    // Load node.json from `dataDir`; if absent (or unreadable), generate a fresh
    // node_uuid (128-bit, 32 hex) with an empty cluster_uuid and persist it. Always
    // returns a node with a non-empty node_uuid.
    static NodeIdentity loadOrCreate(const std::filesystem::path& dataDir);
    // Write node.json atomically (temp + rename) so a crash mid-write cannot leave a
    // half-written identity.
    void persist(const std::filesystem::path& dataDir) const;

    static std::string generateUuid();  // 32 lowercase hex chars (128-bit)
    static std::filesystem::path pathIn(const std::filesystem::path& dataDir);
};

}  // namespace timestar::cluster

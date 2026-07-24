#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <string>
#include <vector>

namespace timestar::cluster {

// The multi-node gateway on ONE node (one instance per reactor shard). It knows
// this node's identity and its peers, and replicates accepted writes to every
// peer so that any node holds all data and can serve any read.
//
// Milestone 1 is best-effort FULL REPLICATION: a /write accepted here is applied
// locally AND forwarded to every peer, each of which applies it locally only (a
// loop-guard header stops re-forwarding). It is asynchronous and has no consensus;
// it makes a working, testable cluster while the partitioned/Raft-replicated paths
// are wired on top.
class ClusterGateway {
public:
    // The header that marks an inter-node forwarded request; the receiving node
    // applies it locally and does NOT re-forward (loop guard).
    static constexpr const char* kForwardedHeader = "x-timestar-cluster-forwarded";

    // Reads timestar::config().cluster. A disabled or single-node config yields a
    // gateway whose enabled() is false and whose replicate calls are no-ops.
    ClusterGateway();

    bool enabled() const { return enabled_; }
    uint16_t nodeId() const { return nodeId_; }
    size_t peerCount() const { return peers_.size(); }

    // Replicate a raw mutation body to `path` on every peer (best effort).
    // `mimeType` is the Content-Type ("application/json" / "application/x-protobuf").
    // Resolves once every peer has responded or errored; peer errors are swallowed
    // (logged) so a peer outage never fails the client's request in M1.
    seastar::future<> replicate(std::string path, std::string mimeType, std::string body);

    // Convenience: replicate an accepted write to peers' /write.
    seastar::future<> replicateWrite(std::string mimeType, std::string body) {
        return replicate("/write", std::move(mimeType), std::move(body));
    }
    // Convenience: replicate an accepted delete to peers' /delete.
    seastar::future<> replicateDelete(std::string mimeType, std::string body) {
        return replicate("/delete", std::move(mimeType), std::move(body));
    }

    // True if `req` carries the forwarded loop-guard header (so the handler must
    // apply it locally and not replicate again).
    static bool isForwarded(const seastar::http::request& req);

private:
    struct Peer {
        std::string host;
        uint16_t port = 0;
    };
    seastar::future<> ensureInit();  // resolve peers + open clients once per shard

    bool enabled_ = false;
    uint16_t nodeId_ = 0;
    std::vector<Peer> peers_;  // excludes self
    std::optional<std::string> authHeader_;
    bool inited_ = false;
    std::vector<std::unique_ptr<seastar::http::experimental::client>> clients_;
};

// The per-reactor-shard gateway singleton (one instance per shard; the HTTP
// handlers run per shard). Constructed from config() on first access.
ClusterGateway& shardGateway();

}  // namespace timestar::cluster

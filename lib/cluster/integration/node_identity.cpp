#include "node_identity.hpp"

#include "../../http/http_auth.hpp"

#include <glaze/json.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>

template <>
struct glz::meta<timestar::cluster::NodeIdentity> {
    using T = timestar::cluster::NodeIdentity;
    static constexpr auto value =
        object("node_uuid", &T::node_uuid, "cluster_uuid", &T::cluster_uuid, "static_topology", &T::static_topology);
};

namespace timestar::cluster {

std::filesystem::path NodeIdentity::pathIn(const std::filesystem::path& dataDir) {
    return dataDir / "node.json";
}

std::string NodeIdentity::generateUuid() {
    // 16 random bytes -> 32 lowercase hex chars, drawn from /dev/urandom via
    // http::generateToken. Not RFC-4122-formatted (no dashes/version nibble); we only
    // need a collision-free opaque identifier, and the flat hex form is what the Raft
    // NodeId codecs already carry.
    return http::generateToken(16);
}

NodeIdentity NodeIdentity::loadOrCreate(const std::filesystem::path& dataDir) {
    const auto path = pathIn(dataDir);
    std::error_code ec;
    if (std::filesystem::exists(path, ec) && !ec) {
        std::ifstream in(path, std::ios::binary);
        if (in) {
            std::ostringstream ss;
            ss << in.rdbuf();
            std::string raw = ss.str();
            NodeIdentity id;
            auto err = glz::read_json(id, raw);
            // A present-but-corrupt or node_uuid-less node.json is NOT silently
            // replaced: overwriting it would mint a new identity for a node that may
            // already be a cluster member, forking it from its own Raft state. Fail
            // loudly so an operator repairs it.
            if (err)
                throw std::runtime_error("node.json is present but unparseable at " + path.string() +
                                         " (refusing to overwrite an existing identity)");
            if (id.node_uuid.empty())
                throw std::runtime_error("node.json at " + path.string() +
                                         " has an empty node_uuid (refusing to overwrite an existing identity)");
            return id;
        }
        throw std::runtime_error("node.json exists but could not be opened: " + path.string());
    }

    NodeIdentity id;
    id.node_uuid = generateUuid();
    id.persist(dataDir);
    return id;
}

void NodeIdentity::persist(const std::filesystem::path& dataDir) const {
    const auto path = pathIn(dataDir);
    const auto tmp = path.string() + ".tmp";

    std::string out;
    if (auto err = glz::write_json(*this, out))
        throw std::runtime_error("failed to serialize node identity");

    // POSIX write + fsync (not std::ofstream): the temp file's BYTES must be durable
    // before the rename, or a crash can make the rename visible ahead of the data and
    // resurrect an empty/old identity -- losing a just-set cluster_uuid.
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        throw std::runtime_error("failed to open node.json.tmp for write: " + tmp + ": " + std::strerror(errno));
    size_t written = 0;
    while (written < out.size()) {
        ssize_t n = ::write(fd, out.data() + written, out.size() - written);
        if (n < 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("failed to write node.json.tmp: " + tmp + ": " + std::strerror(e));
        }
        written += static_cast<size_t>(n);
    }
    if (::fsync(fd) != 0) {
        int e = errno;
        ::close(fd);
        throw std::runtime_error("failed to fsync node.json.tmp: " + tmp + ": " + std::strerror(e));
    }
    ::close(fd);

    // Atomic replace: a crash before the rename leaves the old (or no) node.json, never
    // a half-written one.
    std::filesystem::rename(tmp, path);

    // Also fsync the containing directory so the rename itself is durable (otherwise the
    // new dir entry can be lost even though the file bytes are on disk).
    int dfd = ::open(dataDir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
}

}  // namespace timestar::cluster

#include "durable_control_map.hpp"

#include <xxhash.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace timestar::control {

namespace {

constexpr uint64_t kMagic = 0x5453434d'41503100ull;  // "TSCMAP1\0"
constexpr uint64_t kVersion = 1;
constexpr size_t kHeaderBytes = 4 * sizeof(uint64_t);  // magic, version, payload bytes, checksum
constexpr size_t kMaxCacheBytes = 16 * 1024 * 1024;

[[noreturn]] void throwIo(const std::string& operation, int error = errno) {
    throw std::system_error(error, std::generic_category(), operation);
}

void putU64(std::string& out, uint64_t value) {
    for (unsigned i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

uint64_t getU64(const std::string& bytes, size_t& offset) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[offset++])) << (8 * i);
    return value;
}

void syncFd(int fd, const std::string& operation) {
    int result;
    do {
        result = ::fsync(fd);
    } while (result < 0 && errno == EINTR);
    if (result < 0)
        throwIo(operation);
}

void closeFd(int fd, const std::string& operation) {
    if (::close(fd) < 0)
        throwIo(operation);
}

void syncDirectory(const std::filesystem::path& path) {
    const auto directory = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
    int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        throwIo("open control-map directory for fsync: " + directory.string());
    try {
        syncFd(fd, "fsync control-map directory: " + directory.string());
    } catch (...) {
        ::close(fd);
        throw;
    }
    closeFd(fd, "close control-map directory: " + directory.string());
}

std::optional<std::string> readFile(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        if (errno == ENOENT)
            return std::nullopt;
        throwIo("open control-map cache: " + path.string());
    }

    try {
        struct stat statBuf {};
        if (::fstat(fd, &statBuf) < 0)
            throwIo("stat control-map cache: " + path.string());
        if (!S_ISREG(statBuf.st_mode) || statBuf.st_size < static_cast<off_t>(kHeaderBytes) ||
            static_cast<uint64_t>(statBuf.st_size) > kMaxCacheBytes)
            throw std::runtime_error("control-map cache has an invalid file type or size: " + path.string());

        std::string bytes(static_cast<size_t>(statBuf.st_size), '\0');
        size_t offset = 0;
        while (offset < bytes.size()) {
            const ssize_t n = ::read(fd, bytes.data() + offset, bytes.size() - offset);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throwIo("read control-map cache: " + path.string());
            }
            if (n == 0)
                throw std::runtime_error("control-map cache was truncated while reading: " + path.string());
            offset += static_cast<size_t>(n);
        }
        const int completedFd = fd;
        fd = -1;  // close(2) errors must not lead to a dangerous second close
        closeFd(completedFd, "close control-map cache: " + path.string());
        return bytes;
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        throw;
    }
}

std::string envelope(const std::string& payload) {
    std::string bytes;
    bytes.reserve(kHeaderBytes + payload.size());
    putU64(bytes, kMagic);
    putU64(bytes, kVersion);
    putU64(bytes, payload.size());
    putU64(bytes, XXH3_64bits(payload.data(), payload.size()));
    bytes += payload;
    return bytes;
}

void writeAtomically(const std::filesystem::path& path, const std::string& bytes) {
    const std::filesystem::path temporary = path.string() + ".tmp";
    int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0644);
    if (fd < 0)
        throwIo("open temporary control-map cache: " + temporary.string());

    bool renamed = false;
    try {
        size_t offset = 0;
        while (offset < bytes.size()) {
            const ssize_t n = ::write(fd, bytes.data() + offset, bytes.size() - offset);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throwIo("write temporary control-map cache: " + temporary.string());
            }
            if (n == 0)
                throw std::runtime_error("zero-length write to temporary control-map cache: " + temporary.string());
            offset += static_cast<size_t>(n);
        }
        syncFd(fd, "fsync temporary control-map cache: " + temporary.string());
        const int completedFd = fd;
        fd = -1;  // close(2) errors must not lead to a dangerous second close
        closeFd(completedFd, "close temporary control-map cache: " + temporary.string());

        if (::rename(temporary.c_str(), path.c_str()) < 0)
            throwIo("rename control-map cache into place: " + path.string());
        renamed = true;
        syncDirectory(path);
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        if (!renamed)
            ::unlink(temporary.c_str());
        throw;
    }
}

}  // namespace

std::optional<ControlMap> DurableControlMapStore::load() const {
    auto bytes = readFile(path_);
    if (!bytes)
        return std::nullopt;
    if (bytes->size() < kHeaderBytes)
        throw std::runtime_error("control-map cache header is truncated: " + path_.string());

    size_t offset = 0;
    const uint64_t magic = getU64(*bytes, offset);
    const uint64_t version = getU64(*bytes, offset);
    const uint64_t payloadBytes = getU64(*bytes, offset);
    const uint64_t checksum = getU64(*bytes, offset);
    if (magic != kMagic || version != kVersion || payloadBytes != bytes->size() - kHeaderBytes)
        throw std::runtime_error("control-map cache has an unknown or corrupt envelope: " + path_.string());

    const std::string payload = bytes->substr(offset);
    if (XXH3_64bits(payload.data(), payload.size()) != checksum)
        throw std::runtime_error("control-map cache checksum mismatch: " + path_.string());

    ControlMapCache decoded;
    if (!decoded.load(payload) || !isCompleteControlMap(decoded.current()))
        throw std::runtime_error("control-map cache contains an invalid or incomplete serving map: " + path_.string());
    return decoded.current();
}

void DurableControlMapStore::persist(const ControlMap& map) const {
    if (!isCompleteControlMap(map))
        throw std::invalid_argument("refusing to persist an incomplete or invalid serving map");
    if (auto existing = load()) {
        if (map.epoch < existing->epoch)
            throw std::invalid_argument("refusing to regress the durable control-map epoch");
        if (map.epoch == existing->epoch) {
            if (map != *existing)
                throw std::invalid_argument("conflicting control maps have the same epoch");
            // Also closes an ambiguous prior directory-fsync failure: retrying
            // the exact map re-establishes the rename's durability boundary.
            syncDirectory(path_);
            return;
        }
    }

    ControlMapCache encoded;
    if (!encoded.update(map))
        throw std::invalid_argument("refusing to serialize an invalid serving map");
    const std::string payload = encoded.serialize();
    if (payload.size() > kMaxCacheBytes - kHeaderBytes)
        throw std::length_error("control-map cache exceeds the bounded on-disk format");
    writeAtomically(path_, envelope(payload));
}

}  // namespace timestar::control

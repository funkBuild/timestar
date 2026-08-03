#include "cluster_restore_seeder.hpp"

#include "../../core/vshard.hpp"
#include "../../storage/journal_writer.hpp"
#include "../../utils/crc32.hpp"
#include "../control/control_map_cache.hpp"
#include "../control/durable_control_map.hpp"
#include "../control/group0_state_machine.hpp"
#include "../data/snapshot_payload.hpp"
#include "../raft/raft_journal_persistence.hpp"
#include "backup_restore.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <set>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace timestar::features {

namespace {

constexpr uint32_t kMarkerMagic = 0x49525354;  // "TSRI" little-endian
constexpr uint32_t kMarkerVersion = 1;
constexpr size_t kMaxMarkerBytes = 512;
constexpr uint32_t kReleaseMagic = 0x52525354;  // "TSRR" little-endian
constexpr uint32_t kReleaseVersion = 1;
constexpr size_t kMaxReleaseBytes = 4u << 20;
constexpr size_t kJournalSegmentBytes = 1u << 20;
constexpr uint64_t kImportTerm = 1;
constexpr uint64_t kFnvOffset = 1469598103934665603ull;
constexpr uint64_t kFnvPrime = 1099511628211ull;
constexpr size_t kProgressBatch = 16;
inline constexpr VShardId kControlJournalStorageId{0};

struct Marker {
    bool complete = false;
    bool controlDone = false;
    std::string sourceClusterUuid;
    std::string newClusterUuid;
    uint64_t manifestBytes = 0;
    uint64_t manifestHash = 0;
    uint64_t ceremonyHash = 0;
    uint64_t participantsHash = 0;
    uint64_t targetHash = 0;
    uint64_t self = 0;
    uint32_t participantCount = 0;
    uint32_t localCount = 0;
    uint32_t nextLocal = 0;
};

struct ReleaseEntry {
    uint64_t self = 0;
    uint64_t markerBytes = 0;
    uint64_t markerHash = 0;

    friend bool operator==(const ReleaseEntry&, const ReleaseEntry&) = default;
};

struct Release {
    std::string sourceClusterUuid;
    std::string newClusterUuid;
    uint64_t manifestBytes = 0;
    uint64_t manifestHash = 0;
    uint64_t ceremonyHash = 0;
    uint64_t participantsHash = 0;
    uint32_t participantCount = 0;
    std::vector<ReleaseEntry> entries;
};

class UniqueFd {
public:
    explicit UniqueFd(int fd = -1) : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    ~UniqueFd() {
        if (fd_ >= 0)
            ::close(fd_);
    }
    int get() const noexcept { return fd_; }
    int release() noexcept { return std::exchange(fd_, -1); }

private:
    int fd_;
};

[[noreturn]] void throwIo(std::string operation, const std::filesystem::path& path, int error = errno) {
    throw std::system_error(error, std::generic_category(), std::move(operation) + ": " + path.string());
}

void closeChecked(UniqueFd& fd, const std::filesystem::path& path) {
    const int raw = fd.release();
    // Linux releases the descriptor even if close reports EINTR. Retrying can
    // close an unrelated descriptor reused by another thread.
    if (::close(raw) < 0 && errno != EINTR)
        throwIo("close restore file", path);
}

void fsyncDirectory(const std::filesystem::path& directory) {
    UniqueFd fd(::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.get() < 0)
        throwIo("open restore directory for fsync", directory);
    int rc;
    do {
        rc = ::fsync(fd.get());
    } while (rc < 0 && errno == EINTR);
    if (rc < 0)
        throwIo("fsync restore directory", directory);
    closeChecked(fd, directory);
}

void ensureDirectoryDurable(const std::filesystem::path& directory) {
    if (directory.empty())
        throw std::invalid_argument("restore directory is empty");
    std::vector<std::filesystem::path> missing;
    std::filesystem::path cursor = directory;
    std::error_code ec;
    while (!cursor.empty() && !std::filesystem::exists(cursor, ec)) {
        if (ec)
            throw std::filesystem::filesystem_error("inspect restore directory", cursor, ec);
        missing.push_back(cursor);
        cursor = cursor.parent_path();
    }
    if (cursor.empty())
        cursor = ".";
    if (!std::filesystem::is_directory(cursor, ec) || ec)
        throw std::runtime_error("restore target has no valid existing ancestor: " + directory.string());
    for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
        if (!std::filesystem::create_directory(*it, ec) && ec)
            throw std::filesystem::filesystem_error("create restore directory", *it, ec);
        fsyncDirectory(it->parent_path().empty() ? std::filesystem::path(".") : it->parent_path());
    }
}

void writeAll(int fd, const char* data, size_t size, const std::filesystem::path& path) {
    size_t offset = 0;
    while (offset < size) {
        const ssize_t written = ::write(fd, data + offset, size - offset);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            throwIo("write restore file", path);
        }
        if (written == 0)
            throw std::runtime_error("zero-byte write to restore file " + path.string());
        offset += static_cast<size_t>(written);
    }
}

void copyFileDurably(const std::filesystem::path& source, const std::filesystem::path& destination,
                     uint64_t expectedSize) {
    UniqueFd input(::open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (input.get() < 0)
        throwIo("open restore source", source);
    struct stat sourceStat{};
    if (::fstat(input.get(), &sourceStat) < 0)
        throwIo("stat restore source", source);
    if (!S_ISREG(sourceStat.st_mode) || sourceStat.st_size < 0 ||
        static_cast<uint64_t>(sourceStat.st_size) != expectedSize)
        throw std::invalid_argument("restore source changed after archive validation");

    UniqueFd output(::open(destination.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (output.get() < 0)
        throwIo("create imported snapshot staging file", destination);
    try {
        std::vector<char> buffer(size_t{1} << 20);
        uint64_t copied = 0;
        while (copied < expectedSize) {
            const size_t wanted = static_cast<size_t>(std::min<uint64_t>(buffer.size(), expectedSize - copied));
            ssize_t count;
            do {
                count = ::read(input.get(), buffer.data(), wanted);
            } while (count < 0 && errno == EINTR);
            if (count < 0)
                throwIo("read restore source", source);
            if (count == 0)
                throw std::invalid_argument("restore source was truncated during import");
            writeAll(output.get(), buffer.data(), static_cast<size_t>(count), destination);
            copied += static_cast<uint64_t>(count);
            seastar::thread::yield();
        }
        char extra = 0;
        ssize_t trailing;
        do {
            trailing = ::read(input.get(), &extra, 1);
        } while (trailing < 0 && errno == EINTR);
        if (trailing < 0)
            throwIo("read restore source trailer", source);
        if (trailing != 0)
            throw std::invalid_argument("restore source grew during import");
        int rc;
        do {
            rc = ::fsync(output.get());
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            throwIo("fsync imported snapshot staging file", destination);
        closeChecked(output, destination);
        closeChecked(input, source);
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(destination, ec);
        throw;
    }
}

uint64_t fnvExtend(uint64_t hash, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= kFnvPrime;
    }
    return hash;
}

uint64_t fnvString(std::string_view value) {
    return fnvExtend(kFnvOffset, value.data(), value.size());
}

uint64_t fnvU64(uint64_t hash, uint64_t value) {
    std::array<uint8_t, 8> encoded{};
    for (int i = 0; i < 8; ++i)
        encoded[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xff);
    return fnvExtend(hash, encoded.data(), encoded.size());
}

uint64_t hashParticipants(const std::vector<raft::NodeId>& participants) {
    uint64_t hash = kFnvOffset;
    for (const auto node : participants)
        hash = fnvU64(hash, node);
    return hash;
}

void putU32(std::string& out, uint32_t value) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

void putU64(std::string& out, uint64_t value) {
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

void putBlob(std::string& out, std::string_view value) {
    if (value.size() > UINT32_MAX)
        throw std::length_error("restore marker field is too large");
    putU32(out, static_cast<uint32_t>(value.size()));
    out.append(value);
}

class Reader {
public:
    explicit Reader(std::span<const char> input) : input_(input) {}
    uint8_t u8() {
        if (!take(1))
            return 0;
        return static_cast<uint8_t>(input_[offset_ - 1]);
    }
    uint32_t u32() {
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i)
            value |= static_cast<uint32_t>(u8()) << (8 * i);
        return value;
    }
    uint64_t u64() {
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i)
            value |= static_cast<uint64_t>(u8()) << (8 * i);
        return value;
    }
    std::string blob(size_t maximum) {
        const uint32_t size = u32();
        if (!ok_ || size > maximum || !take(size)) {
            ok_ = false;
            return {};
        }
        return std::string(input_.data() + offset_ - size, size);
    }
    bool exhausted() const noexcept { return ok_ && offset_ == input_.size(); }

private:
    bool take(size_t size) {
        if (!ok_ || size > input_.size() - offset_) {
            ok_ = false;
            return false;
        }
        offset_ += size;
        return true;
    }

    std::span<const char> input_;
    size_t offset_ = 0;
    bool ok_ = true;
};

std::string encodeMarker(const Marker& marker) {
    std::string out;
    out.reserve(160);
    putU32(out, kMarkerMagic);
    putU32(out, kMarkerVersion);
    out.push_back(marker.complete ? 1 : 0);
    out.push_back(marker.controlDone ? 1 : 0);
    putBlob(out, marker.sourceClusterUuid);
    putBlob(out, marker.newClusterUuid);
    putU64(out, marker.manifestBytes);
    putU64(out, marker.manifestHash);
    putU64(out, marker.ceremonyHash);
    putU64(out, marker.participantsHash);
    putU64(out, marker.targetHash);
    putU64(out, marker.self);
    putU32(out, marker.participantCount);
    putU32(out, marker.localCount);
    putU32(out, marker.nextLocal);
    putU32(out, CRC32::compute(out.data(), out.size()));
    return out;
}

std::optional<Marker> decodeMarker(const std::string& bytes) {
    if (bytes.size() < 4 || bytes.size() > kMaxMarkerBytes)
        return std::nullopt;
    const size_t bodySize = bytes.size() - 4;
    uint32_t storedCrc = 0;
    for (int i = 0; i < 4; ++i)
        storedCrc |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[bodySize + i])) << (8 * i);
    if (CRC32::compute(bytes.data(), bodySize) != storedCrc)
        return std::nullopt;
    Reader reader(std::span<const char>(bytes.data(), bodySize));
    if (reader.u32() != kMarkerMagic || reader.u32() != kMarkerVersion)
        return std::nullopt;
    const uint8_t complete = reader.u8();
    const uint8_t controlDone = reader.u8();
    Marker marker;
    marker.complete = complete != 0;
    marker.controlDone = controlDone != 0;
    marker.sourceClusterUuid = reader.blob(32);
    marker.newClusterUuid = reader.blob(32);
    marker.manifestBytes = reader.u64();
    marker.manifestHash = reader.u64();
    marker.ceremonyHash = reader.u64();
    marker.participantsHash = reader.u64();
    marker.targetHash = reader.u64();
    marker.self = reader.u64();
    marker.participantCount = reader.u32();
    marker.localCount = reader.u32();
    marker.nextLocal = reader.u32();
    if (complete > 1 || controlDone > 1 || !reader.exhausted() || marker.sourceClusterUuid.size() != 32 ||
        marker.newClusterUuid.size() != 32 || marker.manifestBytes == 0 || marker.self == raft::kNoNode ||
        marker.participantCount == 0 || marker.nextLocal > marker.localCount ||
        (marker.complete && marker.nextLocal != marker.localCount))
        return std::nullopt;
    return marker;
}

std::string encodeRelease(const Release& release) {
    std::string out;
    out.reserve(96 + release.entries.size() * 24);
    putU32(out, kReleaseMagic);
    putU32(out, kReleaseVersion);
    putBlob(out, release.sourceClusterUuid);
    putBlob(out, release.newClusterUuid);
    putU64(out, release.manifestBytes);
    putU64(out, release.manifestHash);
    putU64(out, release.ceremonyHash);
    putU64(out, release.participantsHash);
    putU32(out, release.participantCount);
    putU32(out, static_cast<uint32_t>(release.entries.size()));
    for (const auto& entry : release.entries) {
        putU64(out, entry.self);
        putU64(out, entry.markerBytes);
        putU64(out, entry.markerHash);
    }
    putU32(out, CRC32::compute(out.data(), out.size()));
    if (out.size() > kMaxReleaseBytes)
        throw std::length_error("cluster restore release exceeds the exact-v1 size bound");
    return out;
}

std::optional<Release> decodeRelease(const std::string& bytes) {
    if (bytes.size() < 4 || bytes.size() > kMaxReleaseBytes)
        return std::nullopt;
    const size_t bodySize = bytes.size() - 4;
    uint32_t storedCrc = 0;
    for (int i = 0; i < 4; ++i)
        storedCrc |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[bodySize + i])) << (8 * i);
    if (CRC32::compute(bytes.data(), bodySize) != storedCrc)
        return std::nullopt;
    Reader reader(std::span<const char>(bytes.data(), bodySize));
    if (reader.u32() != kReleaseMagic || reader.u32() != kReleaseVersion)
        return std::nullopt;
    Release release;
    release.sourceClusterUuid = reader.blob(32);
    release.newClusterUuid = reader.blob(32);
    release.manifestBytes = reader.u64();
    release.manifestHash = reader.u64();
    release.ceremonyHash = reader.u64();
    release.participantsHash = reader.u64();
    release.participantCount = reader.u32();
    const uint32_t entryCount = reader.u32();
    if (entryCount == 0 || entryCount != release.participantCount || entryCount > (kMaxReleaseBytes - 96) / 24)
        return std::nullopt;
    release.entries.reserve(entryCount);
    std::vector<raft::NodeId> participants;
    participants.reserve(entryCount);
    uint64_t previous = 0;
    for (uint32_t i = 0; i < entryCount; ++i) {
        ReleaseEntry entry{reader.u64(), reader.u64(), reader.u64()};
        if (entry.self == raft::kNoNode || entry.self <= previous || entry.markerBytes == 0 ||
            entry.markerBytes > kMaxMarkerBytes)
            return std::nullopt;
        previous = entry.self;
        participants.push_back(entry.self);
        release.entries.push_back(entry);
    }
    if (!reader.exhausted() || release.sourceClusterUuid.size() != 32 || release.newClusterUuid.size() != 32 ||
        release.manifestBytes == 0 || hashParticipants(participants) != release.participantsHash)
        return std::nullopt;
    return release;
}

bool releaseMatchesMarker(const Release& release, const Marker& marker, std::string_view markerBytes) {
    if (!marker.complete || !marker.controlDone || release.sourceClusterUuid != marker.sourceClusterUuid ||
        release.newClusterUuid != marker.newClusterUuid || release.manifestBytes != marker.manifestBytes ||
        release.manifestHash != marker.manifestHash || release.ceremonyHash != marker.ceremonyHash ||
        release.participantsHash != marker.participantsHash || release.participantCount != marker.participantCount)
        return false;
    const auto entry = std::ranges::lower_bound(release.entries, marker.self, {}, &ReleaseEntry::self);
    return entry != release.entries.end() && entry->self == marker.self && entry->markerBytes == markerBytes.size() &&
           entry->markerHash == fnvString(markerBytes);
}

std::optional<std::string> readBoundedRegularFile(const std::filesystem::path& path, size_t maximum,
                                                  std::string_view description) {
    UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.get() < 0) {
        if (errno == ENOENT)
            return std::nullopt;
        throwIo("open " + std::string(description), path);
    }
    struct stat st{};
    if (::fstat(fd.get(), &st) < 0)
        throwIo("stat " + std::string(description), path);
    if (!S_ISREG(st.st_mode) || st.st_size <= 0 || static_cast<size_t>(st.st_size) > maximum)
        throw std::runtime_error(std::string(description) + " has an invalid type or size: " + path.string());
    std::string bytes(static_cast<size_t>(st.st_size), '\0');
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count = ::read(fd.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR)
                continue;
            throwIo("read " + std::string(description), path);
        }
        if (count == 0)
            throw std::runtime_error(std::string(description) + " was truncated while reading");
        offset += static_cast<size_t>(count);
    }
    closeChecked(fd, path);
    return bytes;
}

std::optional<std::string> readMarkerFile(const std::filesystem::path& path) {
    return readBoundedRegularFile(path, kMaxMarkerBytes, "cluster restore marker");
}

void writeMarkerAtomically(const std::filesystem::path& path, const Marker& marker) {
    const auto temporary = path.string() + ".tmp";
    const std::string bytes = encodeMarker(marker);
    UniqueFd fd(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (fd.get() < 0)
        throwIo("open temporary cluster restore marker", temporary);
    bool renamed = false;
    try {
        writeAll(fd.get(), bytes.data(), bytes.size(), temporary);
        int rc;
        do {
            rc = ::fsync(fd.get());
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            throwIo("fsync temporary cluster restore marker", temporary);
        closeChecked(fd, temporary);
        if (::rename(temporary.c_str(), path.c_str()) < 0)
            throwIo("publish cluster restore marker", path);
        renamed = true;
        fsyncDirectory(path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path());
    } catch (...) {
        if (!renamed)
            ::unlink(temporary.c_str());
        throw;
    }
}

void writeFileNoReplaceDurably(const std::filesystem::path& path, std::string_view bytes, size_t maximum,
                               std::string_view description) {
    const auto directory = path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path();
    ensureDirectoryDurable(directory);
    if (auto existing = readBoundedRegularFile(path, maximum, description)) {
        if (*existing == bytes) {
            fsyncDirectory(directory);
            return;
        }
        throw std::runtime_error(std::string(description) + " already exists with different bytes: " + path.string());
    }

    const auto temporary = path.string() + ".tmp." + std::to_string(::getpid());
    UniqueFd fd(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (fd.get() < 0)
        throwIo("create temporary " + std::string(description), temporary);
    bool published = false;
    try {
        writeAll(fd.get(), bytes.data(), bytes.size(), temporary);
        int rc;
        do {
            rc = ::fsync(fd.get());
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            throwIo("fsync temporary " + std::string(description), temporary);
        closeChecked(fd, temporary);
        if (::link(temporary.c_str(), path.c_str()) < 0) {
            if (errno != EEXIST)
                throwIo("publish " + std::string(description), path);
            auto existing = readBoundedRegularFile(path, maximum, description);
            if (!existing || *existing != bytes)
                throw std::runtime_error(std::string(description) +
                                         " was concurrently published with different bytes: " + path.string());
        }
        published = true;
        const int unlinkResult = ::unlink(temporary.c_str());
        const int unlinkError = errno;
        fsyncDirectory(directory);
        if (unlinkResult < 0)
            throwIo("remove temporary " + std::string(description), temporary, unlinkError);
    } catch (...) {
        if (!published)
            ::unlink(temporary.c_str());
        throw;
    }
}

std::array<uint8_t, 16> decodeHex128(std::string_view value) {
    if (value.size() != 32)
        throw std::invalid_argument("cluster restore UUID must contain 32 hexadecimal characters");
    const auto nibble = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9')
            return static_cast<uint8_t>(c - '0');
        if (c >= 'a' && c <= 'f')
            return static_cast<uint8_t>(c - 'a' + 10);
        throw std::invalid_argument("cluster restore UUID must be canonical lowercase hexadecimal");
    };
    std::array<uint8_t, 16> out{};
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<uint8_t>((nibble(value[2 * i]) << 4) | nibble(value[2 * i + 1]));
    return out;
}

std::vector<std::pair<uint16_t, std::vector<raft::NodeId>>> localGroups(const ClusterRestoreSeedRequest& request) {
    std::vector<std::pair<uint16_t, std::vector<raft::NodeId>>> groups;
    for (const auto& [vshard, voters] : request.servingMap.placement)
        if (std::ranges::find(voters, request.self) != voters.end())
            groups.emplace_back(vshard, voters);
    return groups;
}

std::vector<raft::NodeId> restoreParticipants(const ClusterRestoreSeedRequest& request) {
    std::set<raft::NodeId> unique{request.controlSeed};
    for (const auto& [vshard, voters] : request.servingMap.placement) {
        (void)vshard;
        unique.insert(voters.begin(), voters.end());
    }
    return {unique.begin(), unique.end()};
}

uint64_t ceremonyFingerprint(const ClusterRestoreSeedRequest& request) {
    control::ControlMapCache encoded;
    if (!encoded.update(request.servingMap))
        throw std::invalid_argument("cluster restore target has an invalid serving map");
    const std::string mapBytes = encoded.serialize();
    uint64_t hash = fnvExtend(kFnvOffset, request.newClusterUuid.data(), request.newClusterUuid.size());
    hash = fnvU64(hash, request.controlSeed);
    hash = fnvExtend(hash, mapBytes.data(), mapBytes.size());
    return hash;
}

uint64_t targetFingerprint(const ClusterRestoreSeedRequest& request) {
    control::ControlMapCache encoded;
    if (!encoded.update(request.servingMap))
        throw std::invalid_argument("cluster restore target has an invalid serving map");
    const std::string mapBytes = encoded.serialize();
    uint64_t hash = fnvExtend(kFnvOffset, request.newClusterUuid.data(), request.newClusterUuid.size());
    hash = fnvU64(hash, request.self);
    hash = fnvU64(hash, request.controlSeed);
    hash = fnvU64(hash, request.coreCount);
    hash = fnvExtend(hash, mapBytes.data(), mapBytes.size());
    if (request.controlSeedRecord) {
        const auto& record = *request.controlSeedRecord;
        hash = fnvU64(hash, record.raftId);
        hash = fnvExtend(hash, record.uuid.data(), record.uuid.size());
        hash = fnvExtend(hash, record.address.data(), record.address.size());
        hash = fnvExtend(hash, record.failureDomain.data(), record.failureDomain.size());
    }
    return hash;
}

void validateRequest(const ClusterRestoreSeedRequest& request) {
    if (request.archiveDirectory.empty() || request.dataDirectory.empty())
        throw std::invalid_argument("cluster restore archive and data directories are required");
    if (!request.authenticationKey)
        throw std::invalid_argument("cluster restore authentication key is required");
    if (!BackupRestore::canonicalClusterUuid(request.newClusterUuid) ||
        decodeHex128(request.newClusterUuid) != request.clusterUuidBytes)
        throw std::invalid_argument("cluster restore UUID text and journal identity do not match");
    if (request.self == raft::kNoNode || request.controlSeed == raft::kNoNode || request.coreCount == 0)
        throw std::invalid_argument("cluster restore requires non-zero node, control seed, and core count");
    if (!vshardsCohesiveOnCores(request.coreCount))
        throw std::invalid_argument("cluster restore core count does not keep VShards cohesive");
    if (!control::isCompleteControlMap(request.servingMap) || !request.servingMap.groups.empty())
        throw std::invalid_argument("cluster restore requires one complete identity-mapped v1 serving map");
    const auto participants = restoreParticipants(request);
    if (!std::ranges::binary_search(participants, request.self))
        throw std::invalid_argument("cluster restore node is not a Group-0 seed or configured data voter");
    if (request.self == request.controlSeed) {
        if (!request.controlSeedRecord || request.controlSeedRecord->raftId != request.self)
            throw std::invalid_argument("control seed restore requires this node's persistent identity record");
    } else if (request.controlSeedRecord) {
        throw std::invalid_argument("only the configured control seed may seed Group 0");
    }
}

control::Group0State restoredControlState(const ClusterRestoreSeedRequest& request,
                                          const PortableControlBackup& portable) {
    control::Group0State state;
    state.clusterUuid = request.newClusterUuid;
    state.mapEpoch = request.servingMap.epoch;
    state.appliedIndex = 1;
    auto seed = *request.controlSeedRecord;
    seed.state = control::NodeState::Active;
    state.nodes.emplace(seed.raftId, std::move(seed));
    state.metaVoters = {request.controlSeed};
    state.servingMap = request.servingMap;
    state.policies = portable.policies;
    state.lastRetentionSweepId = portable.lastRetentionSweepId;
    state.retentionCutoffs = portable.retentionCutoffs;
    state.frozenDeletePlans = portable.frozenDeletePlans;
    return state;
}

bool exactImportedRecords(const std::vector<JournalRecord>& records, bool& haveSnapshot, bool& haveHardState) {
    haveSnapshot = false;
    haveHardState = false;
    uint64_t previousSeq = 0;
    for (const auto& record : records) {
        if (record.vshardSeq == 0 || record.vshardSeq <= previousSeq)
            return false;
        previousSeq = record.vshardSeq;
        if (record.kind == JournalRecordKind::Snapshot && !haveSnapshot)
            haveSnapshot = true;
        else if (record.kind == JournalRecordKind::HardState && !haveHardState)
            haveHardState = true;
        else
            return false;
    }
    return records.size() == static_cast<size_t>(haveSnapshot) + static_cast<size_t>(haveHardState);
}

seastar::future<> closeWriterPreserving(JournalWriter& writer, std::exception_ptr failure) {
    try {
        co_await writer.close();
    } catch (...) {
        if (!failure)
            throw;
    }
    if (failure)
        std::rethrow_exception(failure);
}

seastar::future<std::shared_ptr<raft::SnapshotFile>> prepareImportedSnapshot(
    const std::filesystem::path& source, const std::filesystem::path& snapshotDirectory, const VShardBackupUnit& unit) {
    co_await seastar::async([snapshotDirectory] { ensureDirectoryDurable(snapshotDirectory); });
    const auto staging = snapshotDirectory / ("snapshot_v1_import_g" + std::to_string(unit.vshard) + ".bin");
    const std::filesystem::path authenticated = staging.string() + ".authenticated";
    const auto extraction = snapshotDirectory / "import_extract_tmp";
    co_await seastar::async([staging, authenticated, extraction] {
        std::error_code ec;
        std::filesystem::remove(staging, ec);
        ec.clear();
        std::filesystem::remove(authenticated, ec);
        ec.clear();
        std::filesystem::remove_all(extraction, ec);
        if (ec)
            throw std::filesystem::filesystem_error("clean interrupted restore staging", extraction, ec);
    });

    co_await seastar::async(
        [source, authenticated, expectedSize = unit.encodedSize] { copyFileDurably(source, authenticated, expectedSize); });
    auto importedInfo = co_await data::inspectSnapshotPayloadFile(authenticated);
    if (!importedInfo || importedInfo->manifest.vshard.value() != unit.vshard ||
        importedInfo->encodedSize != unit.encodedSize || importedInfo->encodedSha256 != unit.encodedSha256)
        throw std::runtime_error("cluster restore TSP1 changed after authenticated archive validation");
    uint64_t size = importedInfo->encodedSize;
    uint64_t hash = importedInfo->encodedHash;
    if (unit.snapshotRevision == 1) {
        auto decoded = co_await data::decodeSnapshotPayloadFile(authenticated, extraction);
        if (!decoded)
            throw std::runtime_error("cluster restore could not decode revision-one TSP1 unit");
        decoded->manifest.snapshotRevision = 2;
        auto encoded = co_await data::encodeSnapshotPayloadFile(std::move(*decoded), staging);
        size = encoded.size;
        hash = encoded.hash;
        encoded.release();
        co_await seastar::async([authenticated, extraction] {
            std::error_code ec;
            std::filesystem::remove(authenticated, ec);
            if (ec)
                throw std::filesystem::filesystem_error("remove authenticated restore staging", authenticated, ec);
            std::filesystem::remove_all(extraction, ec);
            if (ec)
                throw std::filesystem::filesystem_error("remove restore extraction directory", extraction, ec);
        });
    } else {
        co_await seastar::async([authenticated, staging, snapshotDirectory] {
            if (::rename(authenticated.c_str(), staging.c_str()) < 0)
                throwIo("publish authenticated restore staging", staging);
            fsyncDirectory(snapshotDirectory);
        });
    }

    auto file = std::make_shared<raft::SnapshotFile>();
    file->path = staging;
    file->size = size;
    file->hash = hash;
    file->removeOnDestroy = true;
    co_return file;
}

bool importedSnapshotMatches(const raft::Snapshot& snapshot, bool fromPeer, const VShardBackupUnit& unit,
                             const std::vector<raft::NodeId>& voters, const data::SnapshotPayloadFileInfo& info) {
    const uint64_t expectedRevision = std::max<uint64_t>(unit.snapshotRevision, 2);
    return fromPeer && snapshot.index == expectedRevision - 1 && snapshot.term == kImportTerm &&
           snapshot.config.voters == voters && snapshot.config.votersOutgoing.empty() &&
           snapshot.config.learners.empty() && snapshot.fileBacked() && info.manifest.vshard.value() == unit.vshard &&
           info.manifest.snapshotRevision == expectedRevision &&
           info.manifest.verificationHash == unit.verificationHash && info.manifest.catalogHash == unit.catalogHash;
}

seastar::future<> seedVShardJournal(const ClusterRestoreSeedRequest& request, const VShardBackupUnit& unit,
                                    const std::vector<raft::NodeId>& voters) {
    const auto journalRoot = request.dataDirectory / "cluster_raft";
    const auto directory = journalRoot / ("vshard_" + std::to_string(unit.vshard));
    const auto snapshotDirectory = directory / "snapshot_sidecars";
    co_await seastar::async([directory] { ensureDirectoryDurable(directory); });

    JournalSegmentHeader header;
    header.clusterUuid = request.clusterUuidBytes;
    header.bootId = request.bootId;
    header.coreNumber = assignCore(VShardId{unit.vshard}, request.coreCount);
    JournalWriter writer(directory, header, kJournalSegmentBytes);
    std::exception_ptr failure;
    try {
        auto records = co_await writer.open();
        bool haveSnapshot = false;
        bool haveHardState = false;
        if (!exactImportedRecords(records, haveSnapshot, haveHardState))
            throw std::runtime_error("cluster restore found non-import Raft records in " + directory.string());
        auto recovered = raft::recoverRaftState(records, VShardId{unit.vshard}, snapshotDirectory);

        if (!haveSnapshot) {
            if (haveHardState || recovered.snapshot)
                throw std::runtime_error("cluster restore found a hard state without its imported snapshot");
            co_await raft::cleanupSnapshotDirectory(snapshotDirectory);
            raft::JournalRaftPersistence persistence(writer, VShardId{unit.vshard}, 1, snapshotDirectory);
            auto file = co_await prepareImportedSnapshot(
                request.archiveDirectory / BackupRestore::unitRelativePath(unit.vshard), snapshotDirectory, unit);
            raft::Snapshot snapshot;
            snapshot.index = std::max<uint64_t>(unit.snapshotRevision, 2) - 1;
            snapshot.term = kImportTerm;
            snapshot.config.voters = voters;
            snapshot.file = std::move(file);
            co_await persistence.persistSnapshot(std::move(snapshot), /*receivedFromPeer=*/true);
            co_await persistence.persistHardState(raft::HardState{kImportTerm, raft::kNoNode});
            co_await persistence.sync();
        } else {
            if (!recovered.snapshot || !recovered.snapshot->file)
                throw std::runtime_error("cluster restore imported snapshot has no durable sidecar");
            co_await raft::validateSnapshotFile(*recovered.snapshot->file);
            auto info = co_await data::inspectSnapshotPayloadFile(recovered.snapshot->file->path);
            if (!info || !importedSnapshotMatches(*recovered.snapshot, recovered.snapshotFromPeer, unit, voters, *info))
                throw std::runtime_error("cluster restore journal conflicts with the requested TSP1 or voter set");
            if (recovered.log.lastIndex() != recovered.snapshot->index)
                throw std::runtime_error("cluster restore journal contains an unexpected log suffix");
            if (haveHardState) {
                if (recovered.hardState != raft::HardState{kImportTerm, raft::kNoNode})
                    throw std::runtime_error("cluster restore journal has an unexpected hard state");
            } else {
                raft::JournalRaftPersistence persistence(writer, VShardId{unit.vshard}, recovered.nextSeq,
                                                         snapshotDirectory);
                persistence.seedRetention(std::move(recovered.retention));
                persistence.seedSnapshotFile(recovered.snapshot->file);
                co_await persistence.persistHardState(raft::HardState{kImportTerm, raft::kNoNode});
                co_await persistence.sync();
            }
            co_await raft::cleanupSnapshotDirectory(snapshotDirectory, recovered.snapshot->file);
        }
    } catch (...) {
        failure = std::current_exception();
    }
    co_await closeWriterPreserving(writer, failure);
}

seastar::future<> seedControlJournal(const ClusterRestoreSeedRequest& request, const PortableControlBackup& portable) {
    const auto directory = request.dataDirectory / "cluster_raft" / "group0";
    co_await seastar::async([directory] { ensureDirectoryDurable(directory); });
    JournalSegmentHeader header;
    header.clusterUuid = request.clusterUuidBytes;
    header.bootId = request.bootId;
    header.coreNumber = 0;
    JournalWriter writer(directory, header, kJournalSegmentBytes);
    std::exception_ptr failure;
    try {
        auto records = co_await writer.open();
        bool haveSnapshot = false;
        bool haveHardState = false;
        if (!exactImportedRecords(records, haveSnapshot, haveHardState))
            throw std::runtime_error("cluster restore found non-import records in the Group-0 journal");
        auto recovered = raft::recoverRaftState(records, kControlJournalStorageId);
        auto state = restoredControlState(request, portable);
        auto expected = control::Group0StateMachine::snapshotForRestore(std::move(state));
        if (!expected)
            throw std::runtime_error("cluster restore portable control state is not a valid fresh Group-0 snapshot");

        if (!haveSnapshot) {
            if (haveHardState || recovered.snapshot)
                throw std::runtime_error("cluster restore found Group-0 hard state without its snapshot");
            raft::JournalRaftPersistence persistence(writer, kControlJournalStorageId, 1);
            raft::Snapshot snapshot;
            snapshot.index = 1;
            snapshot.term = kImportTerm;
            snapshot.config.voters = {request.controlSeed};
            snapshot.data = *expected;
            co_await persistence.persistSnapshot(std::move(snapshot), /*receivedFromPeer=*/false);
            co_await persistence.persistHardState(raft::HardState{kImportTerm, raft::kNoNode});
            co_await persistence.sync();
        } else {
            if (!recovered.snapshot || recovered.snapshot->fileBacked() || recovered.snapshot->index != 1 ||
                recovered.snapshot->term != kImportTerm ||
                recovered.snapshot->config.voters != std::vector<raft::NodeId>{request.controlSeed} ||
                !recovered.snapshot->config.votersOutgoing.empty() || !recovered.snapshot->config.learners.empty() ||
                recovered.snapshot->data != *expected || recovered.log.lastIndex() != recovered.snapshot->index)
                throw std::runtime_error("cluster restore Group-0 journal conflicts with the requested fresh state");
            if (haveHardState) {
                if (recovered.hardState != raft::HardState{kImportTerm, raft::kNoNode})
                    throw std::runtime_error("cluster restore Group-0 journal has an unexpected hard state");
            } else {
                raft::JournalRaftPersistence persistence(writer, kControlJournalStorageId, recovered.nextSeq);
                persistence.seedRetention(std::move(recovered.retention));
                co_await persistence.persistHardState(raft::HardState{kImportTerm, raft::kNoNode});
                co_await persistence.sync();
            }
        }
    } catch (...) {
        failure = std::current_exception();
    }
    co_await closeWriterPreserving(writer, failure);
}

}  // namespace

std::filesystem::path ClusterRestoreSeeder::markerPath(const std::filesystem::path& dataDirectory) {
    return dataDirectory / "cluster_restore.v1";
}

std::filesystem::path ClusterRestoreSeeder::releaseReceiptPath(const std::filesystem::path& dataDirectory) {
    return dataDirectory / "cluster_restore_release.v1";
}

ClusterRestoreTargetState ClusterRestoreSeeder::inspectTarget(const std::filesystem::path& dataDirectory) {
    try {
        auto bytes = readMarkerFile(markerPath(dataDirectory));
        auto releaseBytes = readBoundedRegularFile(releaseReceiptPath(dataDirectory), kMaxReleaseBytes,
                                                   "cluster restore release receipt");
        if (!bytes)
            return releaseBytes ? ClusterRestoreTargetState::Invalid : ClusterRestoreTargetState::Absent;
        auto marker = decodeMarker(*bytes);
        if (!marker)
            return ClusterRestoreTargetState::Invalid;
        if (!marker->complete)
            return releaseBytes ? ClusterRestoreTargetState::Invalid : ClusterRestoreTargetState::InProgress;
        if (!releaseBytes)
            return ClusterRestoreTargetState::Prepared;
        auto release = decodeRelease(*releaseBytes);
        if (!release || !releaseMatchesMarker(*release, *marker, *bytes))
            return ClusterRestoreTargetState::Invalid;
        return ClusterRestoreTargetState::Activated;
    } catch (...) {
        return ClusterRestoreTargetState::Invalid;
    }
}

seastar::future<ClusterRestoreSeedResult> ClusterRestoreSeeder::seed(ClusterRestoreSeedRequest request) {
    validateRequest(request);
    auto manifest = co_await ClusterBackupArchive::validate(request.archiveDirectory, *request.authenticationKey);
    if (!manifest)
        throw std::runtime_error("cluster restore archive is incomplete, corrupt, unauthenticated, or not exact v1");
    auto plan = BackupRestore::planRestore(*manifest, request.newClusterUuid, *request.authenticationKey);
    if (!plan.ok)
        throw std::runtime_error(plan.error);

    const std::string manifestBytes = manifest->encode();
    const uint64_t manifestHash = fnvString(manifestBytes);
    const uint64_t targetHash = targetFingerprint(request);
    const uint64_t ceremonyHash = ceremonyFingerprint(request);
    const auto participants = restoreParticipants(request);
    auto groups = localGroups(request);
    if (participants.size() > UINT32_MAX || groups.size() > UINT32_MAX)
        throw std::length_error("cluster restore participant or local VShard count exceeds marker framing");

    const auto statePath = markerPath(request.dataDirectory);
    auto existingBytes = co_await seastar::async([statePath] { return readMarkerFile(statePath); });
    std::optional<Marker> existing;
    if (existingBytes) {
        existing = decodeMarker(*existingBytes);
        if (!existing)
            throw std::runtime_error("cluster restore marker is corrupt; refusing to guess import progress");
    }

    Marker marker;
    marker.controlDone = request.self != request.controlSeed;
    marker.sourceClusterUuid = manifest->sourceClusterUuid;
    marker.newClusterUuid = request.newClusterUuid;
    marker.manifestBytes = manifestBytes.size();
    marker.manifestHash = manifestHash;
    marker.ceremonyHash = ceremonyHash;
    marker.participantsHash = hashParticipants(participants);
    marker.targetHash = targetHash;
    marker.self = request.self;
    marker.participantCount = static_cast<uint32_t>(participants.size());
    marker.localCount = static_cast<uint32_t>(groups.size());
    if (existing) {
        if (existing->sourceClusterUuid != marker.sourceClusterUuid ||
            existing->newClusterUuid != marker.newClusterUuid || existing->manifestBytes != marker.manifestBytes ||
            existing->manifestHash != marker.manifestHash || existing->ceremonyHash != marker.ceremonyHash ||
            existing->participantsHash != marker.participantsHash ||
            existing->participantCount != marker.participantCount || existing->targetHash != marker.targetHash ||
            existing->self != marker.self || existing->localCount != marker.localCount)
            throw std::runtime_error("cluster restore request conflicts with the durable in-progress import");
        marker = *existing;
    } else {
        std::error_code ec;
        const auto raftRoot = request.dataDirectory / "cluster_raft";
        if (std::filesystem::exists(raftRoot, ec) || ec)
            throw std::runtime_error("cluster restore requires a target with no existing cluster_raft directory");
        co_await seastar::async([dataDirectory = request.dataDirectory, statePath, marker] {
            ensureDirectoryDurable(dataDirectory);
            writeMarkerAtomically(statePath, marker);
        });
    }

    ClusterRestoreSeedResult result;
    result.localVShards = groups.size();
    result.resumed = existing.has_value();
    if (marker.complete) {
        if (request.self == request.controlSeed && !marker.controlDone)
            throw std::runtime_error("completed cluster restore marker omits the required Group-0 seed");
        co_return result;
    }

    if (!marker.controlDone) {
        co_await seedControlJournal(request, plan.control);
        marker.controlDone = true;
        co_await seastar::async([statePath, marker] { writeMarkerAtomically(statePath, marker); });
    }

    size_t sinceProgress = 0;
    for (size_t i = marker.nextLocal; i < groups.size(); ++i) {
        const auto& [vshard, voters] = groups[i];
        co_await seedVShardJournal(request, plan.vshards[vshard], voters);
        ++result.seededThisRun;
        ++sinceProgress;
        marker.nextLocal = static_cast<uint32_t>(i + 1);
        if (request.vshardDurableForTesting)
            request.vshardDurableForTesting(vshard);
        if (sinceProgress == kProgressBatch || marker.nextLocal == marker.localCount) {
            co_await seastar::async([statePath, marker] { writeMarkerAtomically(statePath, marker); });
            sinceProgress = 0;
        }
    }

    co_await seastar::async([dataDirectory = request.dataDirectory, map = request.servingMap] {
        control::DurableControlMapStore(dataDirectory).persist(map);
    });
    marker.complete = true;
    co_await seastar::async([statePath, marker] { writeMarkerAtomically(statePath, marker); });
    co_return result;
}

void ClusterRestoreSeeder::finalizeRelease(const std::vector<std::filesystem::path>& markerFiles,
                                           const std::filesystem::path& output) {
    if (markerFiles.empty() || output.empty())
        throw std::invalid_argument("cluster restore finalization requires marker files and an output path");

    Release release;
    bool haveCommon = false;
    for (const auto& path : markerFiles) {
        auto bytes = readMarkerFile(path);
        if (!bytes)
            throw std::runtime_error("cluster restore participant marker is missing: " + path.string());
        auto marker = decodeMarker(*bytes);
        if (!marker || !marker->complete || !marker->controlDone)
            throw std::runtime_error("cluster restore participant marker is invalid or incomplete: " + path.string());
        if (!haveCommon) {
            release.sourceClusterUuid = marker->sourceClusterUuid;
            release.newClusterUuid = marker->newClusterUuid;
            release.manifestBytes = marker->manifestBytes;
            release.manifestHash = marker->manifestHash;
            release.ceremonyHash = marker->ceremonyHash;
            release.participantsHash = marker->participantsHash;
            release.participantCount = marker->participantCount;
            haveCommon = true;
        } else if (release.sourceClusterUuid != marker->sourceClusterUuid ||
                   release.newClusterUuid != marker->newClusterUuid || release.manifestBytes != marker->manifestBytes ||
                   release.manifestHash != marker->manifestHash || release.ceremonyHash != marker->ceremonyHash ||
                   release.participantsHash != marker->participantsHash ||
                   release.participantCount != marker->participantCount) {
            throw std::runtime_error("cluster restore participant markers describe different ceremonies");
        }
        release.entries.push_back(ReleaseEntry{marker->self, bytes->size(), fnvString(*bytes)});
    }
    std::ranges::sort(release.entries, {}, &ReleaseEntry::self);
    if (release.entries.size() != release.participantCount)
        throw std::runtime_error("cluster restore release is missing or has extra participant markers");
    std::vector<raft::NodeId> participants;
    participants.reserve(release.entries.size());
    for (size_t i = 0; i < release.entries.size(); ++i) {
        if (i != 0 && release.entries[i - 1].self == release.entries[i].self)
            throw std::runtime_error("cluster restore release contains a duplicate participant marker");
        participants.push_back(release.entries[i].self);
    }
    if (hashParticipants(participants) != release.participantsHash)
        throw std::runtime_error("cluster restore release participant set does not match the target serving map");

    const std::string encoded = encodeRelease(release);
    writeFileNoReplaceDurably(output, encoded, kMaxReleaseBytes, "cluster restore release");
}

void ClusterRestoreSeeder::activate(const std::filesystem::path& dataDirectory,
                                    const std::filesystem::path& releaseFile) {
    if (dataDirectory.empty() || releaseFile.empty())
        throw std::invalid_argument("cluster restore activation requires a data directory and release file");
    auto markerBytes = readMarkerFile(markerPath(dataDirectory));
    auto releaseBytes = readBoundedRegularFile(releaseFile, kMaxReleaseBytes, "cluster restore release");
    if (!markerBytes || !releaseBytes)
        throw std::runtime_error("cluster restore activation requires a prepared marker and release");
    auto marker = decodeMarker(*markerBytes);
    auto release = decodeRelease(*releaseBytes);
    if (!marker || !release || !releaseMatchesMarker(*release, *marker, *markerBytes))
        throw std::runtime_error("cluster restore release does not authorize this prepared data root");
    writeFileNoReplaceDurably(releaseReceiptPath(dataDirectory), *releaseBytes, kMaxReleaseBytes,
                              "cluster restore release receipt");
}

}  // namespace timestar::features

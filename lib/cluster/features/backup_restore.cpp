#include "backup_restore.hpp"

#include "../../utils/crc32.hpp"
#include "../data/snapshot_payload.hpp"

#include <fcntl.h>
#include <gnutls/crypto.h>
#include <gnutls/gnutls.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <fstream>
#include <limits>
#include <seastar/core/thread.hh>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace timestar::features {

namespace {

constexpr uint32_t kMagic = 0x4b425354;  // "TSBK" little-endian
constexpr uint32_t kVersion = 1;
constexpr uint32_t kPortableMagic = 0x43505354;  // "TSPC" little-endian
constexpr size_t kHashHexBytes = 32;
constexpr size_t kAuthenticationHexBytes = 64;
constexpr size_t kMaxManifestBytes = 64u << 20;
constexpr size_t kMaxPortableControlEncodedBytes = kMaxManifestBytes - (size_t{1} << 20);
// Schema CAS cells share Group-0's policy map with retention cells, but only
// the retention subset has the 1,024-policy semantic limit. Keep the portable
// decoder's allocation independently bounded without misapplying that limit to
// schema state.
constexpr size_t kMaxPortablePolicies = 1u << 16;
constexpr std::string_view kManifestFilename = "manifest.tsbk1";
constexpr std::string_view kManifestTemporaryFilename = ".manifest.tsbk1.partial";

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
    int get() const { return fd_; }
    int release() { return std::exchange(fd_, -1); }

private:
    int fd_;
};

[[noreturn]] void throwIo(const std::string& operation, const std::filesystem::path& path) {
    throw std::system_error(errno, std::generic_category(), operation + ": " + path.string());
}

void closeChecked(UniqueFd& fd, const std::filesystem::path& path) {
    const int raw = fd.release();
    // On Linux the descriptor is released even when close reports EINTR.
    // Retrying could therefore close an unrelated descriptor reused by another
    // thread between calls.
    if (::close(raw) < 0 && errno != EINTR)
        throwIo("close backup archive file", path);
}

void fsyncDirectory(const std::filesystem::path& path) {
    UniqueFd fd(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.get() < 0)
        throwIo("open backup archive directory", path);
    int rc = 0;
    do {
        rc = ::fsync(fd.get());
    } while (rc < 0 && errno == EINTR);
    if (rc < 0)
        throwIo("fsync backup archive directory", path);
    closeChecked(fd, path);
}

bool isDirectoryNoSymlink(const std::filesystem::path& path) {
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    return !ec && status.type() == std::filesystem::file_type::directory;
}

std::filesystem::file_status statusAllowNotFound(const std::filesystem::path& path, std::error_code& ec) {
    auto status = std::filesystem::symlink_status(path, ec);
    if (ec == std::errc::no_such_file_or_directory) {
        ec.clear();
        status = std::filesystem::file_status(std::filesystem::file_type::not_found);
    }
    return status;
}

bool isRegularNoSymlink(const std::filesystem::path& path) {
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    return !ec && status.type() == std::filesystem::file_type::regular;
}

bool pathAbsent(const std::filesystem::path& path) {
    std::error_code ec;
    const auto status = statusAllowNotFound(path, ec);
    return !ec && status.type() == std::filesystem::file_type::not_found;
}

void ensureArchiveDirectories(const std::filesystem::path& root) {
    if (root.empty())
        throw std::invalid_argument("backup archive directory is empty");
    std::error_code ec;
    const auto rootStatus = statusAllowNotFound(root, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup archive directory", root, ec);
    if (rootStatus.type() == std::filesystem::file_type::not_found) {
        if (!std::filesystem::create_directory(root))
            throw std::runtime_error("failed to create backup archive directory " + root.string());
        fsyncDirectory(root.parent_path().empty() ? std::filesystem::path(".") : root.parent_path());
    } else if (rootStatus.type() != std::filesystem::file_type::directory) {
        throw std::invalid_argument("backup archive root must be a real directory");
    }

    const auto units = root / "vshards";
    const auto unitStatus = statusAllowNotFound(units, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup VShard directory", units, ec);
    if (unitStatus.type() == std::filesystem::file_type::not_found) {
        if (!std::filesystem::create_directory(units))
            throw std::runtime_error("failed to create backup VShard directory " + units.string());
        fsyncDirectory(root);
    } else if (unitStatus.type() != std::filesystem::file_type::directory) {
        throw std::invalid_argument("backup vshards entry must be a real directory");
    }
}

void writeAll(int fd, const char* data, size_t size, const std::filesystem::path& path) {
    size_t offset = 0;
    while (offset < size) {
        const ssize_t written = ::write(fd, data + offset, size - offset);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            throwIo("write backup archive file", path);
        }
        if (written == 0)
            throw std::runtime_error("zero-byte write to backup archive file " + path.string());
        offset += static_cast<size_t>(written);
    }
}

void writeFileDurably(const std::filesystem::path& path, std::string_view bytes) {
    UniqueFd fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (fd.get() < 0)
        throwIo("create backup archive file", path);
    try {
        writeAll(fd.get(), bytes.data(), bytes.size(), path);
        int rc = 0;
        do {
            rc = ::fsync(fd.get());
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            throwIo("fsync backup archive file", path);
        closeChecked(fd, path);
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        throw;
    }
}

void copyFileDurably(const std::filesystem::path& source, const std::filesystem::path& destination,
                     uint64_t expectedSize) {
    UniqueFd in(::open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (in.get() < 0)
        throwIo("open backup source", source);
    struct stat sourceStat{};
    if (::fstat(in.get(), &sourceStat) < 0)
        throwIo("stat backup source", source);
    if (!S_ISREG(sourceStat.st_mode) || sourceStat.st_size < 0 ||
        static_cast<uint64_t>(sourceStat.st_size) != expectedSize)
        throw std::invalid_argument("backup source changed during staging");

    UniqueFd out(::open(destination.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (out.get() < 0)
        throwIo("create staged backup unit", destination);
    try {
        std::vector<char> buffer(size_t{1} << 20);
        uint64_t copied = 0;
        while (copied < expectedSize) {
            const size_t wanted = static_cast<size_t>(std::min<uint64_t>(buffer.size(), expectedSize - copied));
            ssize_t count = 0;
            do {
                count = ::read(in.get(), buffer.data(), wanted);
            } while (count < 0 && errno == EINTR);
            if (count < 0)
                throwIo("read backup source", source);
            if (count == 0)
                throw std::invalid_argument("backup source was truncated during staging");
            writeAll(out.get(), buffer.data(), static_cast<size_t>(count), destination);
            copied += static_cast<uint64_t>(count);
        }
        char extra = 0;
        ssize_t trailing = 0;
        do {
            trailing = ::read(in.get(), &extra, 1);
        } while (trailing < 0 && errno == EINTR);
        if (trailing < 0)
            throwIo("read backup source trailer", source);
        if (trailing != 0)
            throw std::invalid_argument("backup source grew during staging");
        int rc = 0;
        do {
            rc = ::fsync(out.get());
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            throwIo("fsync staged backup unit", destination);
        closeChecked(out, destination);
        closeChecked(in, source);
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(destination, ec);
        throw;
    }
}

// Publish without replacing an existing name. A hard-link in one directory is
// an atomic appearance boundary; unlinking the private temporary name leaves
// the now-immutable inode reachable through its canonical name.
void publishNoReplace(const std::filesystem::path& temporary, const std::filesystem::path& final) {
    if (::link(temporary.c_str(), final.c_str()) < 0)
        throwIo("publish backup archive file", final);
    fsyncDirectory(final.parent_path());
    if (::unlink(temporary.c_str()) < 0)
        throwIo("unlink backup archive temporary", temporary);
    fsyncDirectory(final.parent_path());
}

std::optional<std::string> readBoundedFile(const std::filesystem::path& path, size_t maximum) {
    if (!isRegularNoSymlink(path))
        return std::nullopt;
    std::error_code ec;
    const uint64_t size = std::filesystem::file_size(path, ec);
    if (ec || size > maximum)
        return std::nullopt;
    std::string bytes(static_cast<size_t>(size), '\0');
    std::ifstream input(path, std::ios::binary);
    if (!input || (size != 0 && !input.read(bytes.data(), static_cast<std::streamsize>(size))))
        return std::nullopt;
    char extra = 0;
    if (input.read(&extra, 1) || input.gcount() != 0)
        return std::nullopt;
    return bytes;
}

bool canonicalHex128(std::string_view value) {
    return value.size() == 32 &&
           std::ranges::all_of(value, [](unsigned char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

bool canonicalHex256(std::string_view value) {
    return value.size() == kAuthenticationHexBytes &&
           std::ranges::all_of(value, [](unsigned char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

unsigned char decodeHexNibble(unsigned char value) {
    return value <= '9' ? static_cast<unsigned char>(value - '0') : static_cast<unsigned char>(value - 'a' + 10);
}

std::array<unsigned char, 32> decodeHex256(std::string_view value) {
    if (!canonicalHex256(value))
        throw std::invalid_argument("cluster backup authentication key must be 64 lowercase hexadecimal characters");
    std::array<unsigned char, 32> out{};
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<unsigned char>((decodeHexNibble(static_cast<unsigned char>(value[2 * i])) << 4) |
                                            decodeHexNibble(static_cast<unsigned char>(value[2 * i + 1])));
    return out;
}

std::string encodeHex256(const std::array<unsigned char, 32>& value) {
    static constexpr std::string_view digits = "0123456789abcdef";
    std::string out(value.size() * 2, '0');
    for (size_t i = 0; i < value.size(); ++i) {
        out[2 * i] = digits[value[i] >> 4];
        out[2 * i + 1] = digits[value[i] & 0x0f];
    }
    return out;
}

void putU16(std::string& out, uint16_t value) {
    for (int i = 0; i < 2; ++i)
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
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
        throw std::length_error("TSBK v1 blob exceeds uint32 framing");
    putU32(out, static_cast<uint32_t>(value.size()));
    out.append(value);
}

class Reader {
public:
    explicit Reader(std::span<const char> bytes) : bytes_(bytes) {}

    uint8_t u8() {
        if (!take(1))
            return 0;
        return static_cast<uint8_t>(bytes_[offset_ - 1]);
    }
    uint16_t u16() {
        uint16_t out = 0;
        for (int i = 0; i < 2; ++i)
            out |= static_cast<uint16_t>(u8()) << (8 * i);
        return out;
    }
    uint32_t u32() {
        uint32_t out = 0;
        for (int i = 0; i < 4; ++i)
            out |= static_cast<uint32_t>(u8()) << (8 * i);
        return out;
    }
    uint64_t u64() {
        uint64_t out = 0;
        for (int i = 0; i < 8; ++i)
            out |= static_cast<uint64_t>(u8()) << (8 * i);
        return out;
    }
    std::string blob(size_t limit) {
        const uint32_t size = u32();
        if (!ok_ || size > limit || !take(size)) {
            ok_ = false;
            return {};
        }
        return std::string(bytes_.data() + offset_ - size, size);
    }
    bool ok() const { return ok_; }
    bool exhausted() const { return ok_ && offset_ == bytes_.size(); }
    size_t remaining() const { return ok_ ? bytes_.size() - offset_ : 0; }

private:
    bool take(size_t size) {
        if (!ok_ || size > bytes_.size() - offset_) {
            ok_ = false;
            return false;
        }
        offset_ += size;
        return true;
    }

    std::span<const char> bytes_;
    size_t offset_ = 0;
    bool ok_ = true;
};

void putFrozenPlan(std::string& out, const control::FrozenDeletePlan& plan) {
    putBlob(out, plan.requestId);
    putBlob(out, plan.requestFingerprint);
    putU64(out, plan.issuedAtMs);
    putU32(out, static_cast<uint32_t>(plan.targets.size()));
    for (const auto& target : plan.targets) {
        putBlob(out, target.seriesKey);
        putU64(out, target.startTime);
        putU64(out, target.endTime);
    }
}

std::optional<control::FrozenDeletePlan> readFrozenPlan(Reader& reader) {
    control::FrozenDeletePlan plan;
    plan.requestId = reader.blob(kHashHexBytes);
    plan.requestFingerprint = reader.blob(kHashHexBytes);
    plan.issuedAtMs = reader.u64();
    const uint32_t count = reader.u32();
    if (!reader.ok() || count > control::kMaxFrozenDeletePlanTargets || count > reader.remaining() / 20)
        return std::nullopt;
    for (uint32_t i = 0; i < count; ++i) {
        control::FrozenDeleteTarget target;
        target.seriesKey = reader.blob(control::kMaxFrozenDeletePlanBytes);
        target.startTime = reader.u64();
        target.endTime = reader.u64();
        if (!reader.ok())
            return std::nullopt;
        plan.targets.push_back(std::move(target));
    }
    if (!control::validFrozenDeletePlan(plan))
        return std::nullopt;
    return plan;
}

bool validPolicy(const std::string& key, const control::PolicyCell& cell) {
    return !key.empty() && key.size() <= control::kMaxPolicyKeyBytes && cell.version != 0 &&
           cell.value.size() <= control::kMaxPolicyValueBytes;
}

}  // namespace

ClusterBackupAuthenticationKey ClusterBackupAuthenticationKey::load(const std::filesystem::path& path) {
    if (path.empty())
        throw std::invalid_argument("cluster backup authentication key file is not configured");
    UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.get() < 0)
        throwIo("open cluster backup authentication key", path);
    struct stat metadata{};
    if (::fstat(fd.get(), &metadata) < 0)
        throwIo("stat cluster backup authentication key", path);
    if (!S_ISREG(metadata.st_mode) || metadata.st_uid != ::geteuid() || (metadata.st_mode & 0077) != 0 ||
        (metadata.st_size != 64 && metadata.st_size != 65))
        throw std::invalid_argument(
            "cluster backup authentication key must be an owner-only regular file owned by the server user");

    std::string encoded(static_cast<size_t>(metadata.st_size), '\0');
    size_t offset = 0;
    while (offset < encoded.size()) {
        ssize_t count = ::read(fd.get(), encoded.data() + offset, encoded.size() - offset);
        if (count < 0) {
            if (errno == EINTR)
                continue;
            throwIo("read cluster backup authentication key", path);
        }
        if (count == 0)
            throw std::invalid_argument("cluster backup authentication key was truncated while reading");
        offset += static_cast<size_t>(count);
    }
    char extra = 0;
    ssize_t trailing = 0;
    do {
        trailing = ::read(fd.get(), &extra, 1);
    } while (trailing < 0 && errno == EINTR);
    if (trailing < 0)
        throwIo("read cluster backup authentication key trailer", path);
    if (trailing != 0)
        throw std::invalid_argument("cluster backup authentication key changed while reading");
    closeChecked(fd, path);
    if (encoded.size() == 65) {
        if (encoded.back() != '\n')
            throw std::invalid_argument("cluster backup authentication key has invalid trailing data");
        encoded.pop_back();
    }
    return fromHex(encoded);
}

ClusterBackupAuthenticationKey ClusterBackupAuthenticationKey::fromHex(std::string_view hex) {
    auto bytes = decodeHex256(hex);
    if (std::ranges::all_of(bytes, [](unsigned char value) { return value == 0; }))
        throw std::invalid_argument("cluster backup authentication key must not be all zero");
    return ClusterBackupAuthenticationKey(bytes);
}

std::string ClusterBackupAuthenticationKey::keyId() const {
    std::array<unsigned char, 32> digest{};
    if (gnutls_hash_fast(GNUTLS_DIG_SHA256, bytes_.data(), bytes_.size(), digest.data()) < 0)
        throw std::runtime_error("failed to derive cluster backup authentication key identifier");
    return encodeHex256(digest);
}

std::string ClusterBackupAuthenticationKey::authenticate(std::string_view bytes) const {
    std::array<unsigned char, 32> tag{};
    if (gnutls_hmac_fast(GNUTLS_MAC_SHA256, bytes_.data(), bytes_.size(), bytes.data(), bytes.size(), tag.data()) < 0)
        throw std::runtime_error("failed to authenticate cluster backup manifest");
    return encodeHex256(tag);
}

bool ClusterBackupAuthenticationKey::verifies(std::string_view bytes, std::string_view lowercaseHexTag) const {
    if (!canonicalHex256(lowercaseHexTag))
        return false;
    const auto supplied = decodeHex256(lowercaseHexTag);
    const auto expected = decodeHex256(authenticate(bytes));
    return gnutls_memcmp(supplied.data(), expected.data(), supplied.size()) == 0;
}

bool PortableControlBackup::valid() const {
    if (policies.size() > kMaxPortablePolicies || retentionCutoffs.size() > control::kMaxRetentionPolicies ||
        frozenDeletePlans.size() > control::kMaxFrozenDeletePlans)
        return false;
    size_t encodedBytes = 4 + 8 + 4 + 4;  // three counts plus lastRetentionSweepId
    const auto addEncoded = [&encodedBytes](size_t bytes) {
        if (bytes > kMaxPortableControlEncodedBytes - encodedBytes)
            return false;
        encodedBytes += bytes;
        return true;
    };
    size_t retentionPolicies = 0;
    for (const auto& [key, cell] : policies) {
        if (!validPolicy(key, cell) || !addEncoded(4 + key.size() + 8 + 4 + cell.value.size()))
            return false;
        const auto measurement = control::retentionMeasurementFromKey(key);
        if (key.starts_with(control::kRetentionPolicyPrefix) && !measurement)
            return false;
        if (measurement) {
            ++retentionPolicies;
            if ((!cell.value.empty() && !control::decodeRetentionPolicyValue(cell.value)) ||
                !control::validRetentionMeasurement(*measurement))
                return false;
        }
    }
    if (retentionPolicies > control::kMaxRetentionPolicies || (lastRetentionSweepId == 0) != retentionCutoffs.empty())
        return false;
    for (const auto& [measurement, cutoff] : retentionCutoffs) {
        const auto policy = policies.find(control::retentionPolicyKey(measurement));
        if (!control::validRetentionMeasurement(measurement) || cutoff.policyVersion == 0 || cutoff.cutoffTime == 0 ||
            policy == policies.end() || cutoff.policyVersion > policy->second.version ||
            !addEncoded(4 + measurement.size() + 8 + 8))
            return false;
    }
    size_t frozenBytes = 0;
    for (const auto& [id, plan] : frozenDeletePlans) {
        if (id != plan.requestId || !control::validFrozenDeletePlan(plan))
            return false;
        if (!addEncoded(4 + plan.requestId.size() + 4 + plan.requestFingerprint.size() + 8 + 4))
            return false;
        for (const auto& target : plan.targets)
            if (!addEncoded(4 + target.seriesKey.size() + 8 + 8))
                return false;
        const size_t bytes = control::frozenDeletePlanBytes(plan);
        if (bytes > control::kMaxFrozenDeletePlanAggregateBytes ||
            frozenBytes > control::kMaxFrozenDeletePlanAggregateBytes - bytes)
            return false;
        frozenBytes += bytes;
    }
    return true;
}

std::string PortableControlBackup::encode() const {
    if (!valid())
        throw std::invalid_argument("cannot encode invalid TSPC v1 portable control state");
    std::string out;
    putU32(out, kPortableMagic);
    putU32(out, kVersion);
    putU32(out, static_cast<uint32_t>(policies.size()));
    for (const auto& [key, cell] : policies) {
        putBlob(out, key);
        putU64(out, cell.version);
        putBlob(out, cell.value);
    }
    putU64(out, lastRetentionSweepId);
    putU32(out, static_cast<uint32_t>(retentionCutoffs.size()));
    for (const auto& [measurement, cutoff] : retentionCutoffs) {
        putBlob(out, measurement);
        putU64(out, cutoff.policyVersion);
        putU64(out, cutoff.cutoffTime);
    }
    putU32(out, static_cast<uint32_t>(frozenDeletePlans.size()));
    for (const auto& [id, plan] : frozenDeletePlans) {
        (void)id;
        putFrozenPlan(out, plan);
    }
    putU32(out, CRC32::compute(out.data(), out.size()));
    if (out.size() > kMaxPortableControlEncodedBytes)
        throw std::length_error("TSPC v1 portable control state exceeds its bounded encoding");
    return out;
}

std::optional<PortableControlBackup> PortableControlBackup::decode(std::string_view bytes) {
    if (bytes.size() < 12 || bytes.size() > kMaxPortableControlEncodedBytes)
        return std::nullopt;
    const size_t bodySize = bytes.size() - 4;
    Reader crcReader(std::span<const char>(bytes.data() + bodySize, 4));
    const uint32_t storedCrc = crcReader.u32();
    if (!crcReader.exhausted() || CRC32::compute(bytes.data(), bodySize) != storedCrc)
        return std::nullopt;
    Reader reader(std::span<const char>(bytes.data(), bodySize));
    if (reader.u32() != kPortableMagic || reader.u32() != kVersion)
        return std::nullopt;

    PortableControlBackup out;
    const uint32_t policyCount = reader.u32();
    if (!reader.ok() || policyCount > kMaxPortablePolicies)
        return std::nullopt;
    for (uint32_t i = 0; i < policyCount; ++i) {
        std::string key = reader.blob(control::kMaxPolicyKeyBytes);
        control::PolicyCell cell{reader.u64(), reader.blob(control::kMaxPolicyValueBytes)};
        if (!reader.ok() || !validPolicy(key, cell) || !out.policies.emplace(std::move(key), std::move(cell)).second)
            return std::nullopt;
    }
    out.lastRetentionSweepId = reader.u64();
    const uint32_t cutoffCount = reader.u32();
    if (!reader.ok() || cutoffCount > control::kMaxRetentionPolicies)
        return std::nullopt;
    for (uint32_t i = 0; i < cutoffCount; ++i) {
        std::string measurement = reader.blob(control::kMaxRetentionMeasurementBytes);
        control::RetentionCutoffRecord cutoff{reader.u64(), reader.u64()};
        if (!reader.ok() || !control::validRetentionMeasurement(measurement) || cutoff.policyVersion == 0 ||
            cutoff.cutoffTime == 0 || !out.retentionCutoffs.emplace(std::move(measurement), cutoff).second)
            return std::nullopt;
    }
    const uint32_t planCount = reader.u32();
    if (!reader.ok() || planCount > control::kMaxFrozenDeletePlans)
        return std::nullopt;
    for (uint32_t i = 0; i < planCount; ++i) {
        auto plan = readFrozenPlan(reader);
        if (!plan || !out.frozenDeletePlans.emplace(plan->requestId, std::move(*plan)).second)
            return std::nullopt;
    }
    if (!reader.exhausted() || !out.valid())
        return std::nullopt;
    return out;
}

bool VShardBackupUnit::valid() const {
    return vshard < VIRTUAL_SHARD_COUNT && snapshotRevision != 0 && canonicalHex128(verificationHash) &&
           canonicalHex128(catalogHash) && encodedSize >= 12 && encodedSize <= (uint64_t{1} << 40) &&
           canonicalHex256(encodedSha256);
}

namespace {

std::string encodeManifestAuthenticationPayload(const ClusterBackupManifest& manifest) {
    std::string out;
    out.reserve(512u << 10);
    putU32(out, kMagic);
    putU32(out, kVersion);
    putBlob(out, manifest.authenticationKeyId);
    putBlob(out, manifest.sourceClusterUuid);

    putU32(out, static_cast<uint32_t>(manifest.control.policies.size()));
    for (const auto& [key, cell] : manifest.control.policies) {
        putBlob(out, key);
        putU64(out, cell.version);
        putBlob(out, cell.value);
    }
    putU64(out, manifest.control.lastRetentionSweepId);
    putU32(out, static_cast<uint32_t>(manifest.control.retentionCutoffs.size()));
    for (const auto& [measurement, cutoff] : manifest.control.retentionCutoffs) {
        putBlob(out, measurement);
        putU64(out, cutoff.policyVersion);
        putU64(out, cutoff.cutoffTime);
    }
    putU32(out, static_cast<uint32_t>(manifest.control.frozenDeletePlans.size()));
    for (const auto& [id, plan] : manifest.control.frozenDeletePlans) {
        (void)id;
        putFrozenPlan(out, plan);
    }

    putU32(out, static_cast<uint32_t>(manifest.vshards.size()));
    for (const auto& unit : manifest.vshards) {
        putU16(out, unit.vshard);
        putU64(out, unit.snapshotRevision);
        putBlob(out, unit.verificationHash);
        putBlob(out, unit.catalogHash);
        putU64(out, unit.encodedSize);
        putBlob(out, unit.encodedSha256);
    }
    return out;
}

}  // namespace

bool ClusterBackupManifest::valid() const {
    if (!canonicalHex256(authenticationKeyId) || !canonicalHex256(authenticationTag) ||
        !canonicalHex128(sourceClusterUuid) || !control.valid() || vshards.size() != VIRTUAL_SHARD_COUNT)
        return false;
    for (size_t i = 0; i < vshards.size(); ++i)
        if (!vshards[i].valid() || vshards[i].vshard != i)
            return false;
    return true;
}

void ClusterBackupManifest::authenticate(const ClusterBackupAuthenticationKey& key) {
    authenticationKeyId = key.keyId();
    authenticationTag.clear();
    authenticationTag = key.authenticate(encodeManifestAuthenticationPayload(*this));
}

bool ClusterBackupManifest::authenticatedBy(const ClusterBackupAuthenticationKey& key) const {
    return valid() && authenticationKeyId == key.keyId() &&
           key.verifies(encodeManifestAuthenticationPayload(*this), authenticationTag);
}

std::string ClusterBackupManifest::encode() const {
    if (!valid())
        throw std::invalid_argument("cannot encode invalid TSBK v1 manifest");
    std::string out = encodeManifestAuthenticationPayload(*this);
    putBlob(out, authenticationTag);
    const uint32_t crc = CRC32::compute(out.data(), out.size());
    putU32(out, crc);
    if (out.size() > kMaxManifestBytes)
        throw std::length_error("TSBK v1 manifest exceeds its 64-MiB bound");
    return out;
}

std::optional<ClusterBackupManifest> ClusterBackupManifest::decode(std::string_view bytes) {
    if (bytes.size() < 12 || bytes.size() > kMaxManifestBytes)
        return std::nullopt;
    const size_t bodySize = bytes.size() - 4;
    Reader crcReader(std::span<const char>(bytes.data() + bodySize, 4));
    const uint32_t storedCrc = crcReader.u32();
    if (!crcReader.exhausted() || CRC32::compute(bytes.data(), bodySize) != storedCrc)
        return std::nullopt;
    Reader reader(std::span<const char>(bytes.data(), bodySize));
    if (reader.u32() != kMagic || reader.u32() != kVersion)
        return std::nullopt;

    ClusterBackupManifest out;
    out.authenticationKeyId = reader.blob(kAuthenticationHexBytes);
    out.sourceClusterUuid = reader.blob(32);
    const uint32_t policyCount = reader.u32();
    if (!reader.ok() || policyCount > kMaxPortablePolicies)
        return std::nullopt;
    for (uint32_t i = 0; i < policyCount; ++i) {
        std::string key = reader.blob(control::kMaxPolicyKeyBytes);
        control::PolicyCell cell{reader.u64(), reader.blob(control::kMaxPolicyValueBytes)};
        if (!reader.ok() || !validPolicy(key, cell) ||
            !out.control.policies.emplace(std::move(key), std::move(cell)).second)
            return std::nullopt;
    }
    out.control.lastRetentionSweepId = reader.u64();
    const uint32_t cutoffCount = reader.u32();
    if (!reader.ok() || cutoffCount > control::kMaxRetentionPolicies)
        return std::nullopt;
    for (uint32_t i = 0; i < cutoffCount; ++i) {
        std::string measurement = reader.blob(control::kMaxRetentionMeasurementBytes);
        control::RetentionCutoffRecord cutoff{reader.u64(), reader.u64()};
        if (!reader.ok() || !control::validRetentionMeasurement(measurement) || cutoff.policyVersion == 0 ||
            cutoff.cutoffTime == 0 || !out.control.retentionCutoffs.emplace(std::move(measurement), cutoff).second)
            return std::nullopt;
    }
    const uint32_t planCount = reader.u32();
    if (!reader.ok() || planCount > control::kMaxFrozenDeletePlans)
        return std::nullopt;
    for (uint32_t i = 0; i < planCount; ++i) {
        auto plan = readFrozenPlan(reader);
        if (!plan || !out.control.frozenDeletePlans.emplace(plan->requestId, std::move(*plan)).second)
            return std::nullopt;
    }

    const uint32_t unitCount = reader.u32();
    if (!reader.ok() || unitCount != VIRTUAL_SHARD_COUNT)
        return std::nullopt;
    out.vshards.reserve(VIRTUAL_SHARD_COUNT);
    for (uint32_t i = 0; i < unitCount; ++i) {
        VShardBackupUnit unit;
        unit.vshard = reader.u16();
        unit.snapshotRevision = reader.u64();
        unit.verificationHash = reader.blob(kHashHexBytes);
        unit.catalogHash = reader.blob(kHashHexBytes);
        unit.encodedSize = reader.u64();
        unit.encodedSha256 = reader.blob(kAuthenticationHexBytes);
        if (!reader.ok() || !unit.valid() || unit.vshard != i)
            return std::nullopt;
        out.vshards.push_back(std::move(unit));
    }
    out.authenticationTag = reader.blob(kAuthenticationHexBytes);
    if (!reader.exhausted() || !out.valid())
        return std::nullopt;
    return out;
}

std::optional<PortableControlBackup> BackupRestore::capturePortableControl(const control::Group0State& state) {
    if (state.retentionSweep)
        return std::nullopt;
    PortableControlBackup out;
    out.policies = state.policies;
    out.lastRetentionSweepId = state.lastRetentionSweepId;
    out.retentionCutoffs = state.retentionCutoffs;
    out.frozenDeletePlans = state.frozenDeletePlans;
    if (!out.valid())
        return std::nullopt;
    return out;
}

RestorePlan BackupRestore::planRestore(const ClusterBackupManifest& backup, std::string newClusterUuid,
                                       const ClusterBackupAuthenticationKey& authenticationKey) {
    RestorePlan out;
    out.newClusterUuid = std::move(newClusterUuid);
    if (!backup.authenticatedBy(authenticationKey)) {
        out.error = "restore: invalid, incomplete, or unauthenticated TSBK v1 manifest";
        return out;
    }
    if (!canonicalClusterUuid(out.newClusterUuid)) {
        out.error = "restore: new cluster UUID must be 32 lowercase hexadecimal characters";
        return out;
    }
    if (out.newClusterUuid == backup.sourceClusterUuid) {
        out.error = "restore: new cluster UUID must differ from the source";
        return out;
    }
    out.control = backup.control;
    out.vshards = backup.vshards;
    out.ok = true;
    return out;
}

bool BackupRestore::canonicalClusterUuid(std::string_view uuid) {
    return canonicalHex128(uuid);
}

std::string BackupRestore::unitRelativePath(uint16_t vshard) {
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("backup VShard is outside [0,4096)");
    constexpr std::array<unsigned, 4> divisors{1000, 100, 10, 1};
    std::string out = "vshards/";
    for (unsigned divisor : divisors)
        out.push_back(static_cast<char>('0' + (vshard / divisor) % 10));
    out += ".tsp1";
    return out;
}

namespace {

VShardBackupUnit unitFromInfo(uint16_t vshard, const data::SnapshotPayloadFileInfo& info) {
    return VShardBackupUnit{vshard,
                            info.manifest.snapshotRevision,
                            info.manifest.verificationHash,
                            info.manifest.catalogHash,
                            info.encodedSize,
                            info.encodedSha256};
}

bool infoMatchesUnit(const data::SnapshotPayloadFileInfo& info, const VShardBackupUnit& unit) {
    return info.manifest.vshard.value() == unit.vshard && info.manifest.snapshotRevision == unit.snapshotRevision &&
           info.manifest.verificationHash == unit.verificationHash && info.manifest.catalogHash == unit.catalogHash &&
           info.encodedSize == unit.encodedSize && info.encodedSha256 == unit.encodedSha256;
}

std::optional<uint16_t> parseCanonicalUnitFilename(std::string_view name) {
    if (name.size() != 9 || name.substr(4) != ".tsp1")
        return std::nullopt;
    uint16_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        if (name[i] < '0' || name[i] > '9')
            return std::nullopt;
        value = static_cast<uint16_t>(value * 10 + (name[i] - '0'));
    }
    if (value >= VIRTUAL_SHARD_COUNT ||
        std::filesystem::path(BackupRestore::unitRelativePath(value)).filename().string() != name)
        return std::nullopt;
    return value;
}

bool archiveShapeValid(const std::filesystem::path& root, bool requireManifest) {
    if (!isDirectoryNoSymlink(root) || !isDirectoryNoSymlink(root / "vshards"))
        return false;
    bool sawManifest = false;
    bool sawVshards = false;
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        const std::string name = entry.path().filename().string();
        if (name == kManifestFilename) {
            if (sawManifest || !isRegularNoSymlink(entry.path()))
                return false;
            sawManifest = true;
        } else if (name == "vshards") {
            if (sawVshards || !isDirectoryNoSymlink(entry.path()))
                return false;
            sawVshards = true;
        } else {
            return false;
        }
    }
    if (!sawVshards || sawManifest != requireManifest)
        return false;

    std::array<bool, VIRTUAL_SHARD_COUNT> seen{};
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(root / "vshards")) {
        if (!isRegularNoSymlink(entry.path()))
            return false;
        const auto parsed = parseCanonicalUnitFilename(entry.path().filename().string());
        if (!parsed || seen[*parsed])
            return false;
        seen[*parsed] = true;
        ++count;
    }
    return count == VIRTUAL_SHARD_COUNT;
}

seastar::future<bool> validateUnits(const std::filesystem::path& root, const ClusterBackupManifest& manifest) {
    for (const auto& unit : manifest.vshards) {
        auto info = co_await data::inspectSnapshotPayloadFile(root / BackupRestore::unitRelativePath(unit.vshard));
        if (!info || !infoMatchesUnit(*info, unit))
            co_return false;
    }
    co_return true;
}

}  // namespace

std::string ClusterBackupArchive::manifestRelativePath() {
    return std::string(kManifestFilename);
}

seastar::future<std::optional<VShardBackupUnit>> ClusterBackupArchive::stageVShard(
    const std::filesystem::path& archiveDirectory, uint16_t vshard, const std::filesystem::path& sourceTsp1) {
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("backup VShard is outside [0,4096)");
    if (!isRegularNoSymlink(sourceTsp1))
        co_return std::nullopt;
    auto sourceInfo = co_await data::inspectSnapshotPayloadFile(sourceTsp1);
    if (!sourceInfo || sourceInfo->manifest.vshard.value() != vshard)
        co_return std::nullopt;
    const VShardBackupUnit sourceUnit = unitFromInfo(vshard, *sourceInfo);

    co_await seastar::async([archiveDirectory] { ensureArchiveDirectories(archiveDirectory); });
    const auto manifestPath = archiveDirectory / kManifestFilename;
    if (!pathAbsent(manifestPath))
        co_return std::nullopt;

    const auto finalPath = archiveDirectory / BackupRestore::unitRelativePath(vshard);
    const auto temporaryPath = finalPath.string() + ".partial";
    co_await seastar::async([temporaryPath, finalPath] {
        std::error_code ec;
        const auto temporaryStatus = statusAllowNotFound(temporaryPath, ec);
        if (ec)
            throw std::filesystem::filesystem_error("inspect staged backup temporary", temporaryPath, ec);
        if (temporaryStatus.type() != std::filesystem::file_type::not_found) {
            if (temporaryStatus.type() != std::filesystem::file_type::regular)
                throw std::invalid_argument("staged backup temporary must not be a symlink or directory");
            if (!std::filesystem::remove(temporaryPath))
                throw std::runtime_error("failed to remove interrupted backup temporary " + temporaryPath);
            fsyncDirectory(finalPath.parent_path());
        }
    });
    if (!pathAbsent(finalPath)) {
        if (!isRegularNoSymlink(finalPath))
            co_return std::nullopt;
        auto existing = co_await data::inspectSnapshotPayloadFile(finalPath);
        if (!existing || !infoMatchesUnit(*existing, sourceUnit))
            co_return std::nullopt;
        co_return sourceUnit;
    }

    co_await seastar::async([sourceTsp1, temporaryPath, finalPath, expectedSize = sourceInfo->encodedSize] {
        copyFileDurably(sourceTsp1, temporaryPath, expectedSize);
        publishNoReplace(temporaryPath, finalPath);
    });

    auto copiedInfo = co_await data::inspectSnapshotPayloadFile(finalPath);
    if (!copiedInfo || !infoMatchesUnit(*copiedInfo, sourceUnit)) {
        co_await seastar::async([finalPath] {
            std::error_code ec;
            if (!std::filesystem::remove(finalPath, ec) || ec)
                throw std::filesystem::filesystem_error("remove invalid staged backup unit", finalPath, ec);
            fsyncDirectory(finalPath.parent_path());
        });
        co_return std::nullopt;
    }
    co_return sourceUnit;
}

seastar::future<bool> ClusterBackupArchive::publish(const std::filesystem::path& archiveDirectory,
                                                    const ClusterBackupManifest& manifest,
                                                    const ClusterBackupAuthenticationKey& authenticationKey) {
    if (!manifest.authenticatedBy(authenticationKey))
        co_return false;
    const auto manifestPath = archiveDirectory / kManifestFilename;
    const auto temporaryPath = archiveDirectory / kManifestTemporaryFilename;
    co_await seastar::async([archiveDirectory, temporaryPath] {
        ensureArchiveDirectories(archiveDirectory);
        std::error_code ec;
        const auto status = statusAllowNotFound(temporaryPath, ec);
        if (ec)
            throw std::filesystem::filesystem_error("inspect backup manifest temporary", temporaryPath, ec);
        if (status.type() != std::filesystem::file_type::not_found) {
            if (status.type() != std::filesystem::file_type::regular)
                throw std::invalid_argument("backup manifest temporary must not be a symlink or directory");
            if (!std::filesystem::remove(temporaryPath))
                throw std::runtime_error("failed to remove interrupted backup manifest temporary");
            fsyncDirectory(archiveDirectory);
        }
    });
    if (!pathAbsent(manifestPath)) {
        auto existing = co_await validate(archiveDirectory, authenticationKey);
        co_return existing && *existing == manifest;
    }
    if (!co_await seastar::async([archiveDirectory] { return archiveShapeValid(archiveDirectory, false); }))
        co_return false;
    if (!co_await validateUnits(archiveDirectory, manifest))
        co_return false;

    const std::string encoded = manifest.encode();
    co_await seastar::async([temporaryPath, manifestPath, encoded] {
        writeFileDurably(temporaryPath, encoded);
        publishNoReplace(temporaryPath, manifestPath);
    });
    co_return true;
}

seastar::future<std::optional<ClusterBackupManifest>> ClusterBackupArchive::validate(
    const std::filesystem::path& archiveDirectory, const ClusterBackupAuthenticationKey& authenticationKey) {
    const auto bytes = co_await seastar::async(
        [path = archiveDirectory / kManifestFilename] { return readBoundedFile(path, kMaxManifestBytes); });
    if (!bytes)
        co_return std::nullopt;
    auto manifest = ClusterBackupManifest::decode(*bytes);
    if (!manifest || !manifest->authenticatedBy(authenticationKey))
        co_return std::nullopt;
    if (!co_await seastar::async([archiveDirectory] { return archiveShapeValid(archiveDirectory, true); }))
        co_return std::nullopt;
    if (!co_await validateUnits(archiveDirectory, *manifest))
        co_return std::nullopt;
    co_return manifest;
}

}  // namespace timestar::features

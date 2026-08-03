#include "cluster_backup_export.hpp"

#include "../../utils/crc32.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <fstream>
#include <limits>
#include <span>
#include <stdexcept>
#include <system_error>

namespace timestar::features {

namespace {

constexpr uint32_t kMagic = 0x58454254;  // "TBEX" little-endian
constexpr uint32_t kVersion = 1;
constexpr size_t kMaximumCheckpointBytes = 80u << 20;
constexpr std::string_view kCheckpointName = "checkpoint.tbex1";
constexpr std::string_view kCheckpointTemporaryName = ".checkpoint.tbex1.partial";
constexpr std::string_view kLockName = ".checkpoint.tbex1.lock";

[[noreturn]] void throwIo(const std::string& operation, const std::filesystem::path& path);

class ScopedFileLock {
public:
    explicit ScopedFileLock(const std::filesystem::path& path) {
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd_ < 0)
            throwIo("open backup-export checkpoint lock", path);
        int rc = 0;
        do {
            rc = ::flock(fd_, LOCK_EX);
        } while (rc < 0 && errno == EINTR);
        if (rc < 0) {
            const int error = errno;
            ::close(fd_);
            fd_ = -1;
            errno = error;
            throwIo("lock backup-export checkpoint", path);
        }
    }
    ScopedFileLock(const ScopedFileLock&) = delete;
    ScopedFileLock& operator=(const ScopedFileLock&) = delete;
    ~ScopedFileLock() {
        if (fd_ >= 0)
            ::close(fd_);
    }

private:
    int fd_ = -1;
};

[[noreturn]] void throwIo(const std::string& operation, const std::filesystem::path& path) {
    throw std::system_error(errno, std::generic_category(), operation + ": " + path.string());
}

void putU32(std::string& out, uint32_t value) {
    for (unsigned i = 0; i < 4; ++i)
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

void putBlob(std::string& out, std::string_view value) {
    if (value.size() > UINT32_MAX)
        throw std::length_error("TBEX v1 blob exceeds uint32 framing");
    putU32(out, static_cast<uint32_t>(value.size()));
    out.append(value);
}

class Reader {
public:
    explicit Reader(std::span<const char> bytes) : bytes_(bytes) {}

    uint32_t u32() {
        uint32_t value = 0;
        for (unsigned i = 0; i < 4; ++i) {
            if (offset_ == bytes_.size()) {
                ok_ = false;
                return 0;
            }
            value |= static_cast<uint32_t>(static_cast<uint8_t>(bytes_[offset_++])) << (8 * i);
        }
        return value;
    }
    std::string blob(size_t maximum) {
        const uint32_t size = u32();
        if (!ok_ || size > maximum || size > bytes_.size() - offset_) {
            ok_ = false;
            return {};
        }
        std::string value(bytes_.data() + offset_, size);
        offset_ += size;
        return value;
    }
    bool exhausted() const { return ok_ && offset_ == bytes_.size(); }

private:
    std::span<const char> bytes_;
    size_t offset_ = 0;
    bool ok_ = true;
};

bool canonicalHex128(std::string_view value) {
    if (value.size() != 32)
        return false;
    for (unsigned char c : value)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
    return true;
}

void syncFd(int fd, const std::string& operation, const std::filesystem::path& path) {
    int rc = 0;
    do {
        rc = ::fsync(fd);
    } while (rc < 0 && errno == EINTR);
    if (rc < 0)
        throwIo(operation, path);
}

void closeFd(int fd, const std::string& operation, const std::filesystem::path& path) {
    if (::close(fd) < 0 && errno != EINTR)
        throwIo(operation, path);
}

void syncDirectory(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        throwIo("open backup-export directory", path);
    try {
        syncFd(fd, "fsync backup-export directory", path);
        const int completedFd = fd;
        fd = -1;
        closeFd(completedFd, "close backup-export directory", path);
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        throw;
    }
}

std::filesystem::file_status statusAllowMissing(const std::filesystem::path& path, std::error_code& ec) {
    auto status = std::filesystem::symlink_status(path, ec);
    if (ec == std::errc::no_such_file_or_directory) {
        ec.clear();
        status = std::filesystem::file_status(std::filesystem::file_type::not_found);
    }
    return status;
}

std::optional<std::string> readBoundedRegularFile(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        if (errno == ENOENT)
            return std::nullopt;
        throwIo("open backup-export checkpoint", path);
    }
    try {
        struct stat st{};
        if (::fstat(fd, &st) < 0)
            throwIo("stat backup-export checkpoint", path);
        if (!S_ISREG(st.st_mode) || st.st_size < 0 || static_cast<uint64_t>(st.st_size) > kMaximumCheckpointBytes)
            throw std::runtime_error("backup-export checkpoint has an invalid type or size: " + path.string());
        std::string bytes(static_cast<size_t>(st.st_size), '\0');
        size_t offset = 0;
        while (offset < bytes.size()) {
            const ssize_t count = ::read(fd, bytes.data() + offset, bytes.size() - offset);
            if (count < 0) {
                if (errno == EINTR)
                    continue;
                throwIo("read backup-export checkpoint", path);
            }
            if (count == 0)
                throw std::runtime_error("backup-export checkpoint was truncated while reading: " + path.string());
            offset += static_cast<size_t>(count);
        }
        closeFd(fd, "close backup-export checkpoint", path);
        fd = -1;
        return bytes;
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        throw;
    }
}

void writeAll(int fd, std::string_view bytes, const std::filesystem::path& path) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR)
                continue;
            throwIo("write backup-export checkpoint", path);
        }
        if (count == 0)
            throw std::runtime_error("zero-byte write to backup-export checkpoint: " + path.string());
        offset += static_cast<size_t>(count);
    }
}

}  // namespace

bool ClusterBackupExportCheckpoint::valid() const {
    return canonicalOperationId(operationId) && BackupRestore::canonicalClusterUuid(sourceClusterUuid) &&
           control::isCompleteControlMap(servingMap) && control.valid();
}

bool ClusterBackupExportCheckpoint::canonicalOperationId(std::string_view value) {
    return canonicalHex128(value) && value.find_first_not_of('0') != std::string_view::npos;
}

std::string ClusterBackupExportCheckpoint::encode() const {
    if (!valid())
        throw std::invalid_argument("cannot encode invalid TBEX v1 checkpoint");
    control::ControlMapCache mapCodec;
    if (!mapCodec.update(servingMap))
        throw std::invalid_argument("cannot encode invalid TBEX v1 serving map");
    const std::string mapBytes = mapCodec.serialize();
    const std::string controlBytes = control.encode();
    std::string out;
    out.reserve(80 + mapBytes.size() + controlBytes.size());
    putU32(out, kMagic);
    putU32(out, kVersion);
    putBlob(out, operationId);
    putBlob(out, sourceClusterUuid);
    putBlob(out, mapBytes);
    putBlob(out, controlBytes);
    putU32(out, CRC32::compute(out.data(), out.size()));
    if (out.size() > kMaximumCheckpointBytes)
        throw std::length_error("TBEX v1 checkpoint exceeds its 80-MiB bound");
    return out;
}

std::optional<ClusterBackupExportCheckpoint> ClusterBackupExportCheckpoint::decode(std::string_view bytes) {
    if (bytes.size() < 12 || bytes.size() > kMaximumCheckpointBytes)
        return std::nullopt;
    const size_t bodyBytes = bytes.size() - 4;
    Reader crc(std::span<const char>(bytes.data() + bodyBytes, 4));
    const uint32_t storedCrc = crc.u32();
    if (!crc.exhausted() || CRC32::compute(bytes.data(), bodyBytes) != storedCrc)
        return std::nullopt;
    Reader reader(std::span<const char>(bytes.data(), bodyBytes));
    if (reader.u32() != kMagic || reader.u32() != kVersion)
        return std::nullopt;
    ClusterBackupExportCheckpoint out;
    out.operationId = reader.blob(32);
    out.sourceClusterUuid = reader.blob(32);
    const std::string mapBytes = reader.blob(16u << 20);
    const std::string controlBytes = reader.blob(64u << 20);
    control::ControlMapCache mapCodec;
    auto portable = PortableControlBackup::decode(controlBytes);
    if (!reader.exhausted() || !mapCodec.load(mapBytes) || !portable)
        return std::nullopt;
    out.servingMap = mapCodec.current();
    out.control = std::move(*portable);
    return out.valid() ? std::optional<ClusterBackupExportCheckpoint>(std::move(out)) : std::nullopt;
}

DurableClusterBackupExport::DurableClusterBackupExport(std::filesystem::path archiveDirectory)
    : archiveDirectory_(std::move(archiveDirectory)),
      stateDirectory_(archiveDirectory_.string() + ".export.v1"),
      checkpointPath_(stateDirectory_ / kCheckpointName) {
    const auto filename = archiveDirectory_.filename();
    if (archiveDirectory_.empty() || filename.empty() || filename == "." || filename == "..")
        throw std::invalid_argument("backup archive path must name a directory");
}

void DurableClusterBackupExport::ensureStateDirectory() const {
    const auto parent =
        stateDirectory_.parent_path().empty() ? std::filesystem::path{"."} : stateDirectory_.parent_path();
    std::error_code ec;
    if (std::filesystem::symlink_status(parent, ec).type() != std::filesystem::file_type::directory || ec)
        throw std::invalid_argument("backup-export parent must be a real directory");
    const auto status = statusAllowMissing(stateDirectory_, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup-export state directory", stateDirectory_, ec);
    if (status.type() == std::filesystem::file_type::not_found) {
        if (std::filesystem::create_directory(stateDirectory_)) {
            syncDirectory(parent);
        } else {
            const auto racedStatus = std::filesystem::symlink_status(stateDirectory_, ec);
            if (ec || racedStatus.type() != std::filesystem::file_type::directory)
                throw std::runtime_error("failed to create backup-export state directory " + stateDirectory_.string());
        }
    } else if (status.type() != std::filesystem::file_type::directory) {
        throw std::invalid_argument("backup-export state path must be a real directory");
    }
}

std::optional<ClusterBackupExportCheckpoint> DurableClusterBackupExport::load() const {
    std::error_code ec;
    const auto stateStatus = statusAllowMissing(stateDirectory_, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup-export state directory", stateDirectory_, ec);
    if (stateStatus.type() == std::filesystem::file_type::not_found)
        return std::nullopt;
    if (stateStatus.type() != std::filesystem::file_type::directory)
        throw std::runtime_error("backup-export state path is not a real directory: " + stateDirectory_.string());
    const auto bytes = readBoundedRegularFile(checkpointPath_);
    if (!bytes)
        return std::nullopt;
    auto decoded = ClusterBackupExportCheckpoint::decode(*bytes);
    if (!decoded)
        throw std::runtime_error("backup-export checkpoint is corrupt or unsupported: " + checkpointPath_.string());
    return decoded;
}

ClusterBackupExportCheckpoint DurableClusterBackupExport::createOrLoad(
    const ClusterBackupExportCheckpoint& desired) const {
    if (!desired.valid())
        throw std::invalid_argument("backup-export checkpoint is invalid");
    ensureStateDirectory();
    ScopedFileLock lock(stateDirectory_ / kLockName);
    if (auto existing = load()) {
        if (*existing != desired)
            throw std::invalid_argument("backup archive belongs to a different export operation or Group-0 fence");
        return *existing;
    }

    std::error_code archiveError;
    const auto archiveStatus = statusAllowMissing(archiveDirectory_, archiveError);
    if (archiveError)
        throw std::filesystem::filesystem_error("inspect new backup archive", archiveDirectory_, archiveError);
    if (archiveStatus.type() != std::filesystem::file_type::not_found) {
        if (archiveStatus.type() != std::filesystem::file_type::directory)
            throw std::invalid_argument("new backup archive path must be absent or a real empty directory");
        if (std::filesystem::directory_iterator(archiveDirectory_) != std::filesystem::directory_iterator{})
            throw std::invalid_argument("refusing to adopt an existing backup archive without a TBEX v1 checkpoint");
    }

    const std::filesystem::path temporary = stateDirectory_ / kCheckpointTemporaryName;
    std::error_code ec;
    const auto temporaryStatus = statusAllowMissing(temporary, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup-export checkpoint temporary", temporary, ec);
    if (temporaryStatus.type() != std::filesystem::file_type::not_found) {
        if (temporaryStatus.type() != std::filesystem::file_type::regular || !std::filesystem::remove(temporary))
            throw std::runtime_error("backup-export checkpoint temporary is not removable regular state");
        syncDirectory(stateDirectory_);
    }

    const std::string bytes = desired.encode();
    int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0)
        throwIo("create backup-export checkpoint temporary", temporary);
    try {
        writeAll(fd, bytes, temporary);
        syncFd(fd, "fsync backup-export checkpoint temporary", temporary);
        const int completedFd = fd;
        fd = -1;
        closeFd(completedFd, "close backup-export checkpoint temporary", temporary);
        if (::link(temporary.c_str(), checkpointPath_.c_str()) < 0)
            throwIo("publish backup-export checkpoint", checkpointPath_);
        syncDirectory(stateDirectory_);
        if (::unlink(temporary.c_str()) < 0)
            throwIo("unlink backup-export checkpoint temporary", temporary);
        syncDirectory(stateDirectory_);
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        ::unlink(temporary.c_str());
        throw;
    }
    return desired;
}

std::filesystem::path DurableClusterBackupExport::prepareDownload(uint16_t vshard) const {
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("backup-export VShard is outside [0,4096)");
    ensureStateDirectory();
    for (const auto& entry : std::filesystem::directory_iterator(stateDirectory_)) {
        const std::string name = entry.path().filename().string();
        if (name == kCheckpointName || name == kLockName)
            continue;
        if (!name.ends_with(".tsp1.partial") || entry.symlink_status().type() != std::filesystem::file_type::regular)
            throw std::runtime_error("backup-export state directory contains an unexpected entry: " + name);
        if (!std::filesystem::remove(entry.path()))
            throw std::runtime_error("failed to remove interrupted backup-export download " + entry.path().string());
    }
    syncDirectory(stateDirectory_);
    return stateDirectory_ /
           (std::filesystem::path(BackupRestore::unitRelativePath(vshard)).filename().string() + ".partial");
}

void DurableClusterBackupExport::removeDownload(const std::filesystem::path& path) const {
    if (path.parent_path() != stateDirectory_ || !path.filename().string().ends_with(".tsp1.partial"))
        throw std::invalid_argument("refusing to remove a non-export download path");
    std::error_code ec;
    const auto status = statusAllowMissing(path, ec);
    if (ec)
        throw std::filesystem::filesystem_error("inspect backup-export download", path, ec);
    if (status.type() == std::filesystem::file_type::not_found)
        return;
    if (status.type() != std::filesystem::file_type::regular || !std::filesystem::remove(path))
        throw std::runtime_error("backup-export download is not a removable regular file: " + path.string());
    syncDirectory(stateDirectory_);
}

}  // namespace timestar::features

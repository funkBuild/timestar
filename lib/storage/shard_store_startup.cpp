#include "shard_store_startup.hpp"

#include "index/native/manifest_format.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace timestar {
namespace {

constexpr size_t maxControlFileSize = 4096;

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ~ScopedFd() {
        if (fd_ >= 0)
            ::close(fd_);
    }

    [[nodiscard]] int get() const noexcept { return fd_; }

    int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

class ScopedDirectoryStream {
public:
    explicit ScopedDirectoryStream(DIR* stream) : stream_(stream) {}
    ScopedDirectoryStream(const ScopedDirectoryStream&) = delete;
    ScopedDirectoryStream& operator=(const ScopedDirectoryStream&) = delete;
    ~ScopedDirectoryStream() {
        if (stream_ != nullptr)
            ::closedir(stream_);
    }

    [[nodiscard]] DIR* get() const noexcept { return stream_; }

private:
    DIR* stream_ = nullptr;
};

struct ControlFile {
    bool present = false;
    bool valid = false;
    std::string contents;
    std::string error;
};

struct ParsedShardCount {
    bool present = false;
    bool valid = false;
    unsigned value = 0;
    std::string error;
};

struct DirectoryScan {
    bool valid = true;
    unsigned shardCount = 0;
    std::optional<std::string> legacyArtifact;
    std::optional<std::string> invalidReservedArtifact;
    std::optional<std::string> decommissionedWorkerArtifact;
    std::string error;
};

struct LegacyStateInspection {
    bool present = false;
    bool valid = false;
    unsigned oldShardCount = 0;
    unsigned newShardCount = 0;
    std::string detail;
};

std::string errnoMessage(int error) {
    return std::error_code(error, std::generic_category()).message();
}

bool isLockContentionError(int error) {
    if (error == EWOULDBLOCK)
        return true;
#if EAGAIN != EWOULDBLOCK
    if (error == EAGAIN)
        return true;
#endif
    return false;
}

bool parseUnsigned(std::string_view text, unsigned& value, bool allowZero = false) {
    if (text.empty())
        return false;

    unsigned long long parsed = 0;
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end || (!allowZero && parsed == 0) ||
        parsed > std::numeric_limits<unsigned>::max()) {
        return false;
    }

    value = static_cast<unsigned>(parsed);
    return true;
}

ControlFile readControlFile(int directoryFd, std::string_view name) {
    const std::string filename(name);
    ScopedFd file(::openat(directoryFd, filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (file.get() < 0) {
        if (errno == ENOENT)
            return {};
        return {.present = true,
                .valid = false,
                .contents = {},
                .error = "cannot open " + filename + ": " + errnoMessage(errno)};
    }

    struct stat info{};
    if (::fstat(file.get(), &info) != 0) {
        return {.present = true,
                .valid = false,
                .contents = {},
                .error = "cannot stat " + filename + ": " + errnoMessage(errno)};
    }
    if (!S_ISREG(info.st_mode)) {
        return {.present = true, .valid = false, .contents = {}, .error = filename + " is not a regular file"};
    }
    if (info.st_size < 0 || static_cast<uintmax_t>(info.st_size) > maxControlFileSize) {
        return {.present = true,
                .valid = false,
                .contents = {},
                .error = filename + " exceeds the 4096-byte control-file limit"};
    }

    std::string contents;
    contents.reserve(static_cast<size_t>(info.st_size));
    char buffer[256];
    while (true) {
        const ssize_t bytesRead = ::read(file.get(), buffer, sizeof(buffer));
        if (bytesRead == 0)
            break;
        if (bytesRead < 0) {
            if (errno == EINTR)
                continue;
            return {.present = true,
                    .valid = false,
                    .contents = {},
                    .error = "cannot read " + filename + ": " + errnoMessage(errno)};
        }
        contents.append(buffer, static_cast<size_t>(bytesRead));
        if (contents.size() > maxControlFileSize) {
            return {.present = true,
                    .valid = false,
                    .contents = {},
                    .error = filename + " exceeds the 4096-byte control-file limit"};
        }
    }

    const int fd = file.release();
    if (::close(fd) != 0) {
        return {.present = true,
                .valid = false,
                .contents = {},
                .error = "cannot close " + filename + ": " + errnoMessage(errno)};
    }
    return {.present = true, .valid = true, .contents = std::move(contents), .error = {}};
}

ParsedShardCount readShardCount(int directoryFd, const StorageLayout& layout) {
    const auto filename = layout.shardCountMetadataFile().filename().string();
    auto file = readControlFile(directoryFd, filename);
    if (!file.present)
        return {};
    if (!file.valid)
        return {.present = true, .valid = false, .error = std::move(file.error)};

    std::istringstream input(file.contents);
    std::string token;
    std::string extra;
    if (!(input >> token) || (input >> extra)) {
        return {.present = true, .valid = false, .error = "shard_count.meta must contain exactly one positive integer"};
    }

    unsigned count = 0;
    if (!parseUnsigned(token, count)) {
        return {.present = true, .valid = false, .error = "shard_count.meta contains an invalid shard count"};
    }

    return {.present = true, .valid = true, .value = count, .error = {}};
}

LegacyStateInspection inspectLegacyStateFile(int directoryFd, const StorageLayout& layout) {
    const auto filename = layout.rebalanceStateFile().filename().string();
    auto file = readControlFile(directoryFd, filename);
    if (!file.present)
        return {};
    if (!file.valid)
        return {.present = true, .valid = false, .detail = std::move(file.error)};

    std::istringstream input(file.contents);
    std::string phaseToken;
    std::string oldCountToken;
    std::string newCountToken;
    std::string extra;
    if (!(input >> phaseToken >> oldCountToken >> newCountToken) || (input >> extra)) {
        return {.present = true,
                .valid = false,
                .detail = "invalid rebalance.state: expected phase, old shard count, and new shard count"};
    }

    unsigned phase = 0;
    unsigned oldCount = 0;
    unsigned newCount = 0;
    const auto* begin = phaseToken.data();
    const auto* end = begin + phaseToken.size();
    const auto phaseResult = std::from_chars(begin, end, phase);
    if (phaseResult.ec != std::errc{} || phaseResult.ptr != end || phase > 3) {
        return {.present = true, .valid = false, .detail = "invalid rebalance.state: phase must be between 0 and 3"};
    }
    if (!parseUnsigned(oldCountToken, oldCount, phase == 0) || !parseUnsigned(newCountToken, newCount, phase == 0)) {
        return {.present = true,
                .valid = false,
                .detail = "invalid rebalance.state: active shard counts must be positive integers"};
    }

    return {.present = true,
            .valid = true,
            .oldShardCount = oldCount,
            .newShardCount = newCount,
            .detail = "legacy rebalance marker exists (phase " + std::to_string(phase) + ", " +
                      std::to_string(oldCount) + " -> " + std::to_string(newCount) + ")"};
}

DirectoryScan scanShardDirectories(int directoryFd, const StorageLayout& layout) {
    DirectoryScan scan;
    static const std::regex legacyPattern(R"(^shard_[0-9]+_(new|old)$)");
    const auto shardCountName = layout.shardCountMetadataFile().filename().string();
    const auto shardCountTemporaryName = layout.shardCountMetadataTemporaryFile().filename().string();
    const auto rebalanceName = layout.rebalanceStateFile().filename().string();
    const auto rebalanceTemporaryName = layout.rebalanceStateTemporaryFile().filename().string();
    std::vector<unsigned> shardIds;

    // openat(".") creates a new open-file description at directory offset 0.
    // dup() would share the locked descriptor's directory offset, making a
    // second inspection incorrectly appear empty after the first readdir().
    const int scanFd = ::openat(directoryFd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (scanFd < 0) {
        scan.valid = false;
        scan.error = "cannot open locked data root for scanning: " + errnoMessage(errno);
        return scan;
    }
    ScopedDirectoryStream directory(::fdopendir(scanFd));
    if (directory.get() == nullptr) {
        const int error = errno;
        ::close(scanFd);
        scan.valid = false;
        scan.error = "cannot scan locked data root: " + errnoMessage(error);
        return scan;
    }

    errno = 0;
    while (const dirent* entry = ::readdir(directory.get())) {
        const std::string name(entry->d_name);
        if (name == "." || name == "..")
            continue;

        if (name == shardCountName || name == rebalanceName) {
            // Validity is checked through O_NOFOLLOW file descriptors.
        } else if (name == rebalanceTemporaryName || name == shardCountTemporaryName ||
                   std::regex_match(name, legacyPattern)) {
            scan.legacyArtifact = name;
        } else if (const auto shardId = layout.parseShardDirName(name)) {
            struct stat info{};
            if (::fstatat(directoryFd, name.c_str(), &info, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISDIR(info.st_mode)) {
                scan.valid = false;
                scan.error = name + " is not a real directory";
                return scan;
            }
            shardIds.push_back(*shardId);
        } else if (layout.isShardNamespaceEntry(name)) {
            scan.valid = false;
            scan.error = "invalid shard directory name: " + name;
            return scan;
        } else if (layout.isDecommissionedWorkerArtifactName(name)) {
            scan.decommissionedWorkerArtifact = name;
        } else if (name.starts_with(shardCountName) || name.starts_with(rebalanceName)) {
            scan.invalidReservedArtifact = name;
        }
        errno = 0;
    }
    if (errno != 0) {
        scan.valid = false;
        scan.error = "cannot scan locked data root: " + errnoMessage(errno);
        return scan;
    }

    std::sort(shardIds.begin(), shardIds.end());
    for (size_t i = 0; i < shardIds.size(); ++i) {
        if (shardIds[i] != i) {
            scan.valid = false;
            scan.error = "active shard directories are not contiguous from shard_0";
            return scan;
        }
    }
    scan.shardCount = static_cast<unsigned>(shardIds.size());
    return scan;
}

bool verifyRootBinding(int directoryFd, const fs::path& dataDir, std::string& error) {
    struct stat lockedInfo{};
    struct stat pathInfo{};
    if (::fstat(directoryFd, &lockedInfo) != 0) {
        error = "cannot stat locked data root: " + errnoMessage(errno);
        return false;
    }
    if (::fstatat(AT_FDCWD, dataDir.c_str(), &pathInfo, AT_SYMLINK_NOFOLLOW) != 0) {
        error = "configured data-root path no longer names the locked directory: " + errnoMessage(errno);
        return false;
    }
    if (!S_ISDIR(pathInfo.st_mode) || lockedInfo.st_dev != pathInfo.st_dev || lockedInfo.st_ino != pathInfo.st_ino) {
        error = "configured data-root path was replaced after exclusive ownership was acquired";
        return false;
    }
    return true;
}

bool validateCommittedShardStructure(int directoryFd, const StorageLayout& layout, unsigned shardCount,
                                     std::string& error) {
    for (unsigned shard = 0; shard < shardCount; ++shard) {
        const auto shardName = layout.shardDir(shard).filename().string();
        const auto tsmName = layout.tsmDir(shard).filename().string();
        const auto indexName = layout.nativeIndexDir(shard).filename().string();
        const auto manifestName = layout.nativeManifestFile(shard).filename().string();
        ScopedFd shardFd(::openat(directoryFd, shardName.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
        if (shardFd.get() < 0) {
            error = shardName + " cannot be opened as a real directory: " + errnoMessage(errno);
            return false;
        }

        ScopedFd tsmFd(::openat(shardFd.get(), tsmName.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
        if (tsmFd.get() < 0) {
            error = shardName + "/" + tsmName + " is missing or not a real directory: " + errnoMessage(errno);
            return false;
        }

        ScopedFd indexFd(::openat(shardFd.get(), indexName.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
        if (indexFd.get() < 0) {
            error = shardName + "/" + indexName + " is missing or not a real directory: " + errnoMessage(errno);
            return false;
        }

        ScopedFd manifestFd(::openat(indexFd.get(), manifestName.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
        if (manifestFd.get() < 0) {
            error = shardName + "/" + indexName + "/" + manifestName +
                    " is missing or is a symlink: " + errnoMessage(errno);
            return false;
        }
        struct stat manifestInfo{};
        if (::fstat(manifestFd.get(), &manifestInfo) != 0 || !S_ISREG(manifestInfo.st_mode)) {
            error = shardName + "/" + indexName + "/" + manifestName + " is not a regular file";
            return false;
        }
        if (manifestInfo.st_size <= 0 ||
            static_cast<uintmax_t>(manifestInfo.st_size) > std::numeric_limits<size_t>::max()) {
            error = shardName + "/" + indexName + "/" + manifestName + " is empty or cannot fit in memory";
            return false;
        }

        std::string manifestContents;
        manifestContents.reserve(static_cast<size_t>(manifestInfo.st_size));
        char buffer[8192];
        while (true) {
            const ssize_t bytesRead = ::read(manifestFd.get(), buffer, sizeof(buffer));
            if (bytesRead == 0)
                break;
            if (bytesRead < 0) {
                if (errno == EINTR)
                    continue;
                error = shardName + "/" + indexName + "/" + manifestName + " cannot be read: " + errnoMessage(errno);
                return false;
            }
            manifestContents.append(buffer, static_cast<size_t>(bytesRead));
        }

        struct stat finalManifestInfo{};
        if (::fstat(manifestFd.get(), &finalManifestInfo) != 0 || finalManifestInfo.st_dev != manifestInfo.st_dev ||
            finalManifestInfo.st_ino != manifestInfo.st_ino ||
            finalManifestInfo.st_size != static_cast<off_t>(manifestContents.size())) {
            error = shardName + "/" + indexName + "/" + manifestName + " changed while startup inspected it";
            return false;
        }

        const auto decoded = index::decodeManifest(manifestContents);
        if (!decoded.complete()) {
            error = shardName + "/" + indexName + "/" + manifestName + " is not fully recoverable at offset " +
                    std::to_string(decoded.issueOffset) + ": " + decoded.issue;
            return false;
        }

        for (const auto& file : decoded.files) {
            const auto filename = layout.nativeSstableFile(shard, file.fileNumber).filename().string();
            ScopedFd sstableFd(::openat(indexFd.get(), filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
            if (sstableFd.get() < 0) {
                error = shardName + "/" + indexName + "/" + filename +
                        " is referenced by MANIFEST but is missing or a symlink: " + errnoMessage(errno);
                return false;
            }
            struct stat sstableInfo{};
            if (::fstat(sstableFd.get(), &sstableInfo) != 0 || !S_ISREG(sstableInfo.st_mode)) {
                error = shardName + "/" + indexName + "/" + filename +
                        " is referenced by MANIFEST but is not a regular file";
                return false;
            }
            if (sstableInfo.st_size < 0 || static_cast<uint64_t>(sstableInfo.st_size) != file.fileSize) {
                error =
                    shardName + "/" + indexName + "/" + filename + " size does not match the committed MANIFEST record";
                return false;
            }
        }
    }
    return true;
}

ShardStoreInspection invalidInspection(unsigned requestedShardCount, std::string detail) {
    return {.status = ShardStoreStartupStatus::InvalidMetadata,
            .requestedShardCount = requestedShardCount,
            .detail = std::move(detail)};
}

int installMarkerNoReplace(int directoryFd, const char* temporaryName, const char* finalName) {
#if defined(SYS_renameat2) && defined(RENAME_NOREPLACE)
    if (::syscall(SYS_renameat2, directoryFd, temporaryName, directoryFd, finalName, RENAME_NOREPLACE) == 0)
        return 0;
    if (errno != ENOSYS && errno != EINVAL)
        return -1;
#endif

    // linkat is the portable same-filesystem no-replace primitive: it fails
    // with EEXIST instead of replacing a marker created by another process.
    if (::linkat(directoryFd, temporaryName, directoryFd, finalName, 0) != 0)
        return -1;
    if (::unlinkat(directoryFd, temporaryName, 0) == 0)
        return 0;

    const int unlinkError = errno;
    ::unlinkat(directoryFd, finalName, 0);
    errno = unlinkError;
    return -1;
}

}  // namespace

ShardStoreLock::ShardStoreLock(int directoryFd, fs::path dataDir)
    : directoryFd_(directoryFd), dataDir_(std::move(dataDir)) {}

ShardStoreLock::ShardStoreLock(ShardStoreLock&& other) noexcept
    : directoryFd_(other.directoryFd_), dataDir_(std::move(other.dataDir_)) {
    other.directoryFd_ = -1;
}

ShardStoreLock& ShardStoreLock::operator=(ShardStoreLock&& other) noexcept {
    if (this == &other)
        return *this;
    if (directoryFd_ >= 0)
        ::close(directoryFd_);
    directoryFd_ = other.directoryFd_;
    dataDir_ = std::move(other.dataDir_);
    other.directoryFd_ = -1;
    return *this;
}

ShardStoreLock::~ShardStoreLock() {
    if (directoryFd_ >= 0)
        ::close(directoryFd_);
}

ShardStoreStartup::ShardStoreStartup(StorageLayout layout) : layout_(layout.anchored()) {}

ShardStoreLock ShardStoreStartup::acquireExclusiveLock() const {
    const auto& dataDir = layout_.root();
    fs::create_directories(dataDir);
    ScopedFd directory(::open(dataDir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (directory.get() < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "cannot open legacy data root for exclusive ownership: " + dataDir.string());
    }
    if (::flock(directory.get(), LOCK_EX | LOCK_NB) != 0) {
        const int error = errno;
        const std::string context = isLockContentionError(error)
                                        ? "legacy data root is already owned by another TimeStar process: "
                                        : "cannot lock legacy data root for exclusive ownership: ";
        throw std::system_error(error, std::generic_category(), context + dataDir.string());
    }
    return ShardStoreLock(directory.release(), dataDir);
}

ShardStoreInspection ShardStoreStartup::inspect(unsigned requestedShardCount, const ShardStoreLock& lock) const {
    if (lock.directoryFd_ < 0 || lock.dataDir_ != layout_.root())
        throw std::invalid_argument("startup inspection requires the exclusive lock for this data root");
    if (requestedShardCount == 0)
        return invalidInspection(requestedShardCount, "requested shard count must be greater than zero");

    std::string rootError;
    if (!verifyRootBinding(lock.directoryFd_, layout_.root(), rootError))
        return invalidInspection(requestedShardCount, std::move(rootError));

    const auto legacyState = inspectLegacyStateFile(lock.directoryFd_, layout_);
    if (legacyState.present) {
        return {.status = legacyState.valid ? ShardStoreStartupStatus::InterruptedLegacyRebalance
                                            : ShardStoreStartupStatus::InvalidMetadata,
                .previousShardCount = legacyState.oldShardCount,
                .requestedShardCount = requestedShardCount,
                .detail = legacyState.detail};
    }

    const auto directories = scanShardDirectories(lock.directoryFd_, layout_);
    if (!directories.valid)
        return invalidInspection(requestedShardCount, directories.error);
    if (directories.invalidReservedArtifact) {
        return invalidInspection(requestedShardCount,
                                 "unknown reserved storage artifact: " + *directories.invalidReservedArtifact);
    }
    if (directories.decommissionedWorkerArtifact) {
        return invalidInspection(
            requestedShardCount,
            "decommissioned VShard-worker artifact exists: " + *directories.decommissionedWorkerArtifact +
                ". The persisted worker/ownership machinery was removed (Task D0, "
                "docs/clustering-vshard-workers.md); this root was touched by a pre-release build. Move the "
                "artifact out of the data root after confirming it holds no needed state, then restart");
    }
    if (directories.legacyArtifact) {
        return {.status = ShardStoreStartupStatus::InterruptedLegacyRebalance,
                .requestedShardCount = requestedShardCount,
                .detail = "legacy rebalance artifact exists: " + *directories.legacyArtifact};
    }

    const auto metadata = readShardCount(lock.directoryFd_, layout_);
    if (metadata.present && !metadata.valid)
        return invalidInspection(requestedShardCount, metadata.error);

    if (!metadata.present) {
        if (directories.shardCount == 0) {
            return {.status = ShardStoreStartupStatus::FreshStore,
                    .requestedShardCount = requestedShardCount,
                    .detail = "no committed legacy shard storage exists"};
        }
        return {.status = ShardStoreStartupStatus::UncommittedInitialization,
                .previousShardCount = directories.shardCount,
                .requestedShardCount = requestedShardCount,
                .detail = "active shard directories exist without the shard_count.meta initialization marker"};
    }

    const unsigned previousShardCount = metadata.value;
    if (directories.shardCount != previousShardCount) {
        return {.status = ShardStoreStartupStatus::IncompleteStore,
                .previousShardCount = previousShardCount,
                .requestedShardCount = requestedShardCount,
                .detail = "shard_count.meta requires " + std::to_string(previousShardCount) +
                          " active shard directories, but found " + std::to_string(directories.shardCount)};
    }

    std::string structureError;
    if (!validateCommittedShardStructure(lock.directoryFd_, layout_, previousShardCount, structureError)) {
        return {.status = ShardStoreStartupStatus::IncompleteStore,
                .previousShardCount = previousShardCount,
                .requestedShardCount = requestedShardCount,
                .detail = std::move(structureError)};
    }

    if (previousShardCount == requestedShardCount) {
        return {.status = ShardStoreStartupStatus::MatchingShardCount,
                .previousShardCount = previousShardCount,
                .requestedShardCount = requestedShardCount,
                .detail = "persisted shard count and shard structure match the requested count"};
    }

    return {.status = ShardStoreStartupStatus::UnsafeShardCountChange,
            .previousShardCount = previousShardCount,
            .requestedShardCount = requestedShardCount,
            .detail =
                "automatic legacy core-count migration is disabled because it can make series metadata "
                "undiscoverable"};
}

std::string ShardStoreStartup::failureMessage(const ShardStoreInspection& inspection) const {
    std::error_code ec;
    auto activeRoot = fs::absolute(layout_.root(), ec);
    if (ec)
        activeRoot = layout_.root();

    std::ostringstream message;
    message << "Unsafe TimeStar startup for active legacy storage root '" << activeRoot.lexically_normal().string()
            << "': " << inspection.detail << ".";

    switch (inspection.status) {
        case ShardStoreStartupStatus::UnsafeShardCountChange:
            message << " Requested --smp " << inspection.requestedShardCount << ", but this store requires --smp "
                    << inspection.previousShardCount
                    << ". No files were migrated; restart with the required value. Automatic VShard worker "
                       "rebalancing is available only after migration to the future VShard format.";
            break;
        case ShardStoreStartupStatus::InterruptedLegacyRebalance:
            message << " Normal startup will not resume or delete legacy rebalance state. Preserve the entire data "
                       "root and restore a complete pre-rebalance backup; no automatic recovery tool is currently "
                       "available.";
            break;
        case ShardStoreStartupStatus::UncommittedInitialization:
            message << " A previous initialization did not commit. Preserve the data root for offline inspection; "
                       "do not delete shard directories unless they have been verified to contain no data.";
            break;
        case ShardStoreStartupStatus::IncompleteStore:
            message << " The committed legacy layout is incomplete. Restore the missing shard/index state and "
                       "shard_count.meta together from a consistent backup.";
            break;
        case ShardStoreStartupStatus::InvalidMetadata:
            message << " Restore the named control file or reserved artifact state from a consistent backup. No "
                       "automatic metadata repair or migration tool is currently available.";
            break;
        case ShardStoreStartupStatus::FreshStore:
        case ShardStoreStartupStatus::MatchingShardCount:
            message << " This status is normally safe; startup rejected an inconsistent sequencing decision.";
            break;
    }
    return message.str();
}

void ShardStoreStartup::commitAfterInitialization(const ShardStoreInspection& inspection,
                                                  const ShardStoreLock& lock) const {
    if (lock.directoryFd_ < 0 || lock.dataDir_ != layout_.root())
        throw std::invalid_argument("startup commit requires the exclusive lock for this data root");
    if (inspection.status != ShardStoreStartupStatus::FreshStore &&
        inspection.status != ShardStoreStartupStatus::MatchingShardCount) {
        throw std::logic_error("only a safe fresh store can commit shard-count metadata");
    }
    if (inspection.requestedShardCount == 0)
        throw std::logic_error("a zero-shard store cannot commit shard-count metadata");

    if (inspection.status == ShardStoreStartupStatus::MatchingShardCount) {
        const auto revalidated = inspect(inspection.requestedShardCount, lock);
        if (revalidated.status != ShardStoreStartupStatus::MatchingShardCount ||
            revalidated.previousShardCount != inspection.previousShardCount) {
            throw std::runtime_error("matching legacy store changed before final startup commit: " +
                                     revalidated.detail);
        }
        return;
    }

    std::string rootError;
    if (!verifyRootBinding(lock.directoryFd_, layout_.root(), rootError))
        throw std::runtime_error(std::move(rootError));

    const auto legacyState = inspectLegacyStateFile(lock.directoryFd_, layout_);
    if (legacyState.present) {
        throw std::runtime_error("rebalance.state appeared after fresh-store inspection: " + legacyState.detail);
    }

    const auto metadata = readShardCount(lock.directoryFd_, layout_);
    if (metadata.present)
        throw std::runtime_error("shard_count.meta appeared after fresh-store inspection");

    const auto directories = scanShardDirectories(lock.directoryFd_, layout_);
    if (!directories.valid || directories.legacyArtifact || directories.invalidReservedArtifact ||
        directories.decommissionedWorkerArtifact || directories.shardCount != inspection.requestedShardCount) {
        throw std::runtime_error("Engine initialization did not produce the expected canonical shard directories");
    }

    std::string structureError;
    if (!validateCommittedShardStructure(lock.directoryFd_, layout_, inspection.requestedShardCount, structureError))
        throw std::runtime_error("Engine initialization produced an incomplete shard layout: " + structureError);

    const auto tmp = layout_.shardCountMetadataTemporaryFile().filename().string();
    const auto final = layout_.shardCountMetadataFile().filename().string();
    bool renamed = false;

    try {
        ScopedFd output(
            ::openat(lock.directoryFd_, tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0644));
        if (output.get() < 0)
            throw std::system_error(errno, std::generic_category(), "cannot create " + tmp);

        const std::string contents = std::to_string(inspection.requestedShardCount) + "\n";
        size_t written = 0;
        while (written < contents.size()) {
            const ssize_t result = ::write(output.get(), contents.data() + written, contents.size() - written);
            if (result < 0) {
                if (errno == EINTR)
                    continue;
                throw std::system_error(errno, std::generic_category(), "cannot write " + tmp);
            }
            if (result == 0)
                throw std::runtime_error("zero-byte write while creating " + tmp);
            written += static_cast<size_t>(result);
        }
        if (::fsync(output.get()) != 0)
            throw std::system_error(errno, std::generic_category(), "cannot fsync " + tmp);

        const int outputFd = output.release();
        if (::close(outputFd) != 0)
            throw std::system_error(errno, std::generic_category(), "cannot close " + tmp);

        if (installMarkerNoReplace(lock.directoryFd_, tmp.c_str(), final.c_str()) != 0) {
            const int error = errno;
            const std::string context = error == EEXIST ? final + " appeared during initialization"
                                                        : "cannot install " + final + " without replacement";
            throw std::system_error(error, std::generic_category(), context);
        }
        renamed = true;

        if (::fsync(lock.directoryFd_) != 0)
            throw std::system_error(errno, std::generic_category(),
                                    "shard_count.meta was installed but data-root durability is ambiguous because "
                                    "the directory fsync failed");
    } catch (...) {
        if (!renamed)
            ::unlinkat(lock.directoryFd_, tmp.c_str(), 0);
        throw;
    }
}

ShardStoreStartupSession::ShardStoreStartupSession(StorageLayout layout, unsigned requestedShardCount)
    : startup_(std::move(layout)), lock_(startup_.acquireExclusiveLock()) {
    inspection_ = startup_.inspect(requestedShardCount, lock_);
}

std::string ShardStoreStartupSession::failureMessage() const {
    return startup_.failureMessage(inspection_);
}

void ShardStoreStartupSession::authorizeFirstStorageMutation() {
    if (state_ != State::Inspected)
        throw std::logic_error("the first storage mutation can be authorized exactly once");
    if (!inspection_.canStart())
        throw std::runtime_error(failureMessage());

    const auto revalidated = startup_.inspect(inspection_.requestedShardCount, lock_);
    if (revalidated.status != inspection_.status || revalidated.previousShardCount != inspection_.previousShardCount ||
        revalidated.requestedShardCount != inspection_.requestedShardCount) {
        throw std::runtime_error("legacy store changed after initial startup inspection: " + revalidated.detail);
    }
    state_ = State::MutationAuthorized;
}

void ShardStoreStartupSession::commitEngineInitialization() {
    if (state_ != State::MutationAuthorized)
        throw std::logic_error("Engine initialization cannot commit before the first storage mutation is authorized");
    startup_.commitAfterInitialization(inspection_, lock_);
    state_ = State::InitializationCommitted;
}

}  // namespace timestar

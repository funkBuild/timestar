#include "worker_registry_store.hpp"

#include "../storage/shard_store_startup.hpp"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace fs = std::filesystem;

namespace timestar::cluster {
namespace {

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ~ScopedFd() {
        if (fd_ >= 0)
            ::close(fd_);
    }

    [[nodiscard]] int get() const noexcept { return fd_; }

    int release() noexcept {
        const int result = fd_;
        fd_ = -1;
        return result;
    }

private:
    int fd_;
};

enum class RegistryFileStatus {
    Missing,
    Valid,
    Invalid,
};

struct RegistryFileRead {
    RegistryFileStatus status = RegistryFileStatus::Missing;
    std::optional<WorkerRegistry> registry;
    std::string error;
    std::string contents;
    dev_t device = 0;
    ino_t inode = 0;
};

struct FileIdentity {
    dev_t device = 0;
    ino_t inode = 0;
};

std::string errnoMessage(int error) {
    return std::error_code(error, std::generic_category()).message();
}

RegistryFileRead invalidFile(std::string filename, std::string reason) {
    return {.status = RegistryFileStatus::Invalid,
            .registry = std::nullopt,
            .error = std::move(filename) + ": " + std::move(reason),
            .contents = {},
            .device = 0,
            .inode = 0};
}

RegistryFileRead readRegistryFile(int directoryFd, const std::string& filename) {
    // O_NONBLOCK prevents a hostile FIFO at a reserved name from hanging
    // startup before fstat can reject the non-regular file.
    ScopedFd file(::openat(directoryFd, filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (file.get() < 0) {
        if (errno == ENOENT)
            return {};
        return invalidFile(filename, "cannot open: " + errnoMessage(errno));
    }

    struct stat initialInfo{};
    if (::fstat(file.get(), &initialInfo) != 0)
        return invalidFile(filename, "cannot stat: " + errnoMessage(errno));
    if (!S_ISREG(initialInfo.st_mode))
        return invalidFile(filename, "is not a regular file");
    if (initialInfo.st_size <= 0 || static_cast<uintmax_t>(initialInfo.st_size) > MAX_WORKER_REGISTRY_JSON_BYTES) {
        return invalidFile(filename, "size is outside the worker-registry limit");
    }

    std::string contents;
    contents.reserve(static_cast<size_t>(initialInfo.st_size));
    char buffer[4096];
    while (true) {
        const ssize_t bytesRead = ::read(file.get(), buffer, sizeof(buffer));
        if (bytesRead == 0)
            break;
        if (bytesRead < 0) {
            if (errno == EINTR)
                continue;
            return invalidFile(filename, "cannot read: " + errnoMessage(errno));
        }
        contents.append(buffer, static_cast<size_t>(bytesRead));
        if (contents.size() > MAX_WORKER_REGISTRY_JSON_BYTES)
            return invalidFile(filename, "grew beyond the worker-registry limit while being read");
    }

    struct stat finalInfo{};
    if (::fstat(file.get(), &finalInfo) != 0)
        return invalidFile(filename, "cannot restat: " + errnoMessage(errno));
    if (finalInfo.st_dev != initialInfo.st_dev || finalInfo.st_ino != initialInfo.st_ino ||
        finalInfo.st_size != static_cast<off_t>(contents.size()) ||
        finalInfo.st_mtim.tv_sec != initialInfo.st_mtim.tv_sec ||
        finalInfo.st_mtim.tv_nsec != initialInfo.st_mtim.tv_nsec) {
        return invalidFile(filename, "changed while being read");
    }

    const int fd = file.release();
    if (::close(fd) != 0)
        return invalidFile(filename, "cannot close: " + errnoMessage(errno));

    try {
        auto registry = decodeWorkerRegistryJson(contents);
        return {.status = RegistryFileStatus::Valid,
                .registry = std::move(registry),
                .error = {},
                .contents = std::move(contents),
                .device = initialInfo.st_dev,
                .inode = initialInfo.st_ino};
    } catch (const std::exception& error) {
        return invalidFile(filename, error.what());
    }
}

bool isExactInitialRegistry(const WorkerRegistry& registry) {
    if (registry.workers.empty() || registry.workers.size() > MAX_LOCAL_STORAGE_WORKERS)
        return false;
    return registry == createWorkerRegistry(static_cast<unsigned>(registry.workers.size()));
}

void syncDirectory(int directoryFd, std::string_view context) {
    if (::fsync(directoryFd) != 0)
        throw std::system_error(errno, std::generic_category(), std::string(context));
}

void verifyRootBinding(int directoryFd, const fs::path& root) {
    struct stat lockedInfo{};
    struct stat pathInfo{};
    if (::fstat(directoryFd, &lockedInfo) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot stat locked worker-registry data root");
    if (::fstatat(AT_FDCWD, root.c_str(), &pathInfo, AT_SYMLINK_NOFOLLOW) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "configured worker-registry data root no longer names the locked directory");
    }
    if (!S_ISDIR(pathInfo.st_mode) || lockedInfo.st_dev != pathInfo.st_dev || lockedInfo.st_ino != pathInfo.st_ino)
        throw std::runtime_error("configured worker-registry data root was replaced after locking");
}

void observeNoexcept(const WorkerRegistryCommitObserver& observer, WorkerRegistryCommitStage stage) noexcept {
    if (!observer)
        return;
    try {
        observer(stage);
    } catch (...) {
        std::terminate();
    }
}

FileIdentity fileIdentity(int fd, std::string_view context) {
    struct stat info{};
    if (::fstat(fd, &info) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot stat " + std::string(context));
    if (!S_ISREG(info.st_mode))
        throw std::runtime_error(std::string(context) + " is not a regular file");
    return {.device = info.st_dev, .inode = info.st_ino};
}

void verifyNameIdentity(int directoryFd, const std::string& filename, FileIdentity expected) {
    struct stat info{};
    if (::fstatat(directoryFd, filename.c_str(), &info, AT_SYMLINK_NOFOLLOW) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot stat reserved name " + filename);
    if (!S_ISREG(info.st_mode) || info.st_dev != expected.device || info.st_ino != expected.inode)
        throw std::runtime_error(filename + " no longer names the validated registry inode");
}

void verifyExactRegistryFile(int directoryFd, const std::string& filename, FileIdentity expectedIdentity,
                             std::string_view expectedContents) {
    const auto reread = readRegistryFile(directoryFd, filename);
    if (reread.status != RegistryFileStatus::Valid)
        throw std::runtime_error(filename + " is no longer a valid registry: " + reread.error);
    if (reread.device != expectedIdentity.device || reread.inode != expectedIdentity.inode)
        throw std::runtime_error(filename + " no longer names the validated registry inode");
    if (reread.contents != expectedContents)
        throw std::runtime_error(filename + " contents changed after validation");
}

void syncNamedRegistryFile(int directoryFd, const std::string& filename, const RegistryFileRead& expected) {
    ScopedFd file(::openat(directoryFd, filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (file.get() < 0)
        throw std::system_error(errno, std::generic_category(), "cannot reopen " + filename + " for fsync");

    const auto identity = fileIdentity(file.get(), filename + " before fsync");
    if (identity.device != expected.device || identity.inode != expected.inode)
        throw std::runtime_error(filename + " was replaced after validation");
    if (::fsync(file.get()) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot fsync recovered " + filename);

    verifyNameIdentity(directoryFd, filename, identity);
}

void restabilizeAfterObserver(int directoryFd, const fs::path& root, const std::string& filename,
                              FileIdentity expectedIdentity, std::string_view expectedContents) {
    verifyRootBinding(directoryFd, root);
    verifyExactRegistryFile(directoryFd, filename, expectedIdentity, expectedContents);

    ScopedFd file(::openat(directoryFd, filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (file.get() < 0)
        throw std::system_error(errno, std::generic_category(), "cannot reopen " + filename + " for final fsync");
    const auto identity = fileIdentity(file.get(), filename + " before final fsync");
    if (identity.device != expectedIdentity.device || identity.inode != expectedIdentity.inode)
        throw std::runtime_error(filename + " was replaced before final fsync");
    if (::fsync(file.get()) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot perform final fsync of " + filename);
    verifyNameIdentity(directoryFd, filename, expectedIdentity);

    syncDirectory(directoryFd, filename + " final namespace durability is ambiguous");
    verifyRootBinding(directoryFd, root);
    verifyExactRegistryFile(directoryFd, filename, expectedIdentity, expectedContents);
}

void removeTemporary(int directoryFd, const fs::path& root, const std::string& temporaryName,
                     const WorkerRegistryCommitObserver* observer) {
    verifyRootBinding(directoryFd, root);
    if (::unlinkat(directoryFd, temporaryName.c_str(), 0) != 0) {
        if (errno == ENOENT)
            return;
        throw std::system_error(errno, std::generic_category(), "cannot remove " + temporaryName);
    }
    verifyRootBinding(directoryFd, root);
    if (observer != nullptr)
        observeNoexcept(*observer, WorkerRegistryCommitStage::RecoveryTemporaryRemoved);
    syncDirectory(directoryFd, temporaryName + " was removed but data-root durability is ambiguous");
    verifyRootBinding(directoryFd, root);
    if (observer != nullptr)
        observeNoexcept(*observer, WorkerRegistryCommitStage::RecoveryDirectorySynced);
}

void removeTemporaryIfIdentity(int directoryFd, const fs::path& root, const std::string& temporaryName,
                               FileIdentity createdIdentity) {
    verifyRootBinding(directoryFd, root);
    try {
        verifyNameIdentity(directoryFd, temporaryName, createdIdentity);
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::no_such_file_or_directory)
            return;
        throw;
    }
    if (::unlinkat(directoryFd, temporaryName.c_str(), 0) != 0)
        throw std::system_error(errno, std::generic_category(), "cannot remove created " + temporaryName);
    verifyRootBinding(directoryFd, root);
    syncDirectory(directoryFd, temporaryName + " cleanup durability is ambiguous");
    verifyRootBinding(directoryFd, root);
}

int installNoReplace(int directoryFd, const char* temporaryName, const char* finalName) {
#if defined(SYS_renameat2) && defined(RENAME_NOREPLACE)
    if (::syscall(SYS_renameat2, directoryFd, temporaryName, directoryFd, finalName, RENAME_NOREPLACE) == 0)
        return 0;
    if (errno != ENOSYS && errno != EINVAL)
        return -1;
#endif

    if (::linkat(directoryFd, temporaryName, directoryFd, finalName, 0) != 0)
        return -1;
    if (::unlinkat(directoryFd, temporaryName, 0) == 0)
        return 0;

    const int unlinkError = errno;
    ::unlinkat(directoryFd, finalName, 0);
    errno = unlinkError;
    return -1;
}

void promoteTemporary(int directoryFd, const fs::path& root, const std::string& temporaryName,
                      const std::string& finalName, const RegistryFileRead* acceptedFinal,
                      const RegistryFileRead& temporaryFile, const WorkerRegistryCommitObserver& observer) {
    syncNamedRegistryFile(directoryFd, temporaryName, temporaryFile);
    observeNoexcept(observer, WorkerRegistryCommitStage::RecoveryTemporarySynced);
    verifyRootBinding(directoryFd, root);
    const FileIdentity temporaryIdentity{temporaryFile.device, temporaryFile.inode};
    verifyExactRegistryFile(directoryFd, temporaryName, temporaryIdentity, temporaryFile.contents);
    if (acceptedFinal != nullptr) {
        verifyExactRegistryFile(directoryFd, finalName, FileIdentity{acceptedFinal->device, acceptedFinal->inode},
                                acceptedFinal->contents);
    }
    const int result = acceptedFinal != nullptr
                           ? ::renameat(directoryFd, temporaryName.c_str(), directoryFd, finalName.c_str())
                           : installNoReplace(directoryFd, temporaryName.c_str(), finalName.c_str());
    if (result != 0) {
        const int error = errno;
        const std::string reason = error == EEXIST ? finalName + " appeared during recovery"
                                                   : "cannot promote " + temporaryName + " to " + finalName;
        throw std::system_error(error, std::generic_category(), reason);
    }
    verifyRootBinding(directoryFd, root);
    verifyNameIdentity(directoryFd, finalName, temporaryIdentity);
    observeNoexcept(observer, WorkerRegistryCommitStage::RecoveryRenamed);
    syncDirectory(directoryFd, finalName + " was recovered but data-root durability is ambiguous");
    verifyRootBinding(directoryFd, root);
    observeNoexcept(observer, WorkerRegistryCommitStage::RecoveryDirectorySynced);
    restabilizeAfterObserver(directoryFd, root, finalName, temporaryIdentity, temporaryFile.contents);
}

void stabilizeAcceptedFinal(int directoryFd, const fs::path& root, const std::string& finalName,
                            const RegistryFileRead& finalFile, const WorkerRegistryCommitObserver& observer) {
    syncNamedRegistryFile(directoryFd, finalName, finalFile);
    observeNoexcept(observer, WorkerRegistryCommitStage::RecoveryFinalSynced);
    verifyRootBinding(directoryFd, root);
    syncDirectory(directoryFd, finalName + " is valid but data-root namespace durability is ambiguous");
    verifyRootBinding(directoryFd, root);
    observeNoexcept(observer, WorkerRegistryCommitStage::RecoveryDirectorySynced);
    restabilizeAfterObserver(directoryFd, root, finalName, FileIdentity{finalFile.device, finalFile.inode},
                             finalFile.contents);
}

}  // namespace

WorkerRegistryStore::WorkerRegistryStore(StorageLayout layout, WorkerRegistryCommitObserver observer)
    : layout_(layout.anchored()), observer_(std::move(observer)) {}

int WorkerRegistryStore::lockedDirectoryFd(const ShardStoreLock& lock) const {
    if (lock.directoryFd_ < 0 || lock.dataDir_ != layout_.root())
        throw std::invalid_argument("worker registry operation requires the exclusive lock for this data root");
    verifyRootBinding(lock.directoryFd_, layout_.root());
    return lock.directoryFd_;
}

void WorkerRegistryStore::observe(WorkerRegistryCommitStage stage) const noexcept {
    observeNoexcept(observer_, stage);
}

std::optional<WorkerRegistry> WorkerRegistryStore::loadAndRecover(const ShardStoreLock& lock) const {
    const int directoryFd = lockedDirectoryFd(lock);
    const auto finalName = layout_.workerRegistryFile().filename().string();
    const auto temporaryName = layout_.workerRegistryTemporaryFile().filename().string();
    const auto finalFile = readRegistryFile(directoryFd, finalName);
    const auto temporaryFile = readRegistryFile(directoryFd, temporaryName);

    if (finalFile.status == RegistryFileStatus::Invalid)
        throw std::runtime_error("invalid authoritative worker registry " + finalFile.error);

    if (finalFile.status == RegistryFileStatus::Missing) {
        if (temporaryFile.status == RegistryFileStatus::Missing)
            return std::nullopt;
        if (temporaryFile.status == RegistryFileStatus::Invalid) {
            throw std::runtime_error("workers.json is missing and temporary evidence is invalid: " +
                                     temporaryFile.error);
        }
        if (!isExactInitialRegistry(*temporaryFile.registry)) {
            throw std::runtime_error("workers.json is missing but its valid temporary file is not an initial registry");
        }
        promoteTemporary(directoryFd, layout_.root(), temporaryName, finalName, nullptr, temporaryFile, observer_);
        return temporaryFile.registry;
    }

    if (temporaryFile.status == RegistryFileStatus::Missing) {
        stabilizeAcceptedFinal(directoryFd, layout_.root(), finalName, finalFile, observer_);
        return finalFile.registry;
    }
    if (temporaryFile.status == RegistryFileStatus::Invalid) {
        removeTemporary(directoryFd, layout_.root(), temporaryName, &observer_);
        stabilizeAcceptedFinal(directoryFd, layout_.root(), finalName, finalFile, observer_);
        return finalFile.registry;
    }

    const auto& accepted = *finalFile.registry;
    const auto& candidate = *temporaryFile.registry;
    if (candidate == accepted || candidate.generation < accepted.generation) {
        removeTemporary(directoryFd, layout_.root(), temporaryName, &observer_);
        stabilizeAcceptedFinal(directoryFd, layout_.root(), finalName, finalFile, observer_);
        return accepted;
    }
    if (candidate.generation == accepted.generation) {
        throw std::runtime_error("workers.json.tmp diverges from workers.json in the same generation");
    }

    try {
        validateWorkerRegistryTransition(accepted, candidate);
    } catch (const std::exception& error) {
        throw std::runtime_error("workers.json.tmp is a newer invalid transition: " + std::string(error.what()));
    }
    promoteTemporary(directoryFd, layout_.root(), temporaryName, finalName, &finalFile, temporaryFile, observer_);
    return candidate;
}

void WorkerRegistryStore::install(const WorkerRegistry& candidate, const ShardStoreLock& lock) const {
    validateWorkerRegistry(candidate);
    const int directoryFd = lockedDirectoryFd(lock);
    const auto accepted = loadAndRecover(lock);
    const bool replacing = accepted.has_value();
    std::optional<RegistryFileRead> acceptedFile;
    if (accepted) {
        if (*accepted == candidate)
            return;
        validateWorkerRegistryTransition(*accepted, candidate);
        const auto finalName = layout_.workerRegistryFile().filename().string();
        acceptedFile = readRegistryFile(directoryFd, finalName);
        if (acceptedFile->status != RegistryFileStatus::Valid || acceptedFile->registry != accepted) {
            throw std::runtime_error("workers.json changed after its accepted generation was loaded");
        }
    } else if (!isExactInitialRegistry(candidate)) {
        throw std::invalid_argument("a fresh data root requires an exact generation-1 worker registry");
    }

    const auto encoded = encodeWorkerRegistryJson(candidate);
    if (encoded.size() > MAX_WORKER_REGISTRY_JSON_BYTES)
        throw std::logic_error("encoded worker registry exceeds its declared format limit");

    const auto temporaryName = layout_.workerRegistryTemporaryFile().filename().string();
    const auto finalName = layout_.workerRegistryFile().filename().string();
    std::optional<FileIdentity> createdTemporaryIdentity;
    bool renamed = false;
    try {
        verifyRootBinding(directoryFd, layout_.root());
        ScopedFd output(
            ::openat(directoryFd, temporaryName.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0644));
        if (output.get() < 0)
            throw std::system_error(errno, std::generic_category(), "cannot create " + temporaryName);
        createdTemporaryIdentity = fileIdentity(output.get(), temporaryName);
        verifyRootBinding(directoryFd, layout_.root());
        observe(WorkerRegistryCommitStage::TemporaryCreated);

        size_t written = 0;
        while (written < encoded.size()) {
            const ssize_t result = ::write(output.get(), encoded.data() + written, encoded.size() - written);
            if (result < 0) {
                if (errno == EINTR)
                    continue;
                throw std::system_error(errno, std::generic_category(), "cannot write " + temporaryName);
            }
            if (result == 0)
                throw std::runtime_error("zero-byte write while creating " + temporaryName);
            written += static_cast<size_t>(result);
        }
        observe(WorkerRegistryCommitStage::TemporaryWritten);

        if (::fsync(output.get()) != 0)
            throw std::system_error(errno, std::generic_category(), "cannot fsync " + temporaryName);
        observe(WorkerRegistryCommitStage::TemporarySynced);

        const int outputFd = output.release();
        if (::close(outputFd) != 0)
            throw std::system_error(errno, std::generic_category(), "cannot close " + temporaryName);
        observe(WorkerRegistryCommitStage::TemporaryClosed);

        verifyRootBinding(directoryFd, layout_.root());
        verifyExactRegistryFile(directoryFd, temporaryName, *createdTemporaryIdentity, encoded);
        if (acceptedFile) {
            verifyExactRegistryFile(directoryFd, finalName, FileIdentity{acceptedFile->device, acceptedFile->inode},
                                    acceptedFile->contents);
        }
        const int renameResult = replacing
                                     ? ::renameat(directoryFd, temporaryName.c_str(), directoryFd, finalName.c_str())
                                     : installNoReplace(directoryFd, temporaryName.c_str(), finalName.c_str());
        if (renameResult != 0) {
            const int error = errno;
            const std::string reason = error == EEXIST ? finalName + " appeared during initial registry creation"
                                                       : "cannot install " + finalName;
            throw std::system_error(error, std::generic_category(), reason);
        }
        renamed = true;
        verifyRootBinding(directoryFd, layout_.root());
        verifyNameIdentity(directoryFd, finalName, *createdTemporaryIdentity);
        observe(WorkerRegistryCommitStage::Renamed);

        syncDirectory(directoryFd, finalName + " was installed but data-root durability is ambiguous");
        verifyRootBinding(directoryFd, layout_.root());
        observe(WorkerRegistryCommitStage::DirectorySynced);
        restabilizeAfterObserver(directoryFd, layout_.root(), finalName, *createdTemporaryIdentity, encoded);
    } catch (...) {
        if (createdTemporaryIdentity && !renamed) {
            try {
                removeTemporaryIfIdentity(directoryFd, layout_.root(), temporaryName, *createdTemporaryIdentity);
            } catch (...) {
                // Preserve the original failure. Any surviving scratch file is
                // handled conservatively by the next locked recovery.
            }
        }
        throw;
    }
}

}  // namespace timestar::cluster

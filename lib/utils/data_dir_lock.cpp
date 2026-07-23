#include "data_dir_lock.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace timestar {

std::string DataDirLock::lockFilePath(const std::string& dataRoot) {
    if (dataRoot.empty())
        return "LOCK";
    if (dataRoot.back() == '/')
        return dataRoot + "LOCK";
    return dataRoot + "/LOCK";
}

namespace {

// Best-effort: whatever the previous holder wrote about itself. Purely for the
// error message — the authority on whether the lock is held is flock(), not
// this content, which may be empty or stale.
std::string readHolderDescription(int fd) {
    char buf[256];
    ssize_t n = ::pread(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0)
        return "unknown process";
    buf[n] = '\0';
    std::string s(buf);
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r'))
        s.pop_back();
    return s.empty() ? "unknown process" : s;
}

}  // namespace

void DataDirLock::acquire(const std::string& dataRoot) {
    release();

    const std::string path = lockFilePath(dataRoot);

    // O_CLOEXEC so a child process (exec) does not inherit the lock and keep it
    // alive past our own exit.
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Could not open data directory lock file '" + path + "': " + std::strerror(errno));
    }

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int err = errno;
        // EWOULDBLOCK is the "someone else holds it" case. Note we do NOT check
        // whether that PID is alive: flock only reports a conflict while the
        // holder's descriptor is open, so a conflict *is* proof of liveness.
        std::string holder = readHolderDescription(fd);
        ::close(fd);
        if (err == EWOULDBLOCK) {
            throw std::runtime_error("Another TimeStar instance is already running against this data directory (" +
                                     path + "), held by " + holder +
                                     ". Stop it first, or start this one with a different [server] data_dir.");
        }
        throw std::runtime_error("Could not lock data directory '" + path + "': " + std::strerror(err));
    }

    // We hold it. Record who, for the next process's error message. Truncate
    // first so a shorter PID cannot leave trailing bytes from a previous owner.
    if (::ftruncate(fd, 0) == 0) {
        std::string desc = "pid " + std::to_string(::getpid()) + "\n";
        ssize_t written = ::pwrite(fd, desc.data(), desc.size(), 0);
        (void)written;  // diagnostics only — never fail the boot over this
    }

    fd_ = fd;
}

void DataDirLock::release() noexcept {
    if (fd_ >= 0) {
        // Closing drops the flock. The file is intentionally left in place.
        ::close(fd_);
        fd_ = -1;
    }
}

DataDirLock::~DataDirLock() {
    release();
}

DataDirLock::DataDirLock(DataDirLock&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

DataDirLock& DataDirLock::operator=(DataDirLock&& other) noexcept {
    if (this != &other) {
        release();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

}  // namespace timestar

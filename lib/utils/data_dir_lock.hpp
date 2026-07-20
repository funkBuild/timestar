#pragma once

// Exclusive lock over a data directory, so two server instances cannot run
// against the same data and corrupt it.
//
// Uses flock(2), which is the right primitive here because the lock lives on
// the open file description, not in the file's contents. The kernel drops it
// when the last descriptor closes — including when the process dies by SIGKILL,
// a segfault, or an OOM kill. There is no stale-lock problem to clean up and no
// PID-liveness check to race against: if the holder is gone, the lock is gone.
//
// The lock FILE is deliberately never unlinked. Unlinking on shutdown would be
// a race — another process may already have opened the same path and be about
// to flock it, and a third could then create a fresh file at that path, leaving
// two processes each holding an exclusive lock on a different inode. A leftover
// file with no lock on it is harmless: the next start opens it and acquires.
//
// Linux-only (flock semantics on NFS before 2.6.12 were emulated and unsafe;
// local filesystems are what this targets).

#include <string>

namespace timestar {

class DataDirLock {
public:
    DataDirLock() = default;
    ~DataDirLock();

    DataDirLock(const DataDirLock&) = delete;
    DataDirLock& operator=(const DataDirLock&) = delete;
    DataDirLock(DataDirLock&& other) noexcept;
    DataDirLock& operator=(DataDirLock&& other) noexcept;

    // Take the lock for `dataRoot`, creating the lock file if needed.
    // Throws std::runtime_error if another LIVE process holds it (the message
    // names the holder's PID) or if the file cannot be opened.
    void acquire(const std::string& dataRoot);

    // Release early. Also done by the destructor.
    void release() noexcept;

    bool held() const noexcept { return fd_ >= 0; }

    // "<dataRoot>/LOCK"
    static std::string lockFilePath(const std::string& dataRoot);

private:
    int fd_ = -1;
};

}  // namespace timestar

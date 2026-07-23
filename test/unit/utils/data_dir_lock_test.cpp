// Tests for the data-directory lock that stops two server instances from
// running against the same data.
//
// The requirement that drives the design: the lock must be released when the
// holder dies by ANY means, including SIGKILL, and the second instance must
// then start normally even though the lock file itself is still on disk.
// flock(2) gives that for free because the lock lives on the open file
// description, not in the file's contents — there is no stale-lock state to
// reap and no PID-liveness check that could race.

#include "../../../lib/utils/data_dir_lock.hpp"

#include <gtest/gtest.h>

#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace {

class DataDirLockTest : public ::testing::Test {
protected:
    std::filesystem::path dir;

    void SetUp() override {
        dir = std::filesystem::temp_directory_path() /
              ("timestar_lock_test_" + std::to_string(::getpid()) + "_" +
               std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(dir);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

}  // namespace

TEST_F(DataDirLockTest, AcquireCreatesTheLockFile) {
    timestar::DataDirLock lock;
    lock.acquire(dir.string());
    EXPECT_TRUE(lock.held());
    EXPECT_TRUE(std::filesystem::exists(timestar::DataDirLock::lockFilePath(dir.string())));
}

TEST_F(DataDirLockTest, LockFilePathIsUnderTheDataRoot) {
    EXPECT_EQ(timestar::DataDirLock::lockFilePath("/var/lib/timestar"), "/var/lib/timestar/LOCK");
    // A trailing slash must not produce a doubled separator.
    EXPECT_EQ(timestar::DataDirLock::lockFilePath("/var/lib/timestar/"), "/var/lib/timestar/LOCK");
}

// A second lock object contends with the first even inside one process: flock
// conflicts across separate open file descriptions, not across processes.
TEST_F(DataDirLockTest, SecondAcquireWhileHeldFails) {
    timestar::DataDirLock first;
    first.acquire(dir.string());

    timestar::DataDirLock second;
    EXPECT_THROW(second.acquire(dir.string()), std::runtime_error);
    EXPECT_FALSE(second.held());
}

TEST_F(DataDirLockTest, ErrorNamesTheHoldingPid) {
    timestar::DataDirLock first;
    first.acquire(dir.string());

    timestar::DataDirLock second;
    try {
        second.acquire(dir.string());
        FAIL() << "expected the second acquire to throw";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("Another TimeStar instance"), std::string::npos) << msg;
        EXPECT_NE(msg.find(std::to_string(::getpid())), std::string::npos)
            << "the message should name the holder's pid, got: " << msg;
    }
}

TEST_F(DataDirLockTest, ReleaseAllowsReacquisition) {
    {
        timestar::DataDirLock first;
        first.acquire(dir.string());
    }  // destructor releases

    timestar::DataDirLock second;
    EXPECT_NO_THROW(second.acquire(dir.string()));
    EXPECT_TRUE(second.held());
}

// The lock file is deliberately left behind. Unlinking it on shutdown would
// race: another process may already have the same path open and be about to
// lock it, while a third creates a fresh file at that path — leaving two
// "exclusive" holders on different inodes.
TEST_F(DataDirLockTest, LockFileSurvivesRelease) {
    const std::string path = timestar::DataDirLock::lockFilePath(dir.string());
    {
        timestar::DataDirLock lock;
        lock.acquire(dir.string());
    }
    EXPECT_TRUE(std::filesystem::exists(path)) << "the file should remain; only the lock is dropped";
}

// THE REQUIREMENT: SIGKILL the holder, then start again.
TEST_F(DataDirLockTest, SigkilledHolderReleasesTheLock) {
    int pipefd[2];
    ASSERT_EQ(::pipe(pipefd), 0);

    pid_t child = ::fork();
    ASSERT_GE(child, 0) << "fork failed";

    if (child == 0) {
        // Child: take the lock, tell the parent, then block forever.
        ::close(pipefd[0]);
        try {
            timestar::DataDirLock lock;
            lock.acquire(dir.string());
            char c = 'k';
            ssize_t w = ::write(pipefd[1], &c, 1);
            (void)w;
            for (;;)
                ::pause();
        } catch (...) {
            char c = 'x';
            ssize_t w = ::write(pipefd[1], &c, 1);
            (void)w;
        }
        ::_exit(0);
    }

    // Parent
    ::close(pipefd[1]);
    char c = 0;
    ASSERT_EQ(::read(pipefd[0], &c, 1), 1) << "child never reported";
    ASSERT_EQ(c, 'k') << "child failed to acquire the lock";
    ::close(pipefd[0]);

    // While the child lives, we must be locked out.
    {
        timestar::DataDirLock contender;
        EXPECT_THROW(contender.acquire(dir.string()), std::runtime_error)
            << "a live holder must block a second instance";
    }

    // Hard-kill: no destructor, no cleanup handler, no chance to tidy up.
    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    // The file is still there, holding a dead process's pid...
    const std::string path = timestar::DataDirLock::lockFilePath(dir.string());
    ASSERT_TRUE(std::filesystem::exists(path));
    {
        std::ifstream in(path);
        std::string contents;
        std::getline(in, contents);
        EXPECT_NE(contents.find(std::to_string(child)), std::string::npos)
            << "expected the dead child's pid to still be recorded, got: " << contents;
    }

    // ...and it must not stop us. The kernel dropped the lock with the process.
    timestar::DataDirLock after;
    EXPECT_NO_THROW(after.acquire(dir.string()))
        << "a SIGKILLed holder must not leave a lock that blocks the next start";
    EXPECT_TRUE(after.held());
}

// Moving must transfer ownership, not double-release.
TEST_F(DataDirLockTest, MoveTransfersOwnership) {
    timestar::DataDirLock first;
    first.acquire(dir.string());

    timestar::DataDirLock second(std::move(first));
    EXPECT_TRUE(second.held());
    EXPECT_FALSE(first.held());  // NOLINT(bugprone-use-after-move) — checking the moved-from state

    // Still exclusive under the new owner.
    timestar::DataDirLock contender;
    EXPECT_THROW(contender.acquire(dir.string()), std::runtime_error);
}

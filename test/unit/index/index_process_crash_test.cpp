#include "../../seastar_gtest.hpp"

#include <spawn.h>
#include <sys/wait.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <system_error>
#include <vector>

extern char** environ;

class IndexProcessCrashTest : public ::testing::Test {
public:
    const std::string path = std::filesystem::absolute("index_process_crash_test").string();
    void SetUp() override { std::filesystem::remove_all(path); }
    void TearDown() override { std::filesystem::remove_all(path); }

    seastar::future<int> run(const std::string& mode) {
        std::vector<std::string> args{
            INDEX_CRASH_WORKER_PATH, mode,   path, "-c", "1", "--memory", "512M", "--overprovisioned",
            "--default-log-level",   "error"};
        std::vector<char*> argv;
        for (auto& arg : args)
            argv.push_back(arg.data());
        argv.push_back(nullptr);
        pid_t child;
        const int err = ::posix_spawn(&child, argv.front(), nullptr, nullptr, argv.data(), environ);
        if (err != 0)
            throw std::system_error(err, std::generic_category(), "spawn index crash worker");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        bool timedOut = false;
        int status = 0;
        while (true) {
            const auto waited = ::waitpid(child, &status, WNOHANG);
            if (waited == child)
                break;
            if (waited < 0 && errno != EINTR)
                throw std::system_error(errno, std::generic_category(), "wait for index crash worker");
            if (std::chrono::steady_clock::now() > deadline) {
                ::kill(child, SIGKILL);
                timedOut = true;
            }
            co_await seastar::sleep(std::chrono::milliseconds(10));
        }
        EXPECT_FALSE(timedOut) << mode;
        co_return timedOut ? -1 : status;
    }
};

SEASTAR_TEST_F(IndexProcessCrashTest, CrashAfterOpenCannotReuseCleanMarker) {
    EXPECT_EQ(co_await self->run("marker-seed"), 0);
    int crashed = co_await self->run("marker-crash");
    EXPECT_TRUE(WIFSIGNALED(crashed));
    EXPECT_EQ(WTERMSIG(crashed), SIGKILL);
    EXPECT_EQ(co_await self->run("marker-verify"), 0);
}

SEASTAR_TEST_F(IndexProcessCrashTest, AckedNewSeriesAndHistoricalBatchSurviveSigkill) {
    EXPECT_EQ(co_await self->run("seed"), 0);
    int crashed = co_await self->run("write-crash");
    EXPECT_TRUE(WIFSIGNALED(crashed));
    EXPECT_EQ(WTERMSIG(crashed), SIGKILL);
    EXPECT_EQ(co_await self->run("verify"), 0);
    // The first recovery must not leave a clean marker sealing an incomplete index.
    EXPECT_EQ(co_await self->run("verify"), 0);
}

SEASTAR_TEST_F(IndexProcessCrashTest, EngineBatchNeedsNoExternalMetadataAnnouncement) {
    EXPECT_EQ(co_await self->run("seed"), 0);
    int crashed = co_await self->run("write-crash-no-metadata");
    EXPECT_TRUE(WIFSIGNALED(crashed));
    EXPECT_EQ(WTERMSIG(crashed), SIGKILL);
    EXPECT_EQ(co_await self->run("verify"), 0);
}

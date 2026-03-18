#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection test: HTTP server shutdown timeout safety
//
// BUG: with_timeout does not cancel the doShutdown() coroutine. On timeout,
// the coroutine continues running while stack locals (server, g_engine) are
// destroyed, causing use-after-free.
//
// FIX: Call std::_Exit(1) on timeout to terminate immediately without stack
// unwinding. WAL recovery handles unflushed data on restart.
// =============================================================================

class HttpServerShutdownTest : public ::testing::Test {};

static std::string readFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

TEST_F(HttpServerShutdownTest, TimeoutPathCallsExitNotReturn) {
    std::string src = readFile("../bin/timestar_http_server.cpp");
    ASSERT_FALSE(src.empty()) << "Could not read timestar_http_server.cpp";

    // After catching timed_out_error, the code must call _Exit (or _exit)
    // to prevent stack unwinding while the shutdown coroutine is still live.

    // Find the timed_out_error catch block
    auto catchPos = src.find("catch (const seastar::timed_out_error&)");
    ASSERT_NE(catchPos, std::string::npos) << "Missing timed_out_error catch block";

    // The _Exit call must appear between the catch and the next closing brace
    auto exitPos = src.find("_Exit(", catchPos);
    auto bracePos = src.find("return 0;", catchPos);

    EXPECT_NE(exitPos, std::string::npos)
        << "Timeout handler must call std::_Exit() to prevent use-after-free";

    // _Exit must come before any return statement
    if (exitPos != std::string::npos && bracePos != std::string::npos) {
        EXPECT_LT(exitPos, bracePos)
            << "_Exit must be called before any return to prevent stack unwinding";
    }
}

/*
 * Simple test to check if coroutines work at all
 */

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

using namespace seastar;

static seastar::future<uint64_t> simple_async_function() {
    std::cerr << "In simple_async_function" << std::endl;
    co_return 42;
}

// Wrapper to run Seastar code within Google Test
static int run_in_seastar(std::function<seastar::future<>()> func) {
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    
    seastar::app_template app;
    try {
        return app.run(argc, argv, [func = std::move(func)]() {
            return func().handle_exception([](std::exception_ptr ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    std::cerr << "Test failed with exception: " << e.what() << std::endl;
                    return make_exception_future<>(ep);
                }
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to run Seastar app: " << e.what() << std::endl;
        return 1;
    }
}

TEST(SimpleCoroutineTest, BasicCoroutine) {
    int result = run_in_seastar([]() -> seastar::future<> {
        std::cerr << "About to call simple_async_function" << std::endl;
        uint64_t value = co_await simple_async_function();
        std::cerr << "Got value: " << value << std::endl;
        EXPECT_EQ(value, 42);
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}
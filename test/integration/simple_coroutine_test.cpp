/*
 * Simple test to check if coroutines work at all
 */

#include "../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

static seastar::future<uint64_t> simple_async_function() {
    std::cerr << "In simple_async_function" << std::endl;
    co_return 42;
}

SEASTAR_TEST(SimpleCoroutineTest, BasicCoroutine) {
    std::cerr << "About to call simple_async_function" << std::endl;
    uint64_t value = co_await simple_async_function();
    std::cerr << "Got value: " << value << std::endl;
    EXPECT_EQ(value, 42);
}

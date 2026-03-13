#pragma once
#include <gtest/gtest.h>

#include <seastar/core/future.hh>

// Use inside seastar::async context where .get() yields to reactor
#define SEASTAR_TEST(suite, name)                             \
    static seastar::future<> seastar_body_##suite##_##name(); \
    TEST(suite, name) {                                       \
        seastar_body_##suite##_##name().get();                \
    }                                                         \
    static seastar::future<> seastar_body_##suite##_##name()

#define SEASTAR_TEST_F(fixture, name)                                        \
    static seastar::future<> seastar_body_##fixture##_##name(fixture* self); \
    TEST_F(fixture, name) {                                                  \
        seastar_body_##fixture##_##name(this).get();                         \
    }                                                                        \
    static seastar::future<> seastar_body_##fixture##_##name([[maybe_unused]] fixture* self)

#include "../../../lib/storage/aligned_buffer.hpp"

#include <gtest/gtest.h>

#include <climits>
#include <cstdint>
#include <limits>

// =============================================================================
// Bug #19: AlignedBuffer grow_uninit and write_bytes missing overflow checks
//
// When current_size + extra_bytes wraps around SIZE_MAX, the result silently
// underflows, leading to incorrect buffer sizing and potential memory
// corruption.  The fix adds overflow checks that throw std::overflow_error.
// =============================================================================

class AlignedBufferOverflowTest : public ::testing::Test {
protected:
    AlignedBuffer buf;
};

TEST_F(AlignedBufferOverflowTest, GrowUninitThrowsOnOverflow) {
    // Write a single byte so current_size > 0
    buf.write<uint8_t>(0x42);
    EXPECT_EQ(buf.size(), 1u);

    // Attempting to grow by SIZE_MAX should wrap around and throw
    EXPECT_THROW(buf.grow_uninit(SIZE_MAX), std::overflow_error);
}

TEST_F(AlignedBufferOverflowTest, WriteBytesThrowsOnOverflow) {
    // Write a single byte so current_size > 0
    buf.write<uint8_t>(0x42);
    EXPECT_EQ(buf.size(), 1u);

    // Attempting to write SIZE_MAX bytes should wrap around and throw
    const char dummy = 'x';
    EXPECT_THROW(buf.write_bytes(&dummy, SIZE_MAX), std::overflow_error);
}

TEST_F(AlignedBufferOverflowTest, GrowUninitZeroBytesDoesNotThrow) {
    // Growing by 0 bytes should always succeed (no overflow possible)
    EXPECT_NO_THROW(buf.grow_uninit(0));
}

TEST_F(AlignedBufferOverflowTest, WriteBytesZeroBytesDoesNotThrow) {
    const char dummy = 'x';
    EXPECT_NO_THROW(buf.write_bytes(&dummy, 0));
}

TEST_F(AlignedBufferOverflowTest, GrowUninitNormalOperationSucceeds) {
    // Normal growth should still work
    auto* ptr = buf.grow_uninit(16);
    EXPECT_NE(ptr, nullptr);
    EXPECT_EQ(buf.size(), 16u);
}

TEST_F(AlignedBufferOverflowTest, WriteBytesNormalOperationSucceeds) {
    const char data[] = "hello";
    buf.write_bytes(data, 5);
    EXPECT_EQ(buf.size(), 5u);
}

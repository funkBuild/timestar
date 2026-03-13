#include "../../../lib/storage/compressed_buffer.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <vector>

class CompressedBufferBoundsTest : public ::testing::Test {};

// --- Normal round-trip operations ---

TEST_F(CompressedBufferBoundsTest, WriteAndReadBackSingleValue) {
    CompressedBuffer buf;
    buf.write(0xDEADBEEF, 32);
    buf.rewind();
    auto value = buf.read<uint64_t>(32);
    EXPECT_EQ(value, 0xDEADBEEFu);
}

TEST_F(CompressedBufferBoundsTest, WriteAndReadBackMultipleValues) {
    CompressedBuffer buf;
    buf.write(42, 8);
    buf.write(255, 8);
    buf.write(1023, 10);
    buf.rewind();
    EXPECT_EQ(buf.read<uint64_t>(8), 42u);
    EXPECT_EQ(buf.read<uint64_t>(8), 255u);
    EXPECT_EQ(buf.read<uint64_t>(10), 1023u);
}

TEST_F(CompressedBufferBoundsTest, WriteAndReadFull64Bits) {
    CompressedBuffer buf;
    buf.write(0xCAFEBABEDEADBEEFull, 64);
    buf.rewind();
    auto value = buf.readFixed<uint64_t, 64>();
    EXPECT_EQ(value, 0xCAFEBABEDEADBEEFull);
}

TEST_F(CompressedBufferBoundsTest, WriteAndReadBits) {
    CompressedBuffer buf;
    // Write 0b1010 pattern as individual bits via 1-bit writes
    buf.write(0, 1);
    buf.write(1, 1);
    buf.write(0, 1);
    buf.write(1, 1);
    buf.rewind();
    EXPECT_FALSE(buf.readBit());
    EXPECT_TRUE(buf.readBit());
    EXPECT_FALSE(buf.readBit());
    EXPECT_TRUE(buf.readBit());
}

TEST_F(CompressedBufferBoundsTest, CrossWordBoundaryRoundTrip) {
    CompressedBuffer buf;
    // Write 60 bits, then 32 bits that span the word boundary
    buf.write(0x0FFFFFFFFFFFFFFFull, 60);
    buf.write(0xABCD1234u, 32);
    buf.rewind();
    auto first = buf.read<uint64_t>(60);
    EXPECT_EQ(first, 0x0FFFFFFFFFFFFFFFull);
    auto second = buf.read<uint64_t>(32);
    EXPECT_EQ(second, 0xABCD1234u);
}

// --- Empty buffer read throws ---

TEST_F(CompressedBufferBoundsTest, EmptyBufferReadThrows) {
    CompressedBuffer buf;
    buf.rewind();  // Set to read mode, but no data written
    EXPECT_THROW(buf.read<uint64_t>(8), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, EmptyBufferReadFixedThrows) {
    CompressedBuffer buf;
    buf.rewind();
    EXPECT_THROW((buf.readFixed<uint64_t, 64>()), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, EmptyBufferReadBitThrows) {
    CompressedBuffer buf;
    buf.rewind();
    EXPECT_THROW(buf.readBit(), std::out_of_range);
}

// --- Reading past end of buffer throws ---

TEST_F(CompressedBufferBoundsTest, ReadPastEndThrows) {
    CompressedBuffer buf;
    buf.write(42, 32);
    buf.rewind();

    // Read the data that exists
    buf.read<uint64_t>(32);
    // Read remaining bits to exhaust the word
    buf.read<uint64_t>(32);

    // Next read should throw - we've consumed the only word
    EXPECT_THROW(buf.read<uint64_t>(8), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, ReadFixedPastEndThrows) {
    CompressedBuffer buf;
    buf.write(0xFFull, 64);
    buf.rewind();

    // Read the one word
    buf.readFixed<uint64_t, 64>();

    // Next read should throw
    EXPECT_THROW((buf.readFixed<uint64_t, 64>()), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, ReadBitPastEndThrows) {
    CompressedBuffer buf;
    buf.write(0x01, 1);
    buf.rewind();

    // Read all 64 bits of the single word
    for (int i = 0; i < 64; i++) {
        EXPECT_NO_THROW(buf.readBit());
    }

    // 65th bit should throw
    EXPECT_THROW(buf.readBit(), std::out_of_range);
}

// --- Cross-boundary read where second word is out of bounds ---

TEST_F(CompressedBufferBoundsTest, CrossBoundarySecondWordOutOfBoundsThrows_Read) {
    CompressedBuffer buf;
    // Write only enough to fill one word
    buf.write(0xFFFFFFFFFFFFFFFFull, 64);
    buf.rewind();

    // Read 32 bits (consumes half the word)
    buf.read<uint64_t>(32);

    // Try to read 64 bits - this would need to cross into a second word that doesn't exist
    EXPECT_THROW(buf.read<uint64_t>(64), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, CrossBoundarySecondWordOutOfBoundsThrows_ReadFixed) {
    CompressedBuffer buf;
    buf.write(0xFFFFFFFFFFFFFFFFull, 64);
    buf.rewind();

    // Read 32 bits to position halfway through the word
    buf.read<uint64_t>(32);

    // Try to read 64 bits spanning into non-existent second word
    EXPECT_THROW((buf.readFixed<uint64_t, 64>()), std::out_of_range);
}

// --- Multiple reads exhausting the buffer ---

TEST_F(CompressedBufferBoundsTest, MultipleReadsExhaustBufferThenThrows) {
    CompressedBuffer buf;
    buf.write(1, 64);
    buf.write(2, 64);
    buf.rewind();

    // Read both words successfully
    auto val1 = buf.readFixed<uint64_t, 64>();
    EXPECT_EQ(val1, 1u);
    auto val2 = buf.readFixed<uint64_t, 64>();
    EXPECT_EQ(val2, 2u);

    // Third read should throw
    EXPECT_THROW((buf.readFixed<uint64_t, 64>()), std::out_of_range);
}

// --- isAtEnd() consistency ---

TEST_F(CompressedBufferBoundsTest, IsAtEndBeforeAnyRead) {
    CompressedBuffer buf;
    buf.rewind();
    // Empty buffer: offset=0, data.size()=0, so isAtEnd() should be true
    EXPECT_TRUE(buf.isAtEnd());
}

TEST_F(CompressedBufferBoundsTest, IsAtEndFalseWithData) {
    CompressedBuffer buf;
    buf.write(42, 64);
    buf.rewind();
    EXPECT_FALSE(buf.isAtEnd());
}

// --- Direct data manipulation to simulate corrupted/truncated data ---

TEST_F(CompressedBufferBoundsTest, TruncatedBufferThrowsOnRead) {
    CompressedBuffer buf;
    // Write two words of data
    buf.write(0xAAAAAAAAAAAAAAAAull, 64);
    buf.write(0xBBBBBBBBBBBBBBBBull, 64);

    // Truncate to one word (simulating corrupted/truncated data)
    buf.data.resize(1);
    buf.rewind();

    // First word reads fine
    auto firstWord = buf.readFixed<uint64_t, 64>();
    EXPECT_EQ(firstWord, 0xAAAAAAAAAAAAAAAAull);

    // Second word should throw since we truncated
    EXPECT_THROW((buf.readFixed<uint64_t, 64>()), std::out_of_range);
}

TEST_F(CompressedBufferBoundsTest, ReadSmallBitsFromEmptyThrows) {
    CompressedBuffer buf;
    buf.rewind();
    // Even reading 1 bit from an empty buffer should throw
    EXPECT_THROW(buf.read<uint64_t>(1), std::out_of_range);
}

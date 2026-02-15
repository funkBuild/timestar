#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <vector>
#include <stdexcept>

#include "../../../lib/storage/slice_buffer.hpp"

class CompressedSliceTest : public ::testing::Test {
protected:
    // Helper: create a buffer of uint64_t values and return as uint8_t pointer
    static std::vector<uint8_t> makeAlignedBuffer(const std::vector<uint64_t>& words) {
        std::vector<uint8_t> buf(words.size() * 8);
        std::memcpy(buf.data(), words.data(), buf.size());
        return buf;
    }
};

// --- Normal read operations ---

TEST_F(CompressedSliceTest, ReadFixed64ReturnsCorrectValue) {
    uint64_t word = 0xDEADBEEFCAFEBABEull;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    auto result = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(result, word);
}

TEST_F(CompressedSliceTest, ReadBitSequence) {
    // 0x05 = 0b...0101 in the lowest bits
    uint64_t word = 0x05;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    EXPECT_TRUE(slice.readBit());   // bit 0 = 1
    EXPECT_FALSE(slice.readBit());  // bit 1 = 0
    EXPECT_TRUE(slice.readBit());   // bit 2 = 1
    EXPECT_FALSE(slice.readBit());  // bit 3 = 0
}

TEST_F(CompressedSliceTest, ReadSmallBitCounts) {
    // Store value 0b1101 = 13 in lowest 4 bits
    uint64_t word = 13;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    uint64_t result = slice.read<uint64_t>(4);
    EXPECT_EQ(result, 13u);
}

TEST_F(CompressedSliceTest, ReadAcrossWordBoundary) {
    // Put data that will span across two uint64_t words.
    // Write 32 bits of 0s, then 64 bits of a known value across the boundary.
    uint64_t word0 = 0x00000000FFFFFFFFull;  // lower 32 bits set
    uint64_t word1 = 0x00000000DEADBEEF;
    auto buf = makeAlignedBuffer({word0, word1});
    CompressedSlice slice(buf.data(), buf.size());

    // Read 32 bits from first word
    uint64_t first = slice.read<uint64_t>(32);
    EXPECT_EQ(first, 0xFFFFFFFFu);

    // Read 64 bits spanning word boundary: 32 bits from word0 upper + 32 bits from word1 lower
    uint64_t cross = slice.read<uint64_t>(64);
    // Upper 32 bits of word0 are 0, lower 32 bits of word1 are 0xDEADBEEF
    EXPECT_EQ(cross, 0xDEADBEEF00000000ull);
}

TEST_F(CompressedSliceTest, ReadMultipleWords) {
    uint64_t word0 = 0x1111111111111111ull;
    uint64_t word1 = 0x2222222222222222ull;
    uint64_t word2 = 0x3333333333333333ull;
    auto buf = makeAlignedBuffer({word0, word1, word2});
    CompressedSlice slice(buf.data(), buf.size());

    auto r0 = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(r0, word0);
    auto r1 = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(r1, word1);
    auto r2 = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(r2, word2);
}

// --- Bounds checking: read ---

TEST_F(CompressedSliceTest, ReadBeyondBoundsThrows) {
    uint64_t word = 0x42;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    // Read the only word
    (slice.readFixed<uint64_t, 64>());

    // Attempt to read past end should throw
    EXPECT_THROW((slice.readFixed<uint64_t, 64>()), std::runtime_error);
}

TEST_F(CompressedSliceTest, ReadBeyondBoundsOnCrossBoundaryThrows) {
    uint64_t word = 0x42;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    // Read 32 bits (consumes half of the only word)
    slice.read<uint64_t>(32);

    // Try to read 64 bits - needs to cross into a second word that doesn't exist
    EXPECT_THROW(slice.read<uint64_t>(64), std::runtime_error);
}

TEST_F(CompressedSliceTest, ReadDynamic_BeyondBoundsThrows) {
    uint64_t word = 0xAB;
    auto buf = makeAlignedBuffer({word});
    CompressedSlice slice(buf.data(), buf.size());

    slice.read<uint64_t>(64);

    // Should throw on next read since we've exhausted the buffer
    EXPECT_THROW(slice.read<uint64_t>(1), std::runtime_error);
}

// --- Bounds checking: readBit ---

TEST_F(CompressedSliceTest, ReadBitBeyondBoundsThrows) {
    // Single byte of data -> 1 word (ceiling division)
    uint8_t byte = 0xFF;
    CompressedSlice slice(&byte, 1);

    // Read all 64 bits of the single word (1 byte + 7 padding zeros)
    for (int i = 0; i < 64; i++) {
        EXPECT_NO_THROW(slice.readBit());
    }

    // 65th bit should throw since there's no second word
    EXPECT_THROW(slice.readBit(), std::runtime_error);
}

// --- Unaligned input data ---

TEST_F(CompressedSliceTest, UnalignedInputDataWorks) {
    // Create a buffer with 1-byte offset to ensure misalignment
    std::vector<uint8_t> buf(17, 0);  // 1 byte padding + 16 bytes of data
    uint64_t word = 0xCAFEBABEDEADBEEFull;
    std::memcpy(buf.data() + 1, &word, 8);  // Write at offset 1 (unaligned for uint64_t)

    // Construct from the unaligned pointer
    CompressedSlice slice(buf.data() + 1, 8);

    auto result = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(result, word);
}

TEST_F(CompressedSliceTest, UnalignedInputMultipleReads) {
    // Create unaligned buffer with two words
    std::vector<uint8_t> buf(19, 0);  // 3 byte offset + 16 bytes of data
    uint64_t words[2] = {0x1234567890ABCDEFull, 0xFEDCBA0987654321ull};
    std::memcpy(buf.data() + 3, words, 16);

    CompressedSlice slice(buf.data() + 3, 16);

    auto r0 = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(r0, words[0]);
    auto r1 = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(r1, words[1]);
}

// --- Trailing bytes (non-multiple-of-8 lengths) ---

TEST_F(CompressedSliceTest, TrailingBytesHandled) {
    // 10 bytes of data: should result in 2 words (ceiling of 10/8 = 2)
    std::vector<uint8_t> buf = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xAB, 0xCD};
    CompressedSlice slice(buf.data(), buf.size());

    // First word should be all 0xFF
    auto first = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(first, 0xFFFFFFFFFFFFFFFFull);

    // Second word should have 0xCDAB in lowest 16 bits, rest zeroed (padding)
    auto second = (slice.readFixed<uint64_t, 64>());
    EXPECT_EQ(second & 0xFFFF, 0xCDABu);
    EXPECT_EQ(second >> 16, 0u);  // Padding bytes should be zero
}

TEST_F(CompressedSliceTest, SingleByteInput) {
    uint8_t byte = 0x42;
    CompressedSlice slice(&byte, 1);

    // Should be able to read bit-by-bit from the single word
    // 0x42 = 0b01000010
    EXPECT_FALSE(slice.readBit());  // bit 0 = 0
    EXPECT_TRUE(slice.readBit());   // bit 1 = 1
    EXPECT_FALSE(slice.readBit());  // bit 2 = 0
    EXPECT_FALSE(slice.readBit());  // bit 3 = 0
    EXPECT_FALSE(slice.readBit());  // bit 4 = 0
    EXPECT_FALSE(slice.readBit());  // bit 5 = 0
    EXPECT_TRUE(slice.readBit());   // bit 6 = 1
    EXPECT_FALSE(slice.readBit());  // bit 7 = 0
}

TEST_F(CompressedSliceTest, ThreeBytesInput) {
    // 3 bytes -> should ceil to 1 word
    std::vector<uint8_t> buf = {0x01, 0x02, 0x03};
    CompressedSlice slice(buf.data(), buf.size());

    // Read 24 bits of actual data
    uint64_t value = slice.read<uint64_t>(24);
    EXPECT_EQ(value, 0x030201u);  // little-endian byte order
}

// --- Verifying the Slice class std::move removal doesn't break anything ---

TEST_F(CompressedSliceTest, SliceReadStringWorks) {
    const char* testStr = "hello world";
    size_t len = std::strlen(testStr);
    Slice slice(reinterpret_cast<const uint8_t*>(testStr), len);

    std::string result = slice.readString(5);
    EXPECT_EQ(result, "hello");
}

TEST_F(CompressedSliceTest, SliceGetSliceWorks) {
    std::vector<uint8_t> buf = {1, 2, 3, 4, 5, 6, 7, 8};
    Slice slice(buf.data(), buf.size());

    Slice sub = slice.getSlice(4);
    EXPECT_EQ(sub.read<uint8_t>(), 1);
    EXPECT_EQ(sub.read<uint8_t>(), 2);
}

TEST_F(CompressedSliceTest, SliceGetCompressedSliceWorks) {
    uint64_t word = 0xABCDEF0123456789ull;
    std::vector<uint8_t> buf(8);
    std::memcpy(buf.data(), &word, 8);

    Slice slice(buf.data(), buf.size());
    CompressedSlice cs = slice.getCompressedSlice(8);

    auto result = (cs.readFixed<uint64_t, 64>());
    EXPECT_EQ(result, word);
}

// --- Edge case: zero-length input ---

TEST_F(CompressedSliceTest, ZeroLengthThrowsOnRead) {
    uint8_t dummy = 0;
    CompressedSlice slice(&dummy, 0);

    EXPECT_THROW(slice.readBit(), std::runtime_error);
    EXPECT_THROW(slice.read<uint64_t>(1), std::runtime_error);
}

// Tests for TSM readSingleBlock and decodeBlock with corrupted, type-mismatched,
// empty, and truncated block data.
//
// Two test classes are provided:
//
//   TSMDecodeBlockTest  — exercises TSM::decodeBlock() directly (synchronous,
//                         no Seastar reactor required).  A TSM instance is
//                         constructed with a dummy filename (the file is never
//                         opened) purely to access the non-static member.
//
//   TSMBlockCorruptionTest — exercises TSM::readSingleBlock() end-to-end using
//                            real on-disk files.  Each test writes a valid TSM
//                            file via TSMWriter, patches specific bytes at the
//                            block data offset, then calls readSingleBlock()
//                            with the known index block coordinates.  All async
//                            operations are driven synchronously via .get() so
//                            these tests can run in a plain GTest binary that
//                            already has a Seastar reactor thread running.
//
// Block header layout (BLOCK_HEADER_SIZE = 9 bytes):
//   offset 0      : uint8_t  — type byte  (TSMValueType enum)
//   offsets 1-4   : uint32_t — timestampSize (count of timestamps in block)
//   offsets 5-8   : uint32_t — timestampBytes (byte length of encoded timestamps)
//   offsets 9+    : compressed timestamps, then compressed values

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/slice_buffer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// PART 1: TSM::decodeBlock() — synchronous, no file I/O
//
// decodeBlock() is a public non-static member but uses no `this` state (all
// logic is in the Slice parameter and constexpr type dispatch), so any
// TSM instance constructed from a syntactically valid filename works.
// ---------------------------------------------------------------------------

class TSMDecodeBlockTest : public ::testing::Test {
protected:
    // A TSM whose file is never opened — used solely as the receiver for
    // the decodeBlock() call.
    TSM dummyTsm{"0_999.tsm"};

    // Builds the 9-byte BLOCK_HEADER_SIZE prefix for a block.
    //   typeVal        : raw uint8_t stored in the file
    //   timestampSize  : number of timestamp entries declared in the block
    //   timestampBytes : byte length of the compressed timestamp region
    static std::vector<uint8_t> makeHeader(uint8_t typeVal, uint32_t timestampSize, uint32_t timestampBytes) {
        std::vector<uint8_t> hdr(9);
        hdr[0] = typeVal;
        std::memcpy(hdr.data() + 1, &timestampSize, 4);
        std::memcpy(hdr.data() + 5, &timestampBytes, 4);
        return hdr;
    }
};

// ---------------------------------------------------------------------------
// Type-mismatch: Float block (type byte == 0) decoded as bool
// Expected: std::runtime_error("TSM block type mismatch: ...")
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_FloatBlockDecodedAsBool) {
    // Build a minimal Float-labelled header followed by enough padding to
    // satisfy the size guard (blockSize >= 9).
    auto hdr = makeHeader(/*type=Float*/ 0, /*timestampSize=*/1, /*timestampBytes=*/0);
    // Pad with zeros so blockSize == 16 and decodeBlock does not return nullptr
    hdr.resize(16, 0x00);

    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<bool>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: Float block (type byte == 0) decoded as string
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_FloatBlockDecodedAsString) {
    auto hdr = makeHeader(0, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<std::string>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: Boolean block (type byte == 1) decoded as double
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_BoolBlockDecodedAsDouble) {
    auto hdr = makeHeader(/*type=Boolean*/ 1, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<double>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: Boolean block (type byte == 1) decoded as string
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_BoolBlockDecodedAsString) {
    auto hdr = makeHeader(1, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<std::string>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: String block (type byte == 2) decoded as double
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_StringBlockDecodedAsDouble) {
    auto hdr = makeHeader(/*type=String*/ 2, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<double>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: String block (type byte == 2) decoded as bool
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_StringBlockDecodedAsBool) {
    auto hdr = makeHeader(2, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<bool>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: Integer block (type byte == 3) decoded as double
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_IntegerBlockDecodedAsDouble) {
    auto hdr = makeHeader(/*type=Integer*/ 3, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<double>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Type-mismatch: Invalid/unknown type byte (0xFF) decoded as double
// 0xFF is not a valid TSMValueType, so static_cast<TSMValueType>(0xFF) !=
// TSMValueType::Float and the mismatch check fires.
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TypeMismatch_InvalidTypeByte) {
    auto hdr = makeHeader(0xFF, 1, 0);
    hdr.resize(16, 0x00);
    uint32_t blockSize = static_cast<uint32_t>(hdr.size());
    Slice blockSlice(hdr.data(), hdr.size());

    EXPECT_THROW((dummyTsm.decodeBlock<double>(blockSlice, blockSize, 0, UINT64_MAX)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Zero-length block: blockSize == 0 is below BLOCK_HEADER_SIZE (9 bytes).
// decodeBlock() returns nullptr without throwing.
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, ZeroBlockSize_ReturnsNullptr) {
    std::vector<uint8_t> emptyBuf;
    Slice blockSlice(emptyBuf.data(), 0);

    auto result = dummyTsm.decodeBlock<double>(blockSlice, /*blockSize=*/0, 0, UINT64_MAX);
    EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// Partial header (8 bytes): below BLOCK_HEADER_SIZE (9 bytes).
// decodeBlock() returns nullptr without throwing.
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, PartialHeader_8Bytes_ReturnsNullptr) {
    std::vector<uint8_t> buf(8, 0x00);
    Slice blockSlice(buf.data(), buf.size());

    auto result = dummyTsm.decodeBlock<double>(blockSlice, /*blockSize=*/8, 0, UINT64_MAX);
    EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// Exactly BLOCK_HEADER_SIZE bytes with correct type but with timestampBytes
// exceeding the remaining block payload.
// decodeBlock() returns nullptr (the defensive timestampBytes check).
// ---------------------------------------------------------------------------
TEST_F(TSMDecodeBlockTest, TimestampBytesExceedsBlockPayload_ReturnsNullptr) {
    // blockSize = 9 (header only).  timestampBytes = 100 > 9 - 9 = 0.
    // The defensive check: timestampBytes > blockSize - BLOCK_HEADER_SIZE fires.
    auto hdr = makeHeader(/*Float*/ 0, /*timestampSize=*/5, /*timestampBytes=*/100);
    uint32_t blockSize = 9;  // header-only block, matches hdr.size()
    Slice blockSlice(hdr.data(), hdr.size());

    auto result = dummyTsm.decodeBlock<double>(blockSlice, blockSize, 0, UINT64_MAX);
    EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// PART 2: TSM::readSingleBlock() — Seastar reactor required
//
// Strategy: write a valid TSM file, record the exact block offset and size
// from the index, optionally corrupt specific bytes in the file using
// standard POSIX I/O, open the TSM, and call readSingleBlock() via .get().
//
// For type-mismatch tests the corruption is: overwrite the 1-byte type field
// at block_offset+0 with a wrong value.
// For truncated-block tests we reduce the declared block size in the
// TSMIndexBlock we pass to readSingleBlock().
// ---------------------------------------------------------------------------

class TSMBlockCorruptionTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_block_corruption";

    void SetUp() override { fs::create_directories(testDir); }

    void TearDown() override { fs::remove_all(testDir); }

    std::string getTestFilePath(const std::string& name) { return testDir + "/" + name; }

    // Patch a single byte at the given file offset.
    static void patchFileByte(const std::string& path, uint64_t fileOffset, uint8_t value) {
        int fd = ::open(path.c_str(), O_WRONLY);
        ASSERT_GE(fd, 0) << "Failed to open file for patching: " << path;
        ASSERT_EQ(::pwrite(fd, &value, 1, static_cast<off_t>(fileOffset)), 1)
            << "pwrite failed at offset " << fileOffset;
        ::close(fd);
    }
};

// ---------------------------------------------------------------------------
// readSingleBlock for Float data — write a valid float TSM, read with the
// correct template parameter.  Verifies the happy path (no corruption).
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockFloatHappyPath(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.float.happy");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000, 3000};
        std::vector<double> vs = {1.1, 2.2, 3.3};
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    EXPECT_FALSE(entry->indexBlocks.empty());

    TSMIndexBlock indexBlock = entry->indexBlocks[0];
    auto block = co_await tsm.readSingleBlock<double>(indexBlock, 0, UINT64_MAX);

    EXPECT_NE(block, nullptr);
    if (block) {
        EXPECT_EQ(block->timestamps.size(), 3u);
        EXPECT_EQ(block->values.size(), 3u);
        EXPECT_DOUBLE_EQ(block->values[0], 1.1);
        EXPECT_DOUBLE_EQ(block->values[2], 3.3);
    }

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_FloatHappyPath) {
    testReadSingleBlockFloatHappyPath(getTestFilePath("0_200.tsm")).get();
}

// ---------------------------------------------------------------------------
// readSingleBlock for String data — verifies the happy path for strings.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockStringHappyPath(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.string.happy");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100, 200, 300};
        std::vector<std::string> vs = {"alpha", "beta", "gamma"};
        writer.writeSeries(TSMValueType::String, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    EXPECT_FALSE(entry->indexBlocks.empty());

    TSMIndexBlock indexBlock = entry->indexBlocks[0];
    auto block = co_await tsm.readSingleBlock<std::string>(indexBlock, 0, UINT64_MAX);

    EXPECT_NE(block, nullptr);
    if (block) {
        EXPECT_EQ(block->timestamps.size(), 3u);
        EXPECT_EQ(block->values[0], "alpha");
        EXPECT_EQ(block->values[2], "gamma");
    }

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_StringHappyPath) {
    testReadSingleBlockStringHappyPath(getTestFilePath("0_201.tsm")).get();
}

// ---------------------------------------------------------------------------
// Type-mismatch via file corruption: write a Float block, then overwrite
// the type byte (at block_offset+0) with 1 (Boolean).
// readSingleBlock<double> must throw std::runtime_error.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockTypeMismatchFloatCorruptedToBool(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.type.float2bool");

    // Write valid float block.
    uint64_t blockOffset = 0;
    uint32_t blockSize = 0;
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {5000, 6000, 7000};
        std::vector<double> vs = {9.9, 8.8, 7.7};
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    // Open to get the block offset, then close.
    {
        TSM tsm(filename);
        co_await tsm.open();
        auto* entry = co_await tsm.getFullIndexEntry(seriesId);
        EXPECT_NE(entry, nullptr);
        blockOffset = entry->indexBlocks[0].offset;
        blockSize = entry->indexBlocks[0].size;
        co_await tsm.close();
    }

    // Corrupt: overwrite type byte (at blockOffset+0) with 1 (Boolean).
    {
        int fd = ::open(filename.c_str(), O_WRONLY);
        EXPECT_GE(fd, 0);
        uint8_t badType = 1;  // Boolean
        [[maybe_unused]] auto pwriteRet1 = ::pwrite(fd, &badType, 1, static_cast<off_t>(blockOffset));
        ::close(fd);
    }

    // Re-open and attempt to read as Float — must throw.
    TSM tsm(filename);
    co_await tsm.open();

    TSMIndexBlock corruptedIndexBlock{};
    corruptedIndexBlock.offset = blockOffset;
    corruptedIndexBlock.size = blockSize;

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<double>(corruptedIndexBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception message, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for type mismatch";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_TypeMismatch_FloatCorruptedToBool) {
    testReadSingleBlockTypeMismatchFloatCorruptedToBool(getTestFilePath("0_202.tsm")).get();
}

// ---------------------------------------------------------------------------
// Type-mismatch: write a Boolean block, corrupt type byte to Float (0),
// then readSingleBlock<bool> must throw.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockTypeMismatchBoolCorruptedToFloat(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.type.bool2float");

    uint64_t blockOffset = 0;
    uint32_t blockSize = 0;
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000};
        std::vector<bool> vs = {true, false};
        writer.writeSeries(TSMValueType::Boolean, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    {
        TSM tsm(filename);
        co_await tsm.open();
        auto* entry = co_await tsm.getFullIndexEntry(seriesId);
        EXPECT_NE(entry, nullptr);
        blockOffset = entry->indexBlocks[0].offset;
        blockSize = entry->indexBlocks[0].size;
        co_await tsm.close();
    }

    // Corrupt type byte to 0 (Float).
    {
        int fd = ::open(filename.c_str(), O_WRONLY);
        EXPECT_GE(fd, 0);
        uint8_t badType = 0;
        [[maybe_unused]] auto pwriteRet = ::pwrite(fd, &badType, 1, static_cast<off_t>(blockOffset));
        ::close(fd);
    }

    TSM tsm(filename);
    co_await tsm.open();

    TSMIndexBlock corruptedBlock{};
    corruptedBlock.offset = blockOffset;
    corruptedBlock.size = blockSize;

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<bool>(corruptedBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception message, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for type mismatch";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_TypeMismatch_BoolCorruptedToFloat) {
    testReadSingleBlockTypeMismatchBoolCorruptedToFloat(getTestFilePath("0_203.tsm")).get();
}

// ---------------------------------------------------------------------------
// Type-mismatch: write a String block, corrupt type byte to Float (0),
// then readSingleBlock<std::string> must throw.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockTypeMismatchStringCorruptedToFloat(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.type.str2float");

    uint64_t blockOffset = 0;
    uint32_t blockSize = 0;
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100, 200};
        std::vector<std::string> vs = {"x", "y"};
        writer.writeSeries(TSMValueType::String, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    {
        TSM tsm(filename);
        co_await tsm.open();
        auto* entry = co_await tsm.getFullIndexEntry(seriesId);
        EXPECT_NE(entry, nullptr);
        blockOffset = entry->indexBlocks[0].offset;
        blockSize = entry->indexBlocks[0].size;
        co_await tsm.close();
    }

    // Corrupt type byte to 0 (Float).
    {
        int fd = ::open(filename.c_str(), O_WRONLY);
        EXPECT_GE(fd, 0);
        uint8_t badType = 0;
        [[maybe_unused]] auto pwriteRet = ::pwrite(fd, &badType, 1, static_cast<off_t>(blockOffset));
        ::close(fd);
    }

    TSM tsm(filename);
    co_await tsm.open();

    TSMIndexBlock corruptedBlock{};
    corruptedBlock.offset = blockOffset;
    corruptedBlock.size = blockSize;

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<std::string>(corruptedBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for type mismatch";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_TypeMismatch_StringCorruptedToFloat) {
    testReadSingleBlockTypeMismatchStringCorruptedToFloat(getTestFilePath("0_204.tsm")).get();
}

// ---------------------------------------------------------------------------
// Cross-type read: write Float block, try to read it as String (no file
// corruption — the on-disk type byte is already mismatched from <std::string>).
// readSingleBlock<std::string> must throw.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockFloatReadAsString(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.crosstype.float.as.str");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000, 3000};
        std::vector<double> vs = {1.0, 2.0, 3.0};
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);

    TSMIndexBlock indexBlock = entry->indexBlocks[0];

    bool threw = false;
    try {
        // The block on disk has type byte 0 (Float) but we request <std::string>
        co_await tsm.readSingleBlock<std::string>(indexBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for Float-block read as String";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_FloatBlockReadAsString_Throws) {
    testReadSingleBlockFloatReadAsString(getTestFilePath("0_205.tsm")).get();
}

// ---------------------------------------------------------------------------
// Cross-type read: write String block, try to read it as Bool (no corruption).
// readSingleBlock<bool> must throw.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockStringReadAsBool(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.crosstype.str.as.bool");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100};
        std::vector<std::string> vs = {"hello"};
        writer.writeSeries(TSMValueType::String, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);

    TSMIndexBlock indexBlock = entry->indexBlocks[0];

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<bool>(indexBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for String-block read as Bool";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_StringBlockReadAsBool_Throws) {
    testReadSingleBlockStringReadAsBool(getTestFilePath("0_206.tsm")).get();
}

// ---------------------------------------------------------------------------
// Time-range exclusion: read a Float block with a time window that excludes
// all timestamps.  The block must be returned (non-null) but its timestamps
// vector must be empty (decoder filters out all points).
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockTimeRangeExcludesAll(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.timerange.excl");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000, 3000};
        std::vector<double> vs = {1.0, 2.0, 3.0};
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);

    TSMIndexBlock indexBlock = entry->indexBlocks[0];

    // Query window [10000, 20000] — all block timestamps are below 10000.
    auto block = co_await tsm.readSingleBlock<double>(indexBlock, /*startTime=*/10000, /*endTime=*/20000);

    // readSeries() skips blocks with empty timestamps, but readSingleBlock
    // itself returns the (empty) block — callers decide whether to discard it.
    EXPECT_NE(block, nullptr);
    if (block) {
        EXPECT_EQ(block->timestamps.size(), 0u) << "Expected zero timestamps after time-range exclusion";
    }

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_TimeRangeExcludesAll_EmptyResult) {
    testReadSingleBlockTimeRangeExcludesAll(getTestFilePath("0_207.tsm")).get();
}

// ---------------------------------------------------------------------------
// Truncated block: the TSMIndexBlock.size is set to only 4 bytes (less than
// BLOCK_HEADER_SIZE = 9).  readSingleBlock reads exactly that many bytes from
// disk and then tries to parse the header — Slice::getSlice(9) throws because
// only 4 bytes are available.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockTruncatedBlockThrows(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.truncated");

    uint64_t blockOffset = 0;
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {1000, 2000};
        std::vector<double> vs = {5.5, 6.6};
        writer.writeSeries(TSMValueType::Float, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    {
        TSM tsm(filename);
        co_await tsm.open();
        auto* entry = co_await tsm.getFullIndexEntry(seriesId);
        EXPECT_NE(entry, nullptr);
        blockOffset = entry->indexBlocks[0].offset;
        co_await tsm.close();
    }

    // Re-open and call readSingleBlock with a synthetic TSMIndexBlock whose
    // size is 4 — smaller than BLOCK_HEADER_SIZE (9).  The DMA read will
    // return 4 bytes, then getSlice(9) on the Slice will throw.
    TSM tsm(filename);
    co_await tsm.open();

    TSMIndexBlock truncatedBlock{};
    truncatedBlock.offset = blockOffset;
    truncatedBlock.size = 4;  // too small for the 9-byte header

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<double>(truncatedBlock, 0, UINT64_MAX);
    } catch (const std::exception& e) {
        threw = true;
        // The error should mention bounds or getSlice failure.
        std::string msg(e.what());
        EXPECT_FALSE(msg.empty()) << "Expected non-empty exception message for truncated block";
    }
    EXPECT_TRUE(threw) << "Expected an exception when block size < BLOCK_HEADER_SIZE";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_TruncatedBlock_Throws) {
    testReadSingleBlockTruncatedBlockThrows(getTestFilePath("0_208.tsm")).get();
}

// ---------------------------------------------------------------------------
// Boolean block — happy path readSingleBlock for bool, verifying the bool
// decoder path through readSingleBlock is exercised end-to-end.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockBoolHappyPath(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.bool.happy");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100, 200, 300, 400};
        std::vector<bool> vs = {true, false, true, true};
        writer.writeSeries(TSMValueType::Boolean, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    EXPECT_FALSE(entry->indexBlocks.empty());

    TSMIndexBlock indexBlock = entry->indexBlocks[0];
    auto block = co_await tsm.readSingleBlock<bool>(indexBlock, 0, UINT64_MAX);

    EXPECT_NE(block, nullptr);
    if (block) {
        EXPECT_EQ(block->timestamps.size(), 4u);
        EXPECT_EQ(block->values.size(), 4u);
        EXPECT_EQ(block->values[0], true);
        EXPECT_EQ(block->values[1], false);
        EXPECT_EQ(block->values[2], true);
        EXPECT_EQ(block->values[3], true);
    }

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_BoolHappyPath) {
    testReadSingleBlockBoolHappyPath(getTestFilePath("0_209.tsm")).get();
}

// ---------------------------------------------------------------------------
// Integer block — happy path readSingleBlock for int64_t.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockIntegerHappyPath(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.integer.happy");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100, 200, 300};
        std::vector<int64_t> vs = {-10, 0, 42};
        writer.writeSeries(TSMValueType::Integer, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);
    EXPECT_FALSE(entry->indexBlocks.empty());

    TSMIndexBlock indexBlock = entry->indexBlocks[0];
    auto block = co_await tsm.readSingleBlock<int64_t>(indexBlock, 0, UINT64_MAX);

    EXPECT_NE(block, nullptr);
    if (block) {
        EXPECT_EQ(block->timestamps.size(), 3u);
        EXPECT_EQ(block->values.size(), 3u);
        EXPECT_EQ(block->values[0], -10);
        EXPECT_EQ(block->values[1], 0);
        EXPECT_EQ(block->values[2], 42);
    }

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_IntegerHappyPath) {
    testReadSingleBlockIntegerHappyPath(getTestFilePath("0_210.tsm")).get();
}

// ---------------------------------------------------------------------------
// Cross-type: write Integer block, read as Float — must throw type mismatch.
// ---------------------------------------------------------------------------
seastar::future<> testReadSingleBlockIntegerReadAsFloat(std::string filename) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("corrupt.crosstype.int.as.float");

    {
        TSMWriter writer(filename);
        std::vector<uint64_t> ts = {100, 200};
        std::vector<int64_t> vs = {7, 8};
        writer.writeSeries(TSMValueType::Integer, seriesId, ts, vs);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    co_await tsm.open();

    auto* entry = co_await tsm.getFullIndexEntry(seriesId);
    EXPECT_NE(entry, nullptr);

    TSMIndexBlock indexBlock = entry->indexBlocks[0];

    bool threw = false;
    try {
        co_await tsm.readSingleBlock<double>(indexBlock, 0, UINT64_MAX);
    } catch (const std::runtime_error& e) {
        threw = true;
        std::string msg(e.what());
        EXPECT_NE(msg.find("type mismatch"), std::string::npos)
            << "Expected 'type mismatch' in exception, got: " << msg;
    }
    EXPECT_TRUE(threw) << "Expected std::runtime_error for Integer-block read as Float";

    co_await tsm.close();
}

TEST_F(TSMBlockCorruptionTest, ReadSingleBlock_IntegerBlockReadAsFloat_Throws) {
    testReadSingleBlockIntegerReadAsFloat(getTestFilePath("0_211.tsm")).get();
}

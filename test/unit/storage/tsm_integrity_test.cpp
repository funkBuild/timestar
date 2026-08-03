#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/utils/crc32.hpp"
#include "../../seastar_gtest.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class TSMIntegrityTest : public ::testing::Test {
public:
    std::string testDir = "./test_tsm_integrity";

    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }
    std::string path(const std::string& name) const { return testDir + "/" + name; }
};

template <typename T>
T load(const std::vector<uint8_t>& bytes, size_t offset) {
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset) {
        throw std::runtime_error("test fixture read outside file");
    }
    T value;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
}

template <typename T>
void store(std::vector<uint8_t>& bytes, size_t offset, T value) {
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset) {
        throw std::runtime_error("test fixture write outside file");
    }
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
}

std::vector<uint8_t> readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) {
        throw std::runtime_error("cannot open test file for reading: " + path);
    }
    const auto end = in.tellg();
    if (end < 0) {
        throw std::runtime_error("cannot determine test file size: " + path);
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(end));
    in.seekg(0);
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!in && !bytes.empty()) {
        throw std::runtime_error("short read of test file: " + path);
    }
    return bytes;
}

void writeFile(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("cannot open test file for writing: " + path);
    }
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!out) {
        throw std::runtime_error("short write of test file: " + path);
    }
}

void flipByteDurably(const std::string& path, uint64_t offset) {
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        throw std::runtime_error("cannot open test file for mutation: " + path);
    }
    uint8_t byte = 0;
    if (::pread(fd, &byte, 1, static_cast<off_t>(offset)) != 1) {
        ::close(fd);
        throw std::runtime_error("cannot read test mutation byte");
    }
    byte ^= 0x5a;
    if (::pwrite(fd, &byte, 1, static_cast<off_t>(offset)) != 1 || ::fsync(fd) != 0) {
        ::close(fd);
        throw std::runtime_error("cannot persist test mutation byte");
    }
    if (::close(fd) != 0) {
        throw std::runtime_error("cannot close mutated test file");
    }
}

struct Layout {
    size_t footerOffset;
    uint64_t indexOffset;
};

Layout layoutOf(const std::vector<uint8_t>& bytes) {
    if (bytes.size() < 5 + TSM_FOOTER_SIZE) {
        throw std::runtime_error("test fixture is not a complete exact-v1 TSM");
    }
    const size_t footerOffset = bytes.size() - TSM_FOOTER_SIZE;
    const uint64_t indexOffset = load<uint64_t>(bytes, footerOffset + sizeof(uint32_t) + sizeof(uint64_t));
    if (indexOffset > footerOffset) {
        throw std::runtime_error("test fixture has invalid index offset");
    }
    return {footerOffset, indexOffset};
}

void recomputeSingleEntryIntegrity(std::vector<uint8_t>& bytes) {
    const auto layout = layoutOf(bytes);
    if (layout.footerOffset - layout.indexOffset < TSM_INDEX_ENTRY_HEADER_SIZE + sizeof(uint32_t)) {
        throw std::runtime_error("test fixture does not contain one complete index entry");
    }

    // These tests write exactly one non-string series, so its entry consumes the
    // complete index and the final four index bytes are its entry CRC.
    const size_t entryCrcOffset = layout.footerOffset - sizeof(uint32_t);
    const uint32_t entryCrc =
        CRC32::compute(bytes.data() + layout.indexOffset, entryCrcOffset - static_cast<size_t>(layout.indexOffset));
    store(bytes, entryCrcOffset, entryCrc);

    const uint32_t indexCrc = CRC32::compute(bytes.data() + layout.indexOffset,
                                             layout.footerOffset - static_cast<size_t>(layout.indexOffset));
    store(bytes, layout.footerOffset, indexCrc);
    const uint32_t footerCrc = CRC32::compute(bytes.data() + layout.footerOffset, TSM_FOOTER_BODY_SIZE);
    store(bytes, layout.footerOffset + TSM_FOOTER_BODY_SIZE, footerCrc);
}

SeriesId128 writeFloatFile(const std::string& path) {
    const auto series = SeriesId128::fromSeriesKey("integrity.value");
    TSMWriter writer(path);
    writer.writeSeries(TSMValueType::Float, series, std::vector<uint64_t>{100, 200, 300, 400},
                       std::vector<double>{1.0, 2.0, 3.0, 4.0}, std::vector<uint64_t>{7, 8, 9, 10});
    writer.writeIndex();
    writer.close();
    return series;
}

seastar::future<> expectOpenFailure(const std::string& path, std::string_view expected) {
    TSM tsm(path);
    bool threw = false;
    try {
        co_await tsm.open();
    } catch (const std::exception& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
    }
    EXPECT_TRUE(threw) << "corrupt TSM opened successfully";
    if (!threw) {
        co_await tsm.close();
    }
}

SEASTAR_TEST_F(TSMIntegrityTest, EmptyExactV1FileRoundTrips) {
    const auto path = self->path("0_1.tsm");
    {
        TSMWriter writer(path);
        writer.writeIndex();
        writer.close();
    }

    EXPECT_EQ(fs::file_size(path), 5u + TSM_FOOTER_SIZE);
    TSM tsm(path);
    co_await tsm.open();
    EXPECT_EQ(tsm.getSeriesCount(), 0u);
    EXPECT_EQ(tsm.maxRevision(), 0u);
    co_await tsm.close();
}

TEST_F(TSMIntegrityTest, WriterFinalizationIsOneShot) {
    TSMWriter writer(path("0_2.tsm"));
    writer.writeIndex();
    EXPECT_THROW(writer.writeIndex(), std::logic_error);
    EXPECT_NO_THROW(writer.close());

    TSMWriter unfinished(path("0_2_unfinished.tsm"));
    EXPECT_THROW(unfinished.close(), std::logic_error);
}

SEASTAR_TEST_F(TSMIntegrityTest, FooterChecksumRejectsCorruptionAtOpen) {
    const auto path = self->path("0_3.tsm");
    writeFloatFile(path);
    flipByteDurably(path, fs::file_size(path) - 1);
    co_await expectOpenFailure(path, "footer checksum mismatch");
}

SEASTAR_TEST_F(TSMIntegrityTest, CompleteIndexChecksumRejectsCorruptionAtOpen) {
    const auto path = self->path("0_4.tsm");
    writeFloatFile(path);
    const auto bytes = readFile(path);
    const auto layout = layoutOf(bytes);
    flipByteDurably(path, layout.indexOffset);
    co_await expectOpenFailure(path, "index checksum mismatch");
}

SEASTAR_TEST_F(TSMIntegrityTest, LazyEntryRereadAuthenticatesEntry) {
    const auto path = self->path("0_5.tsm");
    const auto series = writeFloatFile(path);
    const auto layout = layoutOf(readFile(path));

    TSM tsm(path);
    co_await tsm.open();  // authenticates the complete index and builds only the sparse entry
    flipByteDurably(path, layout.footerOffset - 1);  // mutate the per-entry CRC after open

    bool threw = false;
    try {
        (void)co_await tsm.getFullIndexEntry(series);
    } catch (const std::exception& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find("entry checksum mismatch"), std::string::npos) << e.what();
    }
    EXPECT_TRUE(threw) << "lazy index reread trusted bytes changed after open";
    co_await tsm.close();
}

SEASTAR_TEST_F(TSMIntegrityTest, DataChecksumCoversRawBatchedAndCompactionReads) {
    const auto path = self->path("0_6.tsm");
    const auto series = writeFloatFile(path);

    TSM tsm(path);
    co_await tsm.open();
    auto* entry = co_await tsm.getFullIndexEntry(series);
    if (entry == nullptr || entry->indexBlocks.size() != 1u) {
        ADD_FAILURE() << "fixture should contain exactly one authenticated block";
        co_await tsm.close();
        co_return;
    }
    const TSMIndexBlock block = entry->indexBlocks.front();
    flipByteDurably(path, block.offset + block.size / 2);

    bool rawFailed = false;
    try {
        TSMResult<double> result(0);
        co_await tsm.readSeries(series, 0, UINT64_MAX, result);
    } catch (const std::exception& e) {
        rawFailed = std::string(e.what()).find("block checksum mismatch") != std::string::npos;
    }
    EXPECT_TRUE(rawFailed) << "ordinary reads did not authenticate block bytes";

    bool batchedFailed = false;
    try {
        TSMResult<double> result(0);
        co_await tsm.readSeriesBatched(series, 0, UINT64_MAX, result);
    } catch (const std::exception& e) {
        batchedFailed = std::string(e.what()).find("block checksum mismatch") != std::string::npos;
    }
    EXPECT_TRUE(batchedFailed) << "batched reads did not authenticate block bytes";

    bool compressedFailed = false;
    try {
        (void)co_await tsm.readCompressedBlock(block);
    } catch (const std::exception& e) {
        compressedFailed = std::string(e.what()).find("block checksum mismatch") != std::string::npos;
    }
    EXPECT_TRUE(compressedFailed) << "zero-copy compaction reads did not authenticate block bytes";
    co_await tsm.close();
}

SEASTAR_TEST_F(TSMIntegrityTest, AuthenticatedButOutOfRangeBlockMetadataFailsOpen) {
    const auto path = self->path("0_7.tsm");
    writeFloatFile(path);
    auto bytes = readFile(path);
    const auto layout = layoutOf(bytes);

    // First entry, first block: block offset is 16-byte series ID + type +
    // blockCount + minTime + maxTime bytes into the index.
    const size_t blockOffsetField = static_cast<size_t>(layout.indexOffset) + TSM_INDEX_ENTRY_HEADER_SIZE + 16;
    store<uint64_t>(bytes, blockOffsetField, layout.indexOffset);  // points into the authenticated index
    recomputeSingleEntryIntegrity(bytes);
    writeFile(path, bytes);

    co_await expectOpenFailure(path, "block range crosses the data region");
}

SEASTAR_TEST_F(TSMIntegrityTest, AuthenticatedFooterRevisionMustMatchIndex) {
    const auto path = self->path("0_8.tsm");
    writeFloatFile(path);
    auto bytes = readFile(path);
    const auto layout = layoutOf(bytes);

    store<uint64_t>(bytes, layout.footerOffset + sizeof(uint32_t), 11u);
    const uint32_t footerCrc = CRC32::compute(bytes.data() + layout.footerOffset, TSM_FOOTER_BODY_SIZE);
    store(bytes, layout.footerOffset + TSM_FOOTER_BODY_SIZE, footerCrc);
    writeFile(path, bytes);

    co_await expectOpenFailure(path, "maximum revision disagrees");
}

}  // namespace

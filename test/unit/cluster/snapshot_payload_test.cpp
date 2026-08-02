#include "../../../lib/cluster/data/snapshot_payload.hpp"

#include "../../../lib/storage/series_catalog.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <unistd.h>

using namespace timestar::data;
namespace fs = std::filesystem;

namespace {
SnapshotPayload payload() {
    SnapshotPayload p;
    p.manifest.vshard = timestar::VShardId{5};
    p.manifest.snapshotRevision = 100;
    p.manifest.verificationHash = std::string(32, 'a');
    p.manifest.dataExtents = {
        timestar::VShardExtent{5, timestar::RevisionRange{1, 50, false}},
        timestar::VShardExtent{6, timestar::RevisionRange{51, 99, false}},
    };

    timestar::SeriesCatalog catalog;
    timestar::CatalogEntry entry;
    entry.measurement = "cpu";
    entry.tags = {{"host", "a"}};
    entry.field = "usage";
    if (!catalog.apply({SeriesId128::fromSeriesKey("cpu,host=a usage"), std::move(entry)}))
        throw std::logic_error("failed to build test catalog");
    p.catalog = catalog.snapshot();
    p.manifest.catalogHash = timestar::SeriesCatalog::snapshotHash(p.catalog);
    p.deleteReceiptsRetiredBeforeMs = 1'000;
    p.deleteReceiptsRetiredAtIndex = 10;
    p.deleteReceipts = {{SeriesId128::fromHex("11111111111111111111111111111111"), 20, 0x1234, 2'000},
                        {SeriesId128::fromHex("22222222222222222222222222222222"), 30, 0x5678, 3'000}};
    p.retentionCutoff = RetentionCutoffSnapshotState{7, "memory", 3, 60, 50};
    p.files = {{"5_0.tsm", std::string("\0\1binary", 8)}, {"5_1.tsm", "second-object"}};
    return p;
}

fs::path scratch(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    return fs::temp_directory_path() /
           ("timestar_tsp1_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
}

std::string readFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}
}  // namespace

TEST(SnapshotPayloadV1, RoundTripsCompleteState) {
    const auto original = payload();
    const auto encoded = encodeSnapshotPayload(original);
    ASSERT_EQ(encoded.substr(0, 4), "TSP1");
    const auto decoded = decodeSnapshotPayload(encoded);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->manifest, original.manifest);
    EXPECT_EQ(decoded->catalog, original.catalog);
    EXPECT_EQ(decoded->deleteReceipts, original.deleteReceipts);
    EXPECT_EQ(decoded->deleteReceiptsRetiredBeforeMs, original.deleteReceiptsRetiredBeforeMs);
    EXPECT_EQ(decoded->deleteReceiptsRetiredAtIndex, original.deleteReceiptsRetiredAtIndex);
    EXPECT_EQ(decoded->retentionCutoff, original.retentionCutoff);
    ASSERT_EQ(decoded->files.size(), 2u);
    EXPECT_EQ(decoded->files[0].bytes, original.files[0].bytes);
}

TEST(SnapshotPayloadV1, LightweightReceiptDecodeMatchesFullDecode) {
    const auto original = payload();
    const auto encoded = encodeSnapshotPayload(original);
    auto receipts = decodeSnapshotDeleteReceipts(encoded);
    ASSERT_TRUE(receipts);
    EXPECT_EQ(*receipts, original.deleteReceipts);
    auto state = decodeSnapshotDeleteReceiptState(encoded);
    ASSERT_TRUE(state);
    EXPECT_EQ(state->retiredBeforeMs, original.deleteReceiptsRetiredBeforeMs);
    EXPECT_EQ(state->retiredAtIndex, original.deleteReceiptsRetiredAtIndex);
    EXPECT_EQ(state->receipts, original.deleteReceipts);
    auto stateMachine = decodeSnapshotStateMachineState(encoded);
    ASSERT_TRUE(stateMachine);
    EXPECT_EQ(stateMachine->deleteReceipts.receipts, original.deleteReceipts);
    EXPECT_EQ(stateMachine->retentionCutoff, original.retentionCutoff);
}

TEST(SnapshotPayloadV1, RejectsMalformedFrames) {
    const auto encoded = encodeSnapshotPayload(payload());
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeSnapshotPayload(encoded.substr(0, n))) << n;
    auto corrupt = encoded;
    corrupt[4] ^= 1;
    EXPECT_FALSE(decodeSnapshotPayload(corrupt));
    EXPECT_FALSE(decodeSnapshotPayload(encoded + "x"));
}

TEST(SnapshotPayloadV1, RejectsInvalidCatalogAndReceiptState) {
    auto p = payload();
    p.catalog[0] ^= 1;
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    std::swap(p.deleteReceipts[0], p.deleteReceipts[1]);
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.deleteReceipts[0].issuedAtMs = p.deleteReceiptsRetiredBeforeMs;
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.deleteReceipts[0].appliedIndex = p.manifest.snapshotRevision;
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.retentionCutoff->appliedIndex = p.manifest.snapshotRevision;
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.retentionCutoff->measurement = "bad\nname";
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.retentionCutoff->sweepId = 0;
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
    p = payload();
    p.manifest.dataExtents.pop_back();
    EXPECT_THROW(encodeSnapshotPayload(p), std::invalid_argument);
}

TEST(SnapshotPayloadV1, ConsumingEncoderIsByteIdentical) {
    const auto original = payload();
    auto consumed = original;
    EXPECT_EQ(encodeSnapshotPayload(original), encodeSnapshotPayload(std::move(consumed)));
}

TEST(SnapshotPayloadV1, FileCodecIsExactV1AndStreamsBothDirections) {
    const auto dir = scratch("stream");
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        const auto original = payload();
        SnapshotPayloadFile disk;
        disk.manifest = original.manifest;
        disk.catalog = original.catalog;
        disk.deleteReceiptsRetiredBeforeMs = original.deleteReceiptsRetiredBeforeMs;
        disk.deleteReceiptsRetiredAtIndex = original.deleteReceiptsRetiredAtIndex;
        disk.deleteReceipts = original.deleteReceipts;
        disk.retentionCutoff = original.retentionCutoff;
        for (size_t i = 0; i < original.files.size(); ++i) {
            const auto source = dir / ("source_" + std::to_string(i) + ".tsm");
            std::ofstream out(source, std::ios::binary | std::ios::trunc);
            out.write(original.files[i].bytes.data(), static_cast<std::streamsize>(original.files[i].bytes.size()));
            out.close();
            disk.files.emplace_back(original.files[i].name, source, original.files[i].bytes.size());
        }

        auto encoded = encodeSnapshotPayloadFile(std::move(disk), dir / "snapshot_v1_test.bin").get();
        EXPECT_EQ(readFile(encoded.path), encodeSnapshotPayload(original))
            << "the disk-backed path must update exact TSP1 v1 in place, not introduce another protocol";

        auto decoded = decodeSnapshotPayloadFile(encoded.path, dir / "extract").get();
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->manifest, original.manifest);
        EXPECT_EQ(decoded->catalog, original.catalog);
        EXPECT_EQ(decoded->deleteReceipts, original.deleteReceipts);
        EXPECT_EQ(decoded->retentionCutoff, original.retentionCutoff);
        ASSERT_EQ(decoded->files.size(), original.files.size());
        for (size_t i = 0; i < decoded->files.size(); ++i) {
            EXPECT_EQ(decoded->files[i].name, original.files[i].name);
            EXPECT_EQ(readFile(decoded->files[i].path), original.files[i].bytes);
        }

        auto state = decodeSnapshotStateMachineStateFile(encoded.path).get();
        ASSERT_TRUE(state);
        EXPECT_EQ(state->deleteReceipts.receipts, original.deleteReceipts);
        EXPECT_EQ(state->retentionCutoff, original.retentionCutoff);

        const auto corrupt = dir / "snapshot_v1_corrupt.bin";
        fs::copy_file(encoded.path, corrupt);
        std::fstream damaged(corrupt, std::ios::binary | std::ios::in | std::ios::out);
        damaged.seekg(4);
        char byte = 0;
        damaged.read(&byte, 1);
        damaged.seekp(4);
        byte ^= 1;
        damaged.write(&byte, 1);
        damaged.close();
        EXPECT_FALSE(decodeSnapshotPayloadFile(corrupt, dir / "bad_extract").get());
    }
    fs::remove_all(dir);
}

#include "../../../lib/cluster/data/snapshot_payload.hpp"

#include "../../../lib/storage/series_catalog.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;

namespace {
SnapshotPayload payload() {
    SnapshotPayload p;
    p.manifest.vshard = timestar::VShardId{5};
    p.manifest.snapshotRevision = 100;
    p.manifest.verificationHash = std::string(32, 'a');

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
    p.files = {{"5_0.tsm", std::string("\0\1binary", 8)}, {"5_0.tomb", "tombstones"}};
    return p;
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
}

TEST(SnapshotPayloadV1, ConsumingEncoderIsByteIdentical) {
    const auto original = payload();
    auto consumed = original;
    EXPECT_EQ(encodeSnapshotPayload(original), encodeSnapshotPayload(std::move(consumed)));
}

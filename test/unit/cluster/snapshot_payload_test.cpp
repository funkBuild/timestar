// Integration M3 (snapshots): the monolithic Raft InstallSnapshot payload codec --
// a VShardSnapshotManifest plus its data files, self-contained so a lagging replica
// installs the whole VShard state. Round-trips, and rejects truncation / a flipped
// checksum / a corrupt inner manifest.
#include "../../../lib/cluster/data/snapshot_payload.hpp"

#include "../../../lib/cluster/data/journal_format.hpp"
#include "../../../lib/storage/series_catalog.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::VShardId;
using timestar::VShardSnapshotManifest;

namespace {
VShardSnapshotManifest validManifest() {
    VShardSnapshotManifest m;
    m.vshard = VShardId{5};
    m.snapshotRevision = 100;
    m.verificationHash = std::string(32, 'a');  // 32 hex chars
    m.catalogHash = std::string(32, 'b');
    // empty extents/tombstones -> trivially valid
    EXPECT_TRUE(m.valid());
    return m;
}

void addValidCatalog(SnapshotPayload& payload) {
    timestar::SeriesCatalog catalog;
    timestar::CatalogEntry entry;
    entry.measurement = "cpu";
    entry.tags = {{"host", "a"}};
    entry.field = "usage";
    entry.valueType = TSMValueType::Float;
    catalog.apply(timestar::CatalogRecord{SeriesId128::fromSeriesKey("catalog-test"), std::move(entry)});
    payload.catalog = catalog.snapshot();
    payload.manifest.catalogHash = timestar::SeriesCatalog::snapshotHash(payload.catalog);
}
}  // namespace

TEST(SnapshotPayloadCodec, PayloadVersionsNameTheirCommittedFormatRequirements) {
    SnapshotPayload payload;
    payload.manifest = validManifest();
    EXPECT_EQ(requiredClusterFormatVersion(payload), 1u);

    addValidCatalog(payload);
    EXPECT_EQ(requiredClusterFormatVersion(payload), kSnapshotV2ActivationVersion);

    payload.deleteReceipts = {
        {SeriesId128::fromHex("11111111111111111111111111111111"), 12, 0x1234},
    };
    EXPECT_EQ(requiredClusterFormatVersion(payload), kDeleteReceiptActivationVersion);

    payload.deleteReceiptsRetiredBeforeMs = 1'000;
    payload.deleteReceiptsRetiredAtIndex = 50;
    EXPECT_EQ(requiredClusterFormatVersion(payload), kBoundedDeleteReceiptActivationVersion);
}

TEST(SnapshotPayloadCodec, RoundTripsManifestAndFiles) {
    SnapshotPayload p;
    p.manifest = validManifest();
    p.files = {{"0_0.tsm", std::string("\x00\x01\x02binary tsm bytes", 20)}, {"1_0.tsm", "another file's contents"}};

    auto back = decodeSnapshotPayload(encodeSnapshotPayload(p));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->manifest, p.manifest);
    ASSERT_EQ(back->files.size(), 2u);
    EXPECT_EQ(back->files[0].name, "0_0.tsm");
    EXPECT_EQ(back->files[0].bytes, p.files[0].bytes);  // binary bytes preserved
    EXPECT_EQ(back->files[1].bytes, "another file's contents");
}

TEST(SnapshotPayloadCodec, Version2AuthenticatesAndRoundTripsCatalog) {
    SnapshotPayload p;
    p.manifest = validManifest();
    addValidCatalog(p);
    p.files = {{"9_1_d1.tsm", "bytes"}};

    auto encoded = encodeSnapshotPayload(p);
    auto back = decodeSnapshotPayload(encoded);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->catalog, p.catalog);
    EXPECT_EQ(back->manifest.catalogHash, p.manifest.catalogHash);
    ASSERT_TRUE(timestar::SeriesCatalog::loadSnapshot(std::span<const char>(back->catalog.data(), back->catalog.size()))
                    .has_value());

    // Recompute the outer checksum after corrupting a catalog byte: the inner
    // catalog hash, not merely the transport checksum, must reject it.
    const auto offset = encoded.find("cpu");
    ASSERT_NE(offset, std::string::npos);
    encoded[offset] ^= 0x01;
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i + 8 < encoded.size(); ++i) {
        h ^= static_cast<uint8_t>(encoded[i]);
        h *= 1099511628211ull;
    }
    for (int i = 0; i < 8; ++i)
        encoded[encoded.size() - 8 + i] = static_cast<char>((h >> (8 * i)) & 0xff);
    EXPECT_FALSE(decodeSnapshotPayload(encoded).has_value());
}

TEST(SnapshotPayloadCodec, Version3AuthenticatesDeleteReceiptsAndSupportsLightweightRecovery) {
    SnapshotPayload p;
    p.manifest = validManifest();
    addValidCatalog(p);
    p.deleteReceipts = {
        {SeriesId128::fromHex("11111111111111111111111111111111"), 12, 0x1234},
        {SeriesId128::fromHex("22222222222222222222222222222222"), 99, 0x5678},
    };
    p.files = {{"9_1_d1.tsm", std::string(4096, 'x')}};

    const std::string encoded = encodeSnapshotPayload(p);
    auto decoded = decodeSnapshotPayload(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->deleteReceipts, p.deleteReceipts);
    auto lightweight = decodeSnapshotDeleteReceipts(encoded);
    ASSERT_TRUE(lightweight.has_value());
    EXPECT_EQ(*lightweight, p.deleteReceipts);

    auto invalid = p;
    std::swap(invalid.deleteReceipts[0], invalid.deleteReceipts[1]);
    EXPECT_THROW(encodeSnapshotPayload(invalid), std::invalid_argument);
    invalid = p;
    invalid.deleteReceipts[1].appliedIndex = invalid.manifest.snapshotRevision;
    EXPECT_THROW(encodeSnapshotPayload(invalid), std::invalid_argument);

    std::string corrupt = encoded;
    corrupt[corrupt.size() - 1] ^= 1;
    EXPECT_FALSE(decodeSnapshotDeleteReceipts(corrupt).has_value());
}

TEST(SnapshotPayloadCodec, Version4RoundTripsBoundedReceiptFloorWithoutReadingObjects) {
    SnapshotPayload p;
    p.manifest = validManifest();
    addValidCatalog(p);
    p.deleteReceiptsRetiredBeforeMs = 1'000;
    p.deleteReceiptsRetiredAtIndex = 50;
    p.deleteReceipts = {
        {SeriesId128::fromHex("11111111111111111111111111111111"), 12, 0x1234, 0},
        {SeriesId128::fromHex("22222222222222222222222222222222"), 60, 0x5678, 1'001},
        {SeriesId128::fromHex("33333333333333333333333333333333"), 99, 0x9abc, 2'000},
    };
    p.files = {{"9_1_d1.tsm", std::string(4096, 'x')}};

    const std::string encoded = encodeSnapshotPayload(p);
    auto decoded = decodeSnapshotPayload(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->deleteReceiptsRetiredBeforeMs, p.deleteReceiptsRetiredBeforeMs);
    EXPECT_EQ(decoded->deleteReceiptsRetiredAtIndex, p.deleteReceiptsRetiredAtIndex);
    EXPECT_EQ(decoded->deleteReceipts, p.deleteReceipts);

    auto lightweight = decodeSnapshotDeleteReceiptState(encoded);
    ASSERT_TRUE(lightweight.has_value());
    EXPECT_EQ(lightweight->retiredBeforeMs, p.deleteReceiptsRetiredBeforeMs);
    EXPECT_EQ(lightweight->retiredAtIndex, p.deleteReceiptsRetiredAtIndex);
    EXPECT_EQ(lightweight->receipts, p.deleteReceipts);

    auto invalid = p;
    invalid.deleteReceipts[1].issuedAtMs = invalid.deleteReceiptsRetiredBeforeMs;
    EXPECT_THROW(encodeSnapshotPayload(invalid), std::invalid_argument);
    invalid = p;
    invalid.deleteReceiptsRetiredAtIndex = invalid.manifest.snapshotRevision;
    EXPECT_THROW(encodeSnapshotPayload(invalid), std::invalid_argument);
    invalid = p;
    invalid.deleteReceiptsRetiredBeforeMs = 0;
    EXPECT_THROW(encodeSnapshotPayload(invalid), std::invalid_argument);
}

TEST(SnapshotPayloadCodec, EmptyFileListRoundTrips) {
    SnapshotPayload p;
    p.manifest = validManifest();
    auto back = decodeSnapshotPayload(encodeSnapshotPayload(p));
    ASSERT_TRUE(back.has_value());
    EXPECT_TRUE(back->files.empty());
    EXPECT_EQ(back->manifest.vshard, VShardId{5});
}

TEST(SnapshotPayloadCodec, TruncationAndCorruptionRejected) {
    SnapshotPayload p;
    p.manifest = validManifest();
    p.files = {{"f", "data"}};
    std::string full = encodeSnapshotPayload(p);
    ASSERT_TRUE(decodeSnapshotPayload(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeSnapshotPayload(full.substr(0, n)).has_value()) << "prefix " << n;
    std::string bad = full;
    bad[bad.size() - 1] ^= 0xff;  // flip FNV trailer
    EXPECT_FALSE(decodeSnapshotPayload(bad).has_value());
    // Corrupt the inner manifest blob (byte 8 is inside the manifest's own encoding):
    // its CRC must reject it, so the whole payload is rejected.
    std::string badManifest = full;
    badManifest[10] ^= 0xff;
    // re-stamp the outer FNV so only the inner manifest CRC catches it
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i + 8 < badManifest.size(); ++i) {
        h ^= static_cast<uint8_t>(badManifest[i]);
        h *= 1099511628211ull;
    }
    for (int i = 0; i < 8; ++i)
        badManifest[badManifest.size() - 8 + i] = static_cast<char>((h >> (8 * i)) & 0xff);
    EXPECT_FALSE(decodeSnapshotPayload(badManifest).has_value()) << "corrupt inner manifest must be rejected";
}

// ---------------------------------------------------------------------------
// The CONSUMING overload (debt D-32)
// ---------------------------------------------------------------------------

// The producer used to hold the payload TWICE at its peak: `encodeSnapshotPayload` took a
// const&, so `snapshotVShard`'s `std::move` was dead and every file was copied into the
// output while the input stayed fully resident. This overload must produce the same bytes
// and must actually release the input as it goes -- both halves matter, because the
// output being right is what makes it safe and the input being released is the whole
// point.
TEST(SnapshotPayloadCodec, TheConsumingOverloadIsByteIdenticalAndDrainsItsInput) {
    SnapshotPayload p;
    p.manifest = validManifest();
    p.files = {{"0_0.tsm", std::string(4096, '\x7f')},
               {"1_0.tsm", std::string("\x00\x01\x02binary tsm bytes", 20)},
               {"2_0.tsm", ""}};
    SnapshotPayload copy = p;

    const std::string byRef = encodeSnapshotPayload(p);
    const std::string byMove = encodeSnapshotPayload(std::move(copy));
    EXPECT_EQ(byRef, byMove) << "the consuming overload must not change the wire format";

    // ...and the input's file bodies are gone, which is the memory claim.
    ASSERT_EQ(copy.files.size(), 3u);
    for (const auto& f : copy.files)
        EXPECT_TRUE(f.bytes.empty()) << "file " << f.name << " was not released";
    // Names are left alone (they are tiny, and clearing them would make the drained
    // payload unloggable).
    EXPECT_EQ(copy.files[0].name, "0_0.tsm");

    // The const& source is untouched, so callers that keep their payload still can.
    EXPECT_EQ(p.files[0].bytes.size(), 4096u);

    auto back = decodeSnapshotPayload(byMove);
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->files.size(), 3u);
    EXPECT_EQ(back->files[0].bytes, std::string(4096, '\x7f'));
    EXPECT_EQ(back->files[2].bytes, "");
}

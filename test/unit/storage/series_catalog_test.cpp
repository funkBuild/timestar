#include "../../../lib/storage/series_catalog.hpp"

#include <gtest/gtest.h>

#include <span>
#include <string>

namespace {

using timestar::CatalogEntry;
using timestar::CatalogRecord;
using timestar::SeriesCatalog;
// SeriesId128 and TSMValueType live in the global namespace.

SeriesId128 mkId(uint8_t seed) {
    return SeriesId128::fromBytes(std::string(16, static_cast<char>(seed)));
}

CatalogEntry entry(std::string measurement, std::vector<std::pair<std::string, std::string>> tags, std::string field,
                   TSMValueType type) {
    CatalogEntry e;
    e.measurement = std::move(measurement);
    e.tags = std::move(tags);
    e.field = std::move(field);
    e.valueType = type;
    return e;
}

std::optional<CatalogEntry> decodeEntry(const std::string& bytes) {
    timestar::codec::Reader r(std::span<const char>(bytes.data(), bytes.size()));
    auto e = CatalogEntry::decode(r);
    if (!e || !r.exhausted())
        return std::nullopt;
    return e;
}

TEST(SeriesCatalogTest, EntryRoundTripsAcrossTypesAndTags) {
    for (auto type : {TSMValueType::Float, TSMValueType::Boolean, TSMValueType::String, TSMValueType::Integer}) {
        auto e = entry("weather", {{"host", "a"}, {"loc", "us-west"}}, "temperature", type);
        std::string encoded;
        e.encodeInto(encoded);
        const auto decoded = decodeEntry(encoded);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(*decoded, e);
    }
    // Empty tags, empty measurement/field.
    auto bare = entry("", {}, "", TSMValueType::Float);
    std::string enc;
    bare.encodeInto(enc);
    EXPECT_EQ(decodeEntry(enc), bare);
}

TEST(SeriesCatalogTest, CanonicalizeSortsTags) {
    auto e = entry("m", {{"z", "1"}, {"a", "2"}, {"a", "1"}}, "f", TSMValueType::Float);
    e.canonicalize();
    ASSERT_EQ(e.tags.size(), 3u);
    EXPECT_EQ(e.tags[0], (std::pair<std::string, std::string>{"a", "1"}));
    EXPECT_EQ(e.tags[1], (std::pair<std::string, std::string>{"a", "2"}));
    EXPECT_EQ(e.tags[2], (std::pair<std::string, std::string>{"z", "1"}));
}

TEST(SeriesCatalogTest, DecodeRejectsNonCanonicalDuplicateAndBadType) {
    // Non-canonical tag order.
    auto nonCanon = entry("m", {{"z", "1"}, {"a", "1"}}, "f", TSMValueType::Float);
    std::string enc;
    nonCanon.encodeInto(enc);
    EXPECT_FALSE(decodeEntry(enc).has_value());

    // Duplicate tag key.
    auto dup = entry("m", {{"a", "1"}, {"a", "1"}}, "f", TSMValueType::Float);
    std::string encDup;
    dup.encodeInto(encDup);
    EXPECT_FALSE(decodeEntry(encDup).has_value());

    // Same key with different values is still a duplicate tag key. Pair-wise
    // sorting alone used to accept this malformed identity.
    auto dupKey = entry("m", {{"a", "1"}, {"a", "2"}}, "f", TSMValueType::Float);
    std::string encDupKey;
    dupKey.encodeInto(encDupKey);
    EXPECT_FALSE(decodeEntry(encDupKey).has_value());

    // Out-of-range value type: encode a valid entry then corrupt the last byte.
    auto ok = entry("m", {{"a", "1"}}, "f", TSMValueType::Float);
    std::string encBad;
    ok.encodeInto(encBad);
    encBad.back() = static_cast<char>(9);  // > Integer(3)
    EXPECT_FALSE(decodeEntry(encBad).has_value());

    // Truncated input.
    std::string encTrunc;
    ok.encodeInto(encTrunc);
    encTrunc.resize(encTrunc.size() - 1);
    EXPECT_FALSE(decodeEntry(encTrunc).has_value());
}

TEST(SeriesCatalogTest, RecordRoundTrips) {
    CatalogRecord rec{mkId(7), entry("cpu", {{"core", "0"}}, "usage", TSMValueType::Float)};
    const auto encoded = rec.encode();
    size_t consumed = 0;
    const auto decoded = CatalogRecord::decode(std::span<const char>(encoded.data(), encoded.size()), consumed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded->seriesId, rec.seriesId);
    EXPECT_EQ(decoded->entry, rec.entry);
}

TEST(SeriesCatalogTest, GetOrCreateReportsFirstSightAndCanonicalizes) {
    SeriesCatalog cat;
    auto first = cat.getOrCreate(mkId(1), entry("m", {{"z", "1"}, {"a", "2"}}, "f", TSMValueType::Integer));
    EXPECT_TRUE(first.created);
    ASSERT_NE(first.entry, nullptr);
    // Stored entry is canonicalized.
    ASSERT_EQ(first.entry->tags.size(), 2u);
    EXPECT_EQ(first.entry->tags[0].first, "a");

    // Second call for the same id returns the existing entry, not created.
    auto second = cat.getOrCreate(mkId(1), entry("other", {}, "x", TSMValueType::Float));
    EXPECT_FALSE(second.created);
    EXPECT_EQ(second.entry->measurement, "m");  // original wins
    EXPECT_EQ(cat.size(), 1u);
    EXPECT_NE(cat.find(mkId(1)), nullptr);
    EXPECT_EQ(cat.find(mkId(2)), nullptr);
}

TEST(SeriesCatalogTest, ApplyIsIdempotentAndRejectsConflicts) {
    SeriesCatalog cat;
    CatalogRecord rec{mkId(5), entry("m", {{"a", "1"}}, "f", TSMValueType::Float)};
    EXPECT_TRUE(cat.apply(rec));
    EXPECT_TRUE(cat.apply(rec));  // identical replay is idempotent

    CatalogRecord conflict{mkId(5), entry("m", {{"a", "1"}}, "f", TSMValueType::Integer)};  // different type, same id
    EXPECT_FALSE(cat.apply(conflict));
    EXPECT_EQ(cat.find(mkId(5))->valueType, TSMValueType::Float);  // first definition preserved
}

TEST(SeriesCatalogTest, SnapshotRoundTripsAndIsInsertionOrderIndependent) {
    SeriesCatalog a;
    a.getOrCreate(mkId(1), entry("m", {{"a", "1"}}, "f", TSMValueType::Float));
    a.getOrCreate(mkId(2), entry("m", {{"b", "2"}}, "g", TSMValueType::Boolean));
    a.getOrCreate(mkId(3), entry("n", {}, "h", TSMValueType::String));

    SeriesCatalog b;  // same series, reverse insertion order
    b.getOrCreate(mkId(3), entry("n", {}, "h", TSMValueType::String));
    b.getOrCreate(mkId(2), entry("m", {{"b", "2"}}, "g", TSMValueType::Boolean));
    b.getOrCreate(mkId(1), entry("m", {{"a", "1"}}, "f", TSMValueType::Float));

    const std::string snapA = a.snapshot();
    // Deterministic: equal catalogs snapshot byte-identically regardless of order.
    EXPECT_EQ(snapA, b.snapshot());
    EXPECT_EQ(SeriesCatalog::snapshotHash(snapA), SeriesCatalog::snapshotHash(b.snapshot()));
    EXPECT_EQ(SeriesCatalog::snapshotHash(snapA).size(), 32u);

    const auto loaded = SeriesCatalog::loadSnapshot(std::span<const char>(snapA.data(), snapA.size()));
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->size(), 3u);
    EXPECT_EQ(loaded->records().size(), 3u);
    ASSERT_NE(loaded->find(mkId(2)), nullptr);
    EXPECT_EQ(loaded->find(mkId(2))->valueType, TSMValueType::Boolean);
    EXPECT_EQ(loaded->snapshot(), snapA);  // re-snapshot is stable
}

TEST(SeriesCatalogTest, LoadSnapshotRejectsCorruption) {
    SeriesCatalog cat;
    cat.getOrCreate(mkId(1), entry("m", {{"a", "1"}}, "f", TSMValueType::Float));
    const std::string snap = cat.snapshot();

    // Truncated.
    EXPECT_FALSE(SeriesCatalog::loadSnapshot(std::span<const char>(snap.data(), snap.size() - 1)).has_value());
    // Trailing garbage after the declared count.
    std::string extra = snap + "junk";
    EXPECT_FALSE(SeriesCatalog::loadSnapshot(std::span<const char>(extra.data(), extra.size())).has_value());
    // A count larger than the records present.
    std::string badCount = snap;
    badCount[0] = static_cast<char>(9);  // claim 9 records
    EXPECT_FALSE(SeriesCatalog::loadSnapshot(std::span<const char>(badCount.data(), badCount.size())).has_value());

    // Snapshots are canonical map encodings: an identical repeated ID is not
    // accepted merely because replaying that record would be idempotent.
    CatalogRecord repeated{mkId(1), entry("m", {{"a", "1"}}, "f", TSMValueType::Float)};
    std::string duplicate;
    timestar::codec::putU32(duplicate, 2);
    repeated.encodeInto(duplicate);
    repeated.encodeInto(duplicate);
    EXPECT_FALSE(SeriesCatalog::loadSnapshot(std::span<const char>(duplicate.data(), duplicate.size())).has_value());
}

}  // namespace

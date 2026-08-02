#include "../../../lib/cluster/data/write_record.hpp"

#include <gtest/gtest.h>
#include <limits>

using namespace timestar::data;

namespace {
WriteSeries series(std::string key, TSMValueType type) {
    WriteSeries s;
    s.seriesKey = std::move(key);
    s.type = type;
    s.timestamps = {10, 20};
    return s;
}

WriteBatch allTypes() {
    WriteBatch b;
    b.schemaVersion = 7;
    auto f = series("cpu,host=h1 load", TSMValueType::Float);
    f.values = std::vector<double>{1.5, -2.25};
    auto i = series("cpu,host=h1 count", TSMValueType::Integer);
    i.values = std::vector<int64_t>{9007199254740993LL, -9007199254740995LL};
    i.revisions = {5, 6};
    auto bo = series("cpu,host=h1 up", TSMValueType::Boolean);
    bo.values = std::vector<bool>{true, false};
    auto str = series("cpu,host=h1 message", TSMValueType::String);
    str.values = std::vector<std::string>{"hello", "UTF-8: ☃"};
    b.series = {std::move(f), std::move(i), std::move(bo), std::move(str)};
    return b;
}
}  // namespace

TEST(WriteRecordV1, RoundTripsEveryValueTypeAndRevision) {
    auto decoded = decodeWriteBatch(encodeWriteBatch(allTypes()));
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->series.size(), 4u);
    EXPECT_EQ(decoded->schemaVersion, 7u);
    EXPECT_EQ(std::get<0>(decoded->series[0].values), (std::vector<double>{1.5, -2.25}));
    EXPECT_EQ(std::get<1>(decoded->series[1].values),
              (std::vector<int64_t>{9007199254740993LL, -9007199254740995LL}));
    EXPECT_EQ(decoded->series[1].revisions, (std::vector<uint64_t>{5, 6}));
    EXPECT_EQ(std::get<2>(decoded->series[2].values), (std::vector<bool>{true, false}));
    EXPECT_EQ(std::get<3>(decoded->series[3].values), (std::vector<std::string>{"hello", "UTF-8: ☃"}));
}

TEST(WriteRecordV1, IsSelfIdentifyingAndRejectsOtherVersions) {
    const std::string encoded = encodeWriteBatch(allTypes(), kWriteBatchFormatV1);
    ASSERT_GE(encoded.size(), 4u);
    EXPECT_EQ(encoded.substr(0, 4), "TSW1");
    EXPECT_THROW(encodeWriteBatch(allTypes(), 0), std::invalid_argument);
    EXPECT_THROW(encodeWriteBatch(allTypes(), 2), std::invalid_argument);
}

TEST(WriteRecordV1, RejectsTruncationCorruptionAndTrailingBytes) {
    const std::string encoded = encodeWriteBatch(allTypes());
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeWriteBatch(encoded.substr(0, n))) << n;

    auto badMagic = encoded;
    badMagic[0] ^= 1;
    EXPECT_FALSE(decodeWriteBatch(badMagic));
    auto badBody = encoded;
    badBody[badBody.size() / 2] ^= 1;
    EXPECT_FALSE(decodeWriteBatch(badBody));
    EXPECT_FALSE(decodeWriteBatch(encoded + "x"));
}

TEST(WriteRecordV1, RejectsInconsistentSeriesBeforeEncoding) {
    auto bad = series("cpu f", TSMValueType::Float);
    bad.values = std::vector<double>{1.0};
    EXPECT_FALSE(bad.consistent());
    EXPECT_THROW(encodeWriteBatch(WriteBatch{{bad}, 0}), std::invalid_argument);

    bad.timestamps = {10};
    bad.type = TSMValueType::Integer;
    EXPECT_FALSE(bad.consistent());
    EXPECT_THROW(encodeWriteBatch(WriteBatch{{bad}, 0}), std::invalid_argument);
}

TEST(WriteRecordV1, BorrowedVShardViewMatchesMergedEncoding) {
    auto groups = splitByVShard(allTypes());
    auto view = viewOf(groups);
    const auto direct = encodeWriteBatch(view, kWriteBatchFormatV1);
    const auto merged = encodeWriteBatch(mergeVShardBatches(std::move(groups)));
    EXPECT_EQ(direct, merged);
}

TEST(WriteRecordV1, SizeBoundCoversEncoding) {
    const auto batch = allTypes();
    EXPECT_EQ(encodedWriteBatchBytes(batch), encodeWriteBatch(batch).size());
}

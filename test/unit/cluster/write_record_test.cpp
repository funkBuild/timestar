// Integration F.1: the lossless inter-node write command codec. Round-trips all
// four value types (incl. strings + int64 above 2^53 + revisions) and rejects any
// truncated / checksum-broken / type-inconsistent frame.
#include "../../../lib/cluster/data/write_record.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;  // brings in TSMValueType (re-exported here)

namespace {
WriteSeries floatSeries() {
    WriteSeries s;
    s.seriesKey = "cpu,host=h1 load";
    s.type = TSMValueType::Float;
    s.timestamps = {10, 20, 30};
    s.values = std::vector<double>{1.5, -2.25, 3.0};
    return s;
}
}  // namespace

TEST(WriteRecordCodec, RoundTripsAllFourTypes) {
    WriteBatch b;
    b.schemaVersion = 7;
    b.series.push_back(floatSeries());
    // int64 above 2^53 (would lose precision through double) with revisions.
    WriteSeries i;
    i.seriesKey = "m,host=h2 bytes";
    i.type = TSMValueType::Integer;
    i.timestamps = {100, 200};
    i.values = std::vector<int64_t>{9007199254740993LL, -9007199254740995LL};
    i.revisions = {5, 6};
    b.series.push_back(i);
    // strings (unrepresentable in DataPoint).
    WriteSeries s;
    s.seriesKey = "log,host=h3 msg";
    s.type = TSMValueType::String;
    s.timestamps = {1};
    s.values = std::vector<std::string>{"a UTF-8 string, with commas and spaces"};
    b.series.push_back(s);
    // booleans.
    WriteSeries bo;
    bo.seriesKey = "up,host=h4 state";
    bo.type = TSMValueType::Boolean;
    bo.timestamps = {1, 2, 3};
    bo.values = std::vector<bool>{true, false, true};
    b.series.push_back(bo);

    auto back = decodeWriteBatch(encodeWriteBatch(b));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->schemaVersion, 7u);
    ASSERT_EQ(back->series.size(), 4u);

    EXPECT_EQ(back->series[0].seriesKey, "cpu,host=h1 load");
    EXPECT_EQ(std::get<std::vector<double>>(back->series[0].values)[1], -2.25);

    EXPECT_EQ(back->series[1].type, TSMValueType::Integer);
    EXPECT_EQ(std::get<std::vector<int64_t>>(back->series[1].values)[0], 9007199254740993LL);
    EXPECT_EQ(back->series[1].revisions, (std::vector<uint64_t>{5, 6}));

    EXPECT_EQ(std::get<std::vector<std::string>>(back->series[2].values)[0],
              "a UTF-8 string, with commas and spaces");

    auto& bv = std::get<std::vector<bool>>(back->series[3].values);
    EXPECT_EQ(bv.size(), 3u);
    EXPECT_TRUE(bv[0]);
    EXPECT_FALSE(bv[1]);
}

TEST(WriteRecordCodec, EmptyBatchRoundTrips) {
    WriteBatch b;
    auto back = decodeWriteBatch(encodeWriteBatch(b));
    ASSERT_TRUE(back.has_value());
    EXPECT_TRUE(back->series.empty());
}

TEST(WriteRecordCodec, TruncationAndCorruptionRejected) {
    WriteBatch b;
    b.series.push_back(floatSeries());
    std::string full = encodeWriteBatch(b);
    ASSERT_TRUE(decodeWriteBatch(full).has_value());
    // Every truncated prefix is rejected (checksum/structure).
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeWriteBatch(full.substr(0, n)).has_value()) << "prefix " << n;
    // A flipped body byte fails the checksum.
    std::string flipped = full;
    flipped[0] ^= 0xff;
    EXPECT_FALSE(decodeWriteBatch(flipped).has_value());
    // A flipped checksum byte fails.
    std::string badsum = full;
    badsum[badsum.size() - 1] ^= 0xff;
    EXPECT_FALSE(decodeWriteBatch(badsum).has_value());
    // Bogus series count with no bytes behind it.
    std::string bogus(12, '\0');
    bogus[8] = static_cast<char>(0xff);  // numSeries high
    bogus[9] = static_cast<char>(0xff);
    EXPECT_FALSE(decodeWriteBatch(bogus).has_value());
}

TEST(WriteRecordCodec, ConsistencyEnforced) {
    // A value column whose size disagrees with timestamps is inconsistent and must
    // not be produced by the decoder (checked here directly).
    WriteSeries s = floatSeries();
    s.timestamps = {1, 2};                        // 2 timestamps
    s.values = std::vector<double>{1.0, 2.0, 3.0};  // 3 values
    EXPECT_FALSE(s.consistent());
    // Revisions of the wrong length are inconsistent.
    WriteSeries r = floatSeries();
    r.revisions = {1};  // 1 revision for 3 points
    EXPECT_FALSE(r.consistent());
    // A well-formed one is consistent.
    EXPECT_TRUE(floatSeries().consistent());
}

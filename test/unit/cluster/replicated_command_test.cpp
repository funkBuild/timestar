// Integration M3 (foundation): the ENRICHED replicated command
// variant<WriteBatch, DeleteRangeKey, RetentionCutoffCmd> that replaces the lossy
// DataPoint-based DataCommand -- re-opens the Phase 5 codec gate with WriteBatch.
#include "../../../lib/cluster/data/replicated_command.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::buildSeriesKey;

namespace {
WriteSeries floatSeries() {
    WriteSeries s;
    s.seriesKey = buildSeriesKey("cpu", {{"host", "h1"}}, "v");
    s.type = TSMValueType::Float;
    s.timestamps = {10, 20};
    s.values = std::vector<double>{1.5, 2.5};
    s.revisions = {};  // stamped at apply time
    return s;
}
}  // namespace

TEST(ReplicatedCommandCodec, WriteBatchArmRoundTrips) {
    WriteBatch b;
    b.series = {floatSeries()};
    b.schemaVersion = 7;
    ReplicatedCommand cmd = b;

    auto back = decodeReplicatedCommand(encodeReplicatedCommand(cmd));
    ASSERT_TRUE(back.has_value());
    ASSERT_TRUE(std::holds_alternative<WriteBatch>(*back));
    const auto& wb = std::get<WriteBatch>(*back);
    ASSERT_EQ(wb.series.size(), 1u);
    EXPECT_EQ(wb.series[0].seriesKey, buildSeriesKey("cpu", {{"host", "h1"}}, "v"));
    EXPECT_EQ(std::get<std::vector<double>>(wb.series[0].values), (std::vector<double>{1.5, 2.5}));
    EXPECT_EQ(wb.schemaVersion, 7u);
}

TEST(ReplicatedCommandCodec, DeleteAndRetentionArmsRoundTrip) {
    DeleteRangeKey d{buildSeriesKey("cpu", {{"host", "h1"}}, "v"), 100, 200};
    auto db = decodeReplicatedCommand(encodeReplicatedCommand(ReplicatedCommand{d}));
    ASSERT_TRUE(db.has_value());
    ASSERT_TRUE(std::holds_alternative<DeleteRangeKey>(*db));
    EXPECT_EQ(std::get<DeleteRangeKey>(*db).seriesKey, d.seriesKey);
    EXPECT_EQ(std::get<DeleteRangeKey>(*db).endTime, 200u);

    RetentionCutoffCmd rc{123456};
    auto rb = decodeReplicatedCommand(encodeReplicatedCommand(ReplicatedCommand{rc}));
    ASSERT_TRUE(rb.has_value());
    ASSERT_TRUE(std::holds_alternative<RetentionCutoffCmd>(*rb));
    EXPECT_EQ(std::get<RetentionCutoffCmd>(*rb).cutoffTime, 123456u);
}

TEST(ReplicatedCommandCodec, TruncationAndCorruptionRejected) {
    WriteBatch b;
    b.series = {floatSeries()};
    std::string full = encodeReplicatedCommand(ReplicatedCommand{b});
    ASSERT_TRUE(decodeReplicatedCommand(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeReplicatedCommand(full.substr(0, n)).has_value()) << "prefix " << n;
    std::string bad = full;
    bad[bad.size() - 1] ^= 0xff;  // flip checksum
    EXPECT_FALSE(decodeReplicatedCommand(bad).has_value());
    // Unknown kind tag.
    std::string unk = encodeReplicatedCommand(ReplicatedCommand{RetentionCutoffCmd{1}});
    unk[0] = 0x7f;  // invalid kind (checksum now wrong too -> rejected either way)
    EXPECT_FALSE(decodeReplicatedCommand(unk).has_value());
}

#include "../../../lib/cluster/data/replicated_command.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;

namespace {
WriteBatch writeBatch() {
    WriteSeries s;
    s.seriesKey = "cpu,host=h1 value";
    s.type = TSMValueType::Integer;
    s.timestamps = {10, 20};
    s.values = std::vector<int64_t>{1, 2};
    return WriteBatch{{std::move(s)}, 4};
}

DeleteRangeBatch deleteBatch() {
    return DeleteRangeBatch{
        {{"cpu,host=a value", 10, 20}, {"cpu,host=b value", 30, 40}},
        SeriesId128::fromSeriesKey("operation-1"),
        1'700'000'000'000};
}
}  // namespace

TEST(ReplicatedCommandV1, RoundTripsEveryCommand) {
    const std::vector<ReplicatedCommand> commands = {
        writeBatch(), deleteBatch(), RetentionCutoffCmd{1234}};
    for (const auto& command : commands) {
        const auto encoded = encodeReplicatedCommand(command);
        ASSERT_EQ(encoded.substr(0, 4), "TSC1");
        const auto decoded = decodeReplicatedCommand(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->index(), command.index());
    }

    auto decodedDelete = decodeReplicatedCommand(encodeReplicatedCommand(deleteBatch()));
    ASSERT_TRUE(decodedDelete);
    EXPECT_EQ(std::get<DeleteRangeBatch>(*decodedDelete).targets, deleteBatch().targets);
    EXPECT_EQ(std::get<DeleteRangeBatch>(*decodedDelete).issuedAtMs, deleteBatch().issuedAtMs);
}

TEST(ReplicatedCommandV1, DirectWriteEncodingMatchesVariantEncoding) {
    const auto batch = writeBatch();
    EXPECT_EQ(encodeWriteCommand(batch), encodeReplicatedCommand(ReplicatedCommand{batch}));
    EXPECT_EQ(encodedWriteCommandBytes(batch), encodeWriteCommand(batch).size());
}

TEST(ReplicatedCommandV1, RejectsMalformedFrames) {
    const auto encoded = encodeReplicatedCommand(deleteBatch());
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeReplicatedCommand(encoded.substr(0, n))) << n;
    auto corrupt = encoded;
    corrupt[5] ^= 1;
    EXPECT_FALSE(decodeReplicatedCommand(corrupt));
    EXPECT_FALSE(decodeReplicatedCommand(encoded + "x"));
}

TEST(ReplicatedCommandV1, DeleteBatchMustBeCanonicalAndIdempotent) {
    auto command = deleteBatch();
    command.operationId = {};
    EXPECT_THROW(encodeReplicatedCommand(command), std::invalid_argument);
    command = deleteBatch();
    command.issuedAtMs = 0;
    EXPECT_THROW(encodeReplicatedCommand(command), std::invalid_argument);
    command = deleteBatch();
    std::swap(command.targets[0], command.targets[1]);
    EXPECT_THROW(encodeReplicatedCommand(command), std::invalid_argument);
    command = deleteBatch();
    command.targets.push_back(command.targets.back());
    EXPECT_THROW(encodeReplicatedCommand(command), std::invalid_argument);
}

TEST(ReplicatedCommandV1, DeleteHashCoversTargetsAndIssuance) {
    auto a = deleteBatch();
    auto b = a;
    EXPECT_EQ(deleteRangeCommandHash(a), deleteRangeCommandHash(b));
    ++b.issuedAtMs;
    EXPECT_NE(deleteRangeCommandHash(a), deleteRangeCommandHash(b));
    b = a;
    ++b.targets[0].endTime;
    EXPECT_NE(deleteRangeCommandHash(a), deleteRangeCommandHash(b));
}

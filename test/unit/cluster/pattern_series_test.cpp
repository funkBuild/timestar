#include "../../../lib/cluster/data/pattern_series.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;

TEST(PatternSeriesCodec, RequestRoundTripsCompleteSelectorAndRoutingFence) {
    PatternSeriesRequest request;
    request.selector.measurement = "cpu";
    request.selector.tags = {{"env", "prod"}, {"host", "a"}};
    request.selector.fields = {"usage", "idle"};
    request.vshards = {3, 7, 11};
    request.resolveVShards = {7, 11};
    request.mapEpoch = 42;
    request.maxSeries = 10'000;

    auto decoded = decodePatternSeriesRequest(encodePatternSeriesRequest(request));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->selector.measurement, request.selector.measurement);
    EXPECT_EQ(decoded->selector.tags, request.selector.tags);
    EXPECT_EQ(decoded->selector.fields, request.selector.fields);
    EXPECT_EQ(decoded->vshards, request.vshards);
    EXPECT_EQ(decoded->resolveVShards, request.resolveVShards);
    EXPECT_EQ(decoded->mapEpoch, request.mapEpoch);
    EXPECT_EQ(decoded->maxSeries, request.maxSeries);
}

TEST(PatternSeriesCodec, ResultRoundTripsKeysRedirectsAndLimitState) {
    PatternSeriesResult result;
    result.seriesKeys = {"cpu,host=a usage", "cpu,host=b idle"};
    result.redirects = {{7, 3, true}, {11, 0, false}};

    auto decoded = decodePatternSeriesResult(encodePatternSeriesResult(result));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->seriesKeys, result.seriesKeys);
    EXPECT_EQ(decoded->redirects.size(), 2u);
    EXPECT_EQ(decoded->redirects[0].vshard, 7);
    EXPECT_EQ(decoded->redirects[0].leader, 3u);
    EXPECT_TRUE(decoded->redirects[0].hosted);
    EXPECT_FALSE(decoded->redirects[1].hosted);

    PatternSeriesResult exceeded;
    exceeded.limitExceeded = true;
    decoded = decodePatternSeriesResult(encodePatternSeriesResult(exceeded));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->limitExceeded);
}

TEST(PatternSeriesCodec, CorruptionTruncationAndNonCanonicalRoutingAreRejected) {
    PatternSeriesRequest request;
    request.selector.measurement = "cpu";
    request.vshards = {3, 7};
    request.resolveVShards = {7};
    request.maxSeries = 10;
    const std::string encoded = encodePatternSeriesRequest(request);
    for (size_t size = 0; size < encoded.size(); ++size)
        EXPECT_FALSE(decodePatternSeriesRequest(encoded.substr(0, size)).has_value()) << "prefix " << size;

    std::string corrupt = encoded;
    corrupt[0] ^= 0x20;
    EXPECT_FALSE(decodePatternSeriesRequest(corrupt).has_value());

    request.vshards = {7, 3};
    EXPECT_THROW(encodePatternSeriesRequest(request), std::invalid_argument);
    request.vshards = {3, 7};
    request.resolveVShards = {4};
    EXPECT_THROW(encodePatternSeriesRequest(request), std::invalid_argument);
    request.resolveVShards.clear();
    request.maxSeries = 0;
    EXPECT_THROW(encodePatternSeriesRequest(request), std::invalid_argument);
    request.maxSeries = kPatternSeriesMaxResults + 1;
    EXPECT_THROW(encodePatternSeriesRequest(request), std::invalid_argument);

    PatternSeriesResult invalidLimit;
    invalidLimit.limitExceeded = true;
    invalidLimit.seriesKeys = {"cpu value"};
    EXPECT_THROW(encodePatternSeriesResult(invalidLimit), std::invalid_argument);
}

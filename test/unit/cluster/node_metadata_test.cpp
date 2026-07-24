// Integration M2 (metadata scatter): the inter-node metadata request/result codec.
#include "../../../lib/cluster/data/node_metadata.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;

TEST(NodeMetadataCodec, RequestRoundTrips) {
    MetadataRequest req;
    req.kind = MetadataKind::TagValues;
    req.measurement = "cpu";
    req.tagKey = "host";
    req.tagValue = "h1";
    auto back = decodeMetadataRequest(encodeMetadataRequest(req));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->kind, MetadataKind::TagValues);
    EXPECT_EQ(back->measurement, "cpu");
    EXPECT_EQ(back->tagKey, "host");
    EXPECT_EQ(back->tagValue, "h1");
}

TEST(NodeMetadataCodec, ResultRoundTripsItemsAndCardinality) {
    MetadataResult res;
    res.items = {"a", "b", "c"};
    res.cardinality = 12345.0;
    auto back = decodeMetadataResult(encodeMetadataResult(res));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->items, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_DOUBLE_EQ(back->cardinality, 12345.0);
}

TEST(NodeMetadataCodec, TruncationAndBadKindRejected) {
    MetadataRequest req;
    req.kind = MetadataKind::Measurements;
    std::string full = encodeMetadataRequest(req);
    ASSERT_TRUE(decodeMetadataRequest(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeMetadataRequest(full.substr(0, n)).has_value()) << "prefix " << n;

    MetadataResult res;
    res.items = {"x"};
    std::string rfull = encodeMetadataResult(res);
    ASSERT_TRUE(decodeMetadataResult(rfull).has_value());
    std::string bad = rfull;
    bad[bad.size() - 1] ^= 0xff;  // flip checksum
    EXPECT_FALSE(decodeMetadataResult(bad).has_value());
}

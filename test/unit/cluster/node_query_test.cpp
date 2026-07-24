// Integration F.2: the inter-node query request/partial codec. Round-trips the
// full QueryRequest (incl. bucketAnchor + booleansAsNumeric -- the migration-
// compat fields that would otherwise make cluster answers diverge) and typed
// per-series partials (double/bool/string/int64), and rejects malformed frames.
#include "../../../lib/cluster/data/node_query.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::AggregationMethod;
using timestar::FieldValues;
using timestar::QueryRequest;
using timestar::http::SeriesResult;

TEST(NodeQueryCodec, RequestRoundTripIncludingCompatFields) {
    NodeQueryRequest req;
    req.request.aggregation = AggregationMethod::SUM;
    req.request.measurement = "cpu";
    req.request.fields = {"load", "temp"};
    req.request.scopes = {{"host", "h1"}, {"dc", "west"}};
    req.request.groupByTags = {"dc"};
    req.request.startTime = 1000;
    req.request.endTime = 2000;
    req.request.aggregationInterval = 300;
    req.request.bucketAnchor = 1000;         // "start" alignment (compat)
    req.request.booleansAsNumeric = true;    // compat
    req.vshards = {3, 17, 4090};
    req.taskId = 42;
    req.mapEpoch = 7;

    auto back = decodeNodeQueryRequest(encodeNodeQueryRequest(req));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->request.aggregation, AggregationMethod::SUM);
    EXPECT_EQ(back->request.measurement, "cpu");
    EXPECT_EQ(back->request.fields, (std::vector<std::string>{"load", "temp"}));
    EXPECT_EQ(back->request.scopes.at("dc"), "west");
    EXPECT_EQ(back->request.groupByTags, (std::vector<std::string>{"dc"}));
    EXPECT_EQ(back->request.aggregationInterval, 300u);
    EXPECT_EQ(back->request.bucketAnchor, 1000u);
    EXPECT_TRUE(back->request.booleansAsNumeric);
    EXPECT_EQ(back->vshards, (std::vector<uint16_t>{3, 17, 4090}));
    EXPECT_EQ(back->taskId, 42u);
    EXPECT_EQ(back->mapEpoch, 7u);
}

TEST(NodeQueryCodec, PartialRoundTripsAllFieldValueTypes) {
    NodeQueryPartial p;
    SeriesResult s;
    s.measurement = "m";
    s.tags = {{"host", "h1"}};
    s.fields["f_dbl"] = {{10, 20}, FieldValues{std::vector<double>{1.5, -2.5}}};
    s.fields["f_int"] = {{10}, FieldValues{std::vector<int64_t>{9007199254740993LL}}};
    s.fields["f_str"] = {{10}, FieldValues{std::vector<std::string>{"hello, cluster"}}};
    s.fields["f_bool"] = {{10, 20, 30}, FieldValues{std::vector<bool>{true, false, true}}};
    p.series.push_back(s);
    p.incompleteReasons.push_back("vshard 9 unreachable");

    auto back = decodeNodeQueryPartial(encodeNodeQueryPartial(p));
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->series.size(), 1u);
    const auto& r = back->series[0];
    EXPECT_EQ(r.measurement, "m");
    EXPECT_EQ(r.tags.at("host"), "h1");
    EXPECT_EQ(std::get<std::vector<double>>(r.fields.at("f_dbl").second)[1], -2.5);
    EXPECT_EQ(std::get<std::vector<int64_t>>(r.fields.at("f_int").second)[0], 9007199254740993LL);
    EXPECT_EQ(std::get<std::vector<std::string>>(r.fields.at("f_str").second)[0], "hello, cluster");
    EXPECT_EQ(std::get<std::vector<bool>>(r.fields.at("f_bool").second).size(), 3u);
    EXPECT_EQ(back->incompleteReasons, (std::vector<std::string>{"vshard 9 unreachable"}));
}

TEST(NodeQueryCodec, TruncationRejected) {
    NodeQueryRequest req;
    req.request.measurement = "m";
    req.vshards = {1, 2};
    std::string full = encodeNodeQueryRequest(req);
    ASSERT_TRUE(decodeNodeQueryRequest(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeNodeQueryRequest(full.substr(0, n)).has_value()) << "req prefix " << n;

    NodeQueryPartial p;
    SeriesResult s;
    s.measurement = "m";
    s.fields["v"] = {{1}, FieldValues{std::vector<double>{1.0}}};
    p.series.push_back(s);
    std::string pfull = encodeNodeQueryPartial(p);
    ASSERT_TRUE(decodeNodeQueryPartial(pfull).has_value());
    for (size_t n = 0; n < pfull.size(); ++n)
        EXPECT_FALSE(decodeNodeQueryPartial(pfull.substr(0, n)).has_value()) << "partial prefix " << n;
    // Flipped checksum.
    std::string bad = pfull;
    bad[bad.size() - 1] ^= 0xff;
    EXPECT_FALSE(decodeNodeQueryPartial(bad).has_value());
}

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

TEST(NodeQueryCodec, RequestRoundTripIncludingVShardResolution) {
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
    req.request.readConsistency = timestar::ReadConsistencyMode::BoundedStaleness;  // M4
    req.request.maxReadLagIndex = 500;                                    // M4
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
    EXPECT_EQ(back->request.readConsistency, timestar::ReadConsistencyMode::BoundedStaleness);
    EXPECT_EQ(back->request.maxReadLagIndex, 500u);
    EXPECT_EQ(back->vshards, (std::vector<uint16_t>{3, 17, 4090}));
    EXPECT_EQ(back->taskId, 42u);
    EXPECT_EQ(back->mapEpoch, 7u);
}

TEST(NodeQueryCodec, PartialRoundTripsPartialsAndNonNumeric) {
    NodeQueryPartial p;
    // A numeric partial (the unfinalized wire form) in the partials vector.
    timestar::PartialAggregationResult pa;
    pa.measurement = "m";
    pa.fieldName = "v";
    pa.groupKey = std::string("m\0region=west\0v", 15);
    pa.groupKeyHash = std::hash<std::string>{}(pa.groupKey);
    pa.cachedTags = {{"region", "west"}};
    {
        timestar::AggregationState st;
        st.addValue(10.0, 5);
        st.addValue(30.0, 5);
        pa.bucketStates[5] = st;
    }
    p.partials.push_back(pa);
    // Non-numeric (string/bool) series pass through untouched.
    SeriesResult s;
    s.measurement = "m";
    s.tags = {{"host", "h1"}};
    s.fields["f_str"] = {{10}, FieldValues{std::vector<std::string>{"hello, cluster"}}};
    s.fields["f_bool"] = {{10, 20, 30}, FieldValues{std::vector<bool>{true, false, true}}};
    p.nonNumeric.push_back(s);
    p.incompleteReasons.push_back("vshard 9 unreachable");

    auto back = decodeNodeQueryPartial(encodeNodeQueryPartial(p));
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->partials.size(), 1u);
    EXPECT_EQ(back->partials[0].groupKey, pa.groupKey);
    EXPECT_EQ(back->partials[0].cachedTags.at("region"), "west");
    ASSERT_EQ(back->partials[0].bucketStates.count(5), 1u);
    EXPECT_EQ(back->partials[0].bucketStates.at(5).count, 2u);
    ASSERT_EQ(back->nonNumeric.size(), 1u);
    const auto& r = back->nonNumeric[0];
    EXPECT_EQ(r.tags.at("host"), "h1");
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
    s.fields["v"] = {{1}, FieldValues{std::vector<std::string>{"x"}}};
    p.nonNumeric.push_back(s);
    std::string pfull = encodeNodeQueryPartial(p);
    ASSERT_TRUE(decodeNodeQueryPartial(pfull).has_value());
    for (size_t n = 0; n < pfull.size(); ++n)
        EXPECT_FALSE(decodeNodeQueryPartial(pfull.substr(0, n)).has_value()) << "partial prefix " << n;
    // Flipped checksum.
    std::string bad = pfull;
    bad[bad.size() - 1] ^= 0xff;
    EXPECT_FALSE(decodeNodeQueryPartial(bad).has_value());
}

TEST(NodeQueryCodec, OutOfRangeAggregationMethodRejected) {
    // SPREAD is the last method; it (the max) must round-trip, and a
    // checksum-valid frame whose method byte is SPREAD+1 must be rejected rather
    // than handed to executeQuery as a bogus scoped-enum value.
    NodeQueryRequest req;
    req.request.aggregation = AggregationMethod::SPREAD;  // body byte 0 == max method
    req.request.measurement = "m";
    ASSERT_TRUE(decodeNodeQueryRequest(encodeNodeQueryRequest(req)).has_value());

    std::string f = encodeNodeQueryRequest(req);
    f[0] = static_cast<char>(static_cast<uint8_t>(AggregationMethod::SPREAD) + 1);
    // Re-stamp the 8-byte little-endian 64-bit FNV-1a trailer over the body so the
    // frame passes verify() and the range check is the only thing rejecting it.
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i + 8 < f.size(); ++i) {
        h ^= static_cast<uint8_t>(f[i]);
        h *= 1099511628211ull;
    }
    for (int i = 0; i < 8; ++i)
        f[f.size() - 8 + i] = static_cast<char>((h >> (8 * i)) & 0xff);
    EXPECT_FALSE(decodeNodeQueryRequest(f).has_value()) << "out-of-range method must be rejected";
}

// A coordinator can only resolve the leader of a VShard it HOSTS, so at RF < N it names
// the rest for the HOLDER to resolve (`resolveVShards`) and the holder answers for what
// it leads, redirecting the rest (`redirects`). The v1 layout always carries both counts.

TEST(NodeQueryCodec, ResolveVShardsRoundTripAndEmptyList) {
    NodeQueryRequest req;
    req.request.measurement = "m";
    req.vshards = {1, 2, 3, 4};
    const std::string emptyList = encodeNodeQueryRequest(req);

    req.resolveVShards = {2, 4};
    const std::string withTail = encodeNodeQueryRequest(req);
    EXPECT_NE(withTail, emptyList);

    auto back = decodeNodeQueryRequest(withTail);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->vshards, (std::vector<uint16_t>{1, 2, 3, 4}));
    EXPECT_EQ(back->resolveVShards, (std::vector<uint16_t>{2, 4}));

    auto empty = decodeNodeQueryRequest(emptyList);
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->resolveVShards.empty());
    NodeQueryRequest same = req;
    same.resolveVShards.clear();
    EXPECT_EQ(encodeNodeQueryRequest(same), emptyList);
}

TEST(NodeQueryCodec, RedirectsRoundTripAndEmptyList) {
    NodeQueryPartial p;
    p.seriesFound = 3;
    const std::string emptyList = encodeNodeQueryPartial(p);

    p.redirects.push_back(VShardRedirect{7, 4, true});     // hosted, node 4 leads it
    p.redirects.push_back(VShardRedirect{9, 0, true});     // hosted, no elected leader
    p.redirects.push_back(VShardRedirect{11, 0, false});   // not hosted here at all
    const std::string withTail = encodeNodeQueryPartial(p);
    EXPECT_NE(withTail, emptyList);

    auto back = decodeNodeQueryPartial(withTail);
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->redirects.size(), 3u);
    EXPECT_EQ(back->redirects[0].vshard, 7);
    EXPECT_EQ(back->redirects[0].leader, 4u);
    EXPECT_TRUE(back->redirects[0].hosted);
    EXPECT_EQ(back->redirects[1].leader, 0u);
    EXPECT_TRUE(back->redirects[1].hosted);
    EXPECT_FALSE(back->redirects[2].hosted);
    EXPECT_EQ(back->seriesFound, 3u);

    auto empty = decodeNodeQueryPartial(emptyList);
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->redirects.empty());
}

TEST(NodeQueryCodec, TruncatedRedirectTailIsRejected) {
    NodeQueryPartial p;
    p.redirects.push_back(VShardRedirect{7, 4, true});
    std::string bytes = encodeNodeQueryPartial(p);
    // Drop one byte of the tail and re-checksum, so the failure is the LENGTH check and
    // not the FNV -- a padded/truncated tail must not decode as a short redirect list.
    ASSERT_GT(bytes.size(), 9u);
    std::string body = bytes.substr(0, bytes.size() - 9);
    uint64_t h = 1469598103934665603ull;
    for (char c : body) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ull;
    }
    for (int i = 0; i < 8; ++i)
        body.push_back(static_cast<char>((h >> (8 * i)) & 0xff));
    EXPECT_FALSE(decodeNodeQueryPartial(body).has_value());
}

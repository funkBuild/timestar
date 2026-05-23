#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection test: handleCardinality shard-dispatch lambda capture safety
//
// BUG: handleCardinality captured local variables by reference in lambdas
// dispatched to a remote shard. Seastar copies the lambda to the remote shard
// where any captured references dangle.
//
// FIX: Capture measurement, tagKey, tagValue by value. The shard fan-out now
// goes through timestar::cluster::scatterAndSum (lib/cluster/scatter_gather.hpp)
// rather than an inline invoke_on loop, but the by-value capture requirement is
// identical — the lambda still crosses to a remote shard.
// =============================================================================

class HttpMetadataHandlerCaptureTest : public ::testing::Test {};

static std::string readFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

TEST_F(HttpMetadataHandlerCaptureTest, InvokeOnLambdasCaptureByValue) {
    std::string src = readFile("../lib/http/http_metadata_handler.cpp");
    ASSERT_FALSE(src.empty()) << "Could not read http_metadata_handler.cpp";

    // Shard-dispatch lambdas must NOT capture measurement/tagKey/tagValue by
    // reference, regardless of whether dispatch is an inline invoke_on or the
    // cluster::scatter* helper. A by-reference capture of these locals dangles
    // once the lambda is copied to a remote shard.
    //
    // Safe: [measurement, tagKey, tagValue] (by value)
    // Unsafe: [&measurement, ...] / [&tagKey, ...] / [&tagValue, ...] / [&, ...]
    EXPECT_EQ(src.find("[&measurement"), std::string::npos)
        << "shard-dispatch lambda must not capture measurement by reference";
    EXPECT_EQ(src.find("[&tagKey"), std::string::npos)
        << "shard-dispatch lambda must not capture tagKey by reference";
    EXPECT_EQ(src.find("[&tagValue"), std::string::npos)
        << "shard-dispatch lambda must not capture tagValue by reference";

    // Verify the by-value capture is present (positive check). The fan-out
    // helper takes a lambda capturing these locals by value.
    EXPECT_NE(src.find("[measurement, tagKey, tagValue]"), std::string::npos)
        << "tag-cardinality shard lambda should capture measurement/tagKey/tagValue by value";
    EXPECT_NE(src.find("[measurement]"), std::string::npos)
        << "measurement-cardinality shard lambda should capture measurement by value";
}

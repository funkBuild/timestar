#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection test: handleCardinality invoke_on lambda capture safety
//
// BUG: handleCardinality captured local variables by reference in lambdas
// passed to invoke_on(). Seastar's invoke_on copies the lambda to a remote
// shard where the references dangle.
//
// FIX: Capture measurement, tagKey, tagValue by value.
// =============================================================================

class HttpMetadataHandlerCaptureTest : public ::testing::Test {};

static std::string readFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

TEST_F(HttpMetadataHandlerCaptureTest, InvokeOnLambdasCaptureByValue) {
    std::string src = readFile("../lib/http/http_metadata_handler.cpp");
    ASSERT_FALSE(src.empty()) << "Could not read http_metadata_handler.cpp";

    // The two invoke_on lambdas in handleCardinality must NOT capture by reference.
    // Verify no &measurement, &tagKey, &tagValue captures exist in invoke_on contexts.
    //
    // Safe captures: [measurement, tagKey, tagValue] (by value)
    // Unsafe captures: [&measurement, &tagKey, &tagValue] (by reference — dangling on remote shard)

    // Check that no dangerous by-reference captures remain
    EXPECT_EQ(src.find("invoke_on(s, [&measurement"), std::string::npos)
        << "invoke_on lambda must not capture measurement by reference";
    EXPECT_EQ(src.find("invoke_on(s, [&tagKey"), std::string::npos)
        << "invoke_on lambda must not capture tagKey by reference";
    EXPECT_EQ(src.find("invoke_on(s, [&tagValue"), std::string::npos)
        << "invoke_on lambda must not capture tagValue by reference";

    // Verify by-value captures are present (positive check)
    EXPECT_NE(src.find("invoke_on(s, [measurement"), std::string::npos)
        << "invoke_on lambda should capture measurement by value";
}

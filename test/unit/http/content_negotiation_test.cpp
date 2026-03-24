#include "content_negotiation.hpp"

#include <gtest/gtest.h>

#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

using timestar::http::WireFormat;
using timestar::http::parseMediaType;
using timestar::http::requestFormat;
using timestar::http::responseFormat;
using timestar::http::setContentType;
using timestar::http::isProtobuf;

// =============================================================================
// parseMediaType() tests — pure string_view, no Seastar objects needed
// =============================================================================

class ContentNegotiationParseTest : public ::testing::Test {};

TEST_F(ContentNegotiationParseTest, EmptyStringDefaultsToJson) {
    EXPECT_EQ(parseMediaType(""), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, ApplicationJsonReturnsJson) {
    EXPECT_EQ(parseMediaType("application/json"), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, ApplicationJsonWithCharsetReturnsJson) {
    EXPECT_EQ(parseMediaType("application/json; charset=utf-8"), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, ApplicationJsonUpperCaseReturnsJson) {
    EXPECT_EQ(parseMediaType("APPLICATION/JSON"), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, ApplicationJsonMixedCaseReturnsJson) {
    EXPECT_EQ(parseMediaType("Application/Json"), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, ApplicationXProtobufReturnsProtobuf) {
    EXPECT_EQ(parseMediaType("application/x-protobuf"), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationParseTest, ApplicationProtobufReturnsProtobuf) {
    EXPECT_EQ(parseMediaType("application/protobuf"), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationParseTest, ApplicationXProtobufUpperCaseReturnsProtobuf) {
    EXPECT_EQ(parseMediaType("APPLICATION/X-PROTOBUF"), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationParseTest, ApplicationProtobufWithParamsReturnsProtobuf) {
    EXPECT_EQ(parseMediaType("application/x-protobuf; proto=timestar.WriteRequest"), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationParseTest, TextPlainDefaultsToJson) {
    EXPECT_EQ(parseMediaType("text/plain"), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, WhitespaceOnlyDefaultsToJson) {
    EXPECT_EQ(parseMediaType("   "), WireFormat::Json);
}

TEST_F(ContentNegotiationParseTest, LeadingTrailingWhitespaceHandled) {
    EXPECT_EQ(parseMediaType("  application/x-protobuf  "), WireFormat::Protobuf);
}

// =============================================================================
// requestFormat() tests — uses Seastar request with headers
// =============================================================================

class ContentNegotiationRequestTest : public ::testing::Test {};

TEST_F(ContentNegotiationRequestTest, NoContentTypeDefaultsToJson) {
    seastar::http::request req;
    EXPECT_EQ(requestFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationRequestTest, JsonContentTypeReturnsJson) {
    seastar::http::request req;
    req._headers["Content-Type"] = "application/json";
    EXPECT_EQ(requestFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationRequestTest, ProtobufContentTypeReturnsProtobuf) {
    seastar::http::request req;
    req._headers["Content-Type"] = "application/x-protobuf";
    EXPECT_EQ(requestFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationRequestTest, ProtobufAltContentTypeReturnsProtobuf) {
    seastar::http::request req;
    req._headers["Content-Type"] = "application/protobuf";
    EXPECT_EQ(requestFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationRequestTest, JsonWithCharsetReturnsJson) {
    seastar::http::request req;
    req._headers["Content-Type"] = "application/json; charset=utf-8";
    EXPECT_EQ(requestFormat(req), WireFormat::Json);
}

// =============================================================================
// responseFormat() tests — Accept header + echo fallback
// =============================================================================

class ContentNegotiationResponseTest : public ::testing::Test {};

TEST_F(ContentNegotiationResponseTest, AcceptProtobufReturnsProtobuf) {
    seastar::http::request req;
    req._headers["Accept"] = "application/x-protobuf";
    EXPECT_EQ(responseFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationResponseTest, AcceptProtobufAltReturnsProtobuf) {
    seastar::http::request req;
    req._headers["Accept"] = "application/protobuf";
    EXPECT_EQ(responseFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationResponseTest, AcceptJsonReturnsJson) {
    seastar::http::request req;
    req._headers["Accept"] = "application/json";
    EXPECT_EQ(responseFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationResponseTest, AcceptEmptyContentTypeProtobufEchoes) {
    seastar::http::request req;
    // No Accept header, but Content-Type is protobuf -> echo
    req._headers["Content-Type"] = "application/x-protobuf";
    EXPECT_EQ(responseFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationResponseTest, AcceptEmptyContentTypeJsonEchoes) {
    seastar::http::request req;
    // No Accept header, no Content-Type -> default Json
    EXPECT_EQ(responseFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationResponseTest, AcceptEmptyContentTypeEmptyDefaultsToJson) {
    seastar::http::request req;
    req._headers["Content-Type"] = "application/json";
    // No Accept -> echo Content-Type (Json)
    EXPECT_EQ(responseFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationResponseTest, AcceptMultipleWithProtobuf) {
    seastar::http::request req;
    req._headers["Accept"] = "application/json, application/x-protobuf";
    EXPECT_EQ(responseFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationResponseTest, AcceptMultipleWithoutProtobuf) {
    seastar::http::request req;
    req._headers["Accept"] = "application/json, text/html";
    EXPECT_EQ(responseFormat(req), WireFormat::Json);
}

TEST_F(ContentNegotiationResponseTest, AcceptWithQualityParameter) {
    seastar::http::request req;
    req._headers["Accept"] = "application/x-protobuf;q=0.9";
    EXPECT_EQ(responseFormat(req), WireFormat::Protobuf);
}

TEST_F(ContentNegotiationResponseTest, AcceptWildcardFallsBackToJson) {
    seastar::http::request req;
    req._headers["Accept"] = "*/*";
    EXPECT_EQ(responseFormat(req), WireFormat::Json);
}

// =============================================================================
// setContentType() tests
// =============================================================================

class ContentNegotiationSetContentTypeTest : public ::testing::Test {};

TEST_F(ContentNegotiationSetContentTypeTest, SetsJsonContentType) {
    seastar::http::reply rep;
    setContentType(rep, WireFormat::Json);
    EXPECT_EQ(rep.get_header("Content-Type"), "application/json");
}

TEST_F(ContentNegotiationSetContentTypeTest, SetsProtobufContentType) {
    seastar::http::reply rep;
    setContentType(rep, WireFormat::Protobuf);
    EXPECT_EQ(rep.get_header("Content-Type"), "application/x-protobuf");
}

// =============================================================================
// isProtobuf() convenience
// =============================================================================

class ContentNegotiationConvenienceTest : public ::testing::Test {};

TEST_F(ContentNegotiationConvenienceTest, IsProtobufTrueForProtobuf) {
    EXPECT_TRUE(isProtobuf(WireFormat::Protobuf));
}

TEST_F(ContentNegotiationConvenienceTest, IsProtobufFalseForJson) {
    EXPECT_FALSE(isProtobuf(WireFormat::Json));
}

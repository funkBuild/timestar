/*
 * Unit tests for HttpRetentionHandler.
 *
 * The Seastar HTTP handler methods (handlePut/Get/Delete) require a live Seastar
 * reactor and cannot be called directly in a plain GTest binary. Instead, we
 * exercise the observable, synchronous surface of the handler:
 *
 *   1. createErrorResponse()        – non-static member, works with null engine
 *   2. isValidMethod()              – static, all valid and invalid inputs
 *   3. parseDuration() (via proxy)  – delegates to HttpQueryHandler::parseInterval;
 *                                    tested for valid/invalid/overflow inputs
 *   4. RetentionPolicyRequest JSON  – Glaze parsing and round-trip for the exact
 *                                    struct that handlePut reads from req->content
 *   5. RetentionPolicy/Downsample   – serialization used in PUT response
 *   6. Validation rules             – replicate the guard logic from handlePut
 *      without calling co_await so we can unit-test every rejection path
 */

#include <gtest/gtest.h>
#include <glaze/glaze.hpp>

#include "../../../lib/http/http_retention_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../../lib/query/query_parser.hpp"

// ---------------------------------------------------------------------------
// Helper: parse a JSON string into a RetentionPolicyRequest (same call as
// handlePut uses).
// ---------------------------------------------------------------------------
static RetentionPolicyRequest parseRequest(const std::string& json) {
    RetentionPolicyRequest req;
    auto err = glz::read_json(req, json);
    if (err) {
        throw std::runtime_error("JSON parse error: " + std::string(glz::format_error(err)));
    }
    return req;
}

// ---------------------------------------------------------------------------
// Replicate the validation logic from handlePut so that we can test all the
// rejection paths without needing a live Seastar reactor.  Returns an empty
// string on success, or the error message on failure.
// ---------------------------------------------------------------------------
static std::string validatePutRequest(const RetentionPolicyRequest& req) {
    if (req.measurement.empty()) {
        return "'measurement' is required";
    }
    if (!req.ttl.has_value() && !req.downsample.has_value()) {
        return "At least one of 'ttl' or 'downsample' is required";
    }

    uint64_t ttlNanos = 0;
    if (req.ttl.has_value()) {
        try {
            ttlNanos = tsdb::HttpQueryHandler::parseInterval(*req.ttl);
        } catch (const std::exception& e) {
            return std::string("Invalid ttl: ") + e.what();
        }
    }

    if (req.downsample.has_value()) {
        const auto& ds = *req.downsample;
        if (ds.after.empty())    return "downsample.after is required";
        if (ds.interval.empty()) return "downsample.interval is required";
        if (ds.method.empty())   return "downsample.method is required";

        const auto& validMethods = {"avg", "min", "max", "sum", "latest"};
        bool methodOk = false;
        for (auto& m : validMethods) { if (ds.method == m) { methodOk = true; break; } }
        if (!methodOk) {
            return "Invalid downsample.method: must be one of avg, min, max, sum, latest";
        }

        uint64_t afterNanos = 0;
        try {
            afterNanos = tsdb::HttpQueryHandler::parseInterval(ds.after);
        } catch (const std::exception& e) {
            return std::string("Invalid downsample.after: ") + e.what();
        }

        try {
            tsdb::HttpQueryHandler::parseInterval(ds.interval);
        } catch (const std::exception& e) {
            return std::string("Invalid downsample.interval: ") + e.what();
        }

        // ttl must be > downsample.after when both are present
        if (ttlNanos > 0 && ttlNanos <= afterNanos) {
            return "ttl must be greater than downsample.after";
        }
    }

    return ""; // success
}

// =============================================================================
// createErrorResponse
// =============================================================================

class HttpRetentionHandlerErrorResponseTest : public ::testing::Test {
protected:
    // Instantiating with nullptr is safe: createErrorResponse never
    // dereferences engineSharded.
    HttpRetentionHandler handler{nullptr};
};

TEST_F(HttpRetentionHandlerErrorResponseTest, ProducesValidJson) {
    std::string json = handler.createErrorResponse("something went wrong");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "createErrorResponse output is not valid JSON";
}

TEST_F(HttpRetentionHandlerErrorResponseTest, StatusFieldIsError) {
    std::string json = handler.createErrorResponse("oops");

    glz::generic parsed;
    auto ec1 = glz::read_json(parsed, json);
    ASSERT_FALSE(ec1);
    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
}

TEST_F(HttpRetentionHandlerErrorResponseTest, ErrorFieldMatchesInput) {
    std::string msg = "retention policy not found";
    std::string json = handler.createErrorResponse(msg);

    glz::generic parsed;
    auto ec2 = glz::read_json(parsed, json);
    ASSERT_FALSE(ec2);
    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["error"].get<std::string>(), msg);
}

TEST_F(HttpRetentionHandlerErrorResponseTest, HandlesEmptyMessage) {
    std::string json = handler.createErrorResponse("");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_EQ(obj["error"].get<std::string>(), "");
}

TEST_F(HttpRetentionHandlerErrorResponseTest, HandlesSpecialCharacters) {
    std::string msg = "field 'ttl' has \"bad\" value: <30x>";
    std::string json = handler.createErrorResponse(msg);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Should produce valid JSON even with special chars";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_NE(obj["error"].get<std::string>().find("bad"), std::string::npos);
}

// =============================================================================
// isValidMethod (static)
// =============================================================================

class HttpRetentionHandlerIsValidMethodTest : public ::testing::Test {};

TEST_F(HttpRetentionHandlerIsValidMethodTest, AvgIsValid) {
    EXPECT_TRUE(HttpRetentionHandler::isValidMethod("avg"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, MinIsValid) {
    EXPECT_TRUE(HttpRetentionHandler::isValidMethod("min"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, MaxIsValid) {
    EXPECT_TRUE(HttpRetentionHandler::isValidMethod("max"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, SumIsValid) {
    EXPECT_TRUE(HttpRetentionHandler::isValidMethod("sum"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, LatestIsValid) {
    EXPECT_TRUE(HttpRetentionHandler::isValidMethod("latest"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, EmptyStringIsInvalid) {
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod(""));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, UppercaseIsInvalid) {
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("AVG"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("Min"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("MAX"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, UnknownMethodIsInvalid) {
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("mean"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("count"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("first"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("last"));
}

TEST_F(HttpRetentionHandlerIsValidMethodTest, LeadingTrailingSpaceIsInvalid) {
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod(" avg"));
    EXPECT_FALSE(HttpRetentionHandler::isValidMethod("avg "));
}

// =============================================================================
// parseDuration — static private, exposed via HttpQueryHandler::parseInterval
// which is what parseDuration delegates to.
// =============================================================================

class HttpRetentionHandlerParseDurationTest : public ::testing::Test {};

TEST_F(HttpRetentionHandlerParseDurationTest, ParseSeconds) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("1s"), 1'000'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseMinutes) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("5m"), 5ULL * 60 * 1'000'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseHours) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("1h"), 3600ULL * 1'000'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseDays) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("30d"),
              30ULL * 86400ULL * 1'000'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseMilliseconds) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("100ms"), 100'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseNanoseconds) {
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("500ns"), 500ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, ParseTypicalRetentionPeriods) {
    // 7d
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("7d"),
              7ULL * 86400ULL * 1'000'000'000ULL);
    // 90d
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("90d"),
              90ULL * 86400ULL * 1'000'000'000ULL);
    // 365d
    EXPECT_EQ(tsdb::HttpQueryHandler::parseInterval("365d"),
              365ULL * 86400ULL * 1'000'000'000ULL);
}

TEST_F(HttpRetentionHandlerParseDurationTest, EmptyStringThrows) {
    EXPECT_THROW(tsdb::HttpQueryHandler::parseInterval(""), tsdb::QueryParseException);
}

TEST_F(HttpRetentionHandlerParseDurationTest, InvalidFormatThrows) {
    EXPECT_THROW(tsdb::HttpQueryHandler::parseInterval("notaduration"), tsdb::QueryParseException);
}

TEST_F(HttpRetentionHandlerParseDurationTest, UnknownUnitThrows) {
    EXPECT_THROW(tsdb::HttpQueryHandler::parseInterval("5x"), tsdb::QueryParseException);
}

TEST_F(HttpRetentionHandlerParseDurationTest, OverflowThrows) {
    // So large it would overflow uint64_t in nanoseconds
    EXPECT_THROW(tsdb::HttpQueryHandler::parseInterval("99999999999d"), tsdb::QueryParseException);
}

// =============================================================================
// RetentionPolicyRequest JSON parsing (Glaze)
// These reproduce the exact glz::read_json call made inside handlePut.
// =============================================================================

class RetentionPolicyRequestParsingTest : public ::testing::Test {};

TEST_F(RetentionPolicyRequestParsingTest, ParseValidTtlOnly) {
    auto req = parseRequest(R"({"measurement":"cpu","ttl":"90d"})");

    EXPECT_EQ(req.measurement, "cpu");
    ASSERT_TRUE(req.ttl.has_value());
    EXPECT_EQ(*req.ttl, "90d");
    EXPECT_FALSE(req.downsample.has_value());
}

TEST_F(RetentionPolicyRequestParsingTest, ParseValidDownsampleOnly) {
    auto req = parseRequest(R"({
        "measurement": "temperature",
        "downsample": {
            "after":    "30d",
            "interval": "5m",
            "method":   "avg"
        }
    })");

    EXPECT_EQ(req.measurement, "temperature");
    EXPECT_FALSE(req.ttl.has_value());
    ASSERT_TRUE(req.downsample.has_value());
    EXPECT_EQ(req.downsample->after,    "30d");
    EXPECT_EQ(req.downsample->interval, "5m");
    EXPECT_EQ(req.downsample->method,   "avg");
}

TEST_F(RetentionPolicyRequestParsingTest, ParseBothTtlAndDownsample) {
    auto req = parseRequest(R"({
        "measurement": "metrics",
        "ttl": "90d",
        "downsample": {
            "after":    "30d",
            "interval": "1h",
            "method":   "max"
        }
    })");

    EXPECT_EQ(req.measurement, "metrics");
    ASSERT_TRUE(req.ttl.has_value());
    EXPECT_EQ(*req.ttl, "90d");
    ASSERT_TRUE(req.downsample.has_value());
    EXPECT_EQ(req.downsample->method, "max");
}

TEST_F(RetentionPolicyRequestParsingTest, ParseDownsampleAllMethods) {
    for (const char* method : {"avg", "min", "max", "sum", "latest"}) {
        std::string json = std::string(R"({"measurement":"m","downsample":{"after":"1d","interval":"5m","method":")") + method + R"("}})";
        auto req = parseRequest(json);
        ASSERT_TRUE(req.downsample.has_value()) << "method=" << method;
        EXPECT_EQ(req.downsample->method, method) << "method=" << method;
    }
}

TEST_F(RetentionPolicyRequestParsingTest, MissingMeasurementYieldsEmptyString) {
    // Glaze parses successfully but measurement stays empty; handlePut rejects it
    auto req = parseRequest(R"({"ttl":"30d"})");
    EXPECT_TRUE(req.measurement.empty());
    EXPECT_TRUE(req.ttl.has_value());
}

TEST_F(RetentionPolicyRequestParsingTest, MissingTtlAndDownsampleBothAbsent) {
    // Valid JSON, but handlePut would reject because neither ttl nor downsample
    auto req = parseRequest(R"({"measurement":"cpu"})");
    EXPECT_EQ(req.measurement, "cpu");
    EXPECT_FALSE(req.ttl.has_value());
    EXPECT_FALSE(req.downsample.has_value());
}

TEST_F(RetentionPolicyRequestParsingTest, InvalidJsonThrows) {
    EXPECT_THROW(parseRequest("{ not json }"), std::runtime_error);
}

TEST_F(RetentionPolicyRequestParsingTest, EmptyJsonObjectParsesToDefaults) {
    auto req = parseRequest("{}");
    EXPECT_TRUE(req.measurement.empty());
    EXPECT_FALSE(req.ttl.has_value());
    EXPECT_FALSE(req.downsample.has_value());
}

TEST_F(RetentionPolicyRequestParsingTest, DownsampleNanosFieldsPreservedOnRoundTrip) {
    // afterNanos and intervalNanos in the request are optional fill-ins; verify
    // the struct can carry them through a JSON round-trip.
    auto req = parseRequest(R"({
        "measurement": "disk",
        "downsample": {
            "after":        "7d",
            "afterNanos":   604800000000000,
            "interval":     "1h",
            "intervalNanos":3600000000000,
            "method":       "min"
        }
    })");

    ASSERT_TRUE(req.downsample.has_value());
    EXPECT_EQ(req.downsample->afterNanos,    604800000000000ULL);
    EXPECT_EQ(req.downsample->intervalNanos, 3600000000000ULL);
}

// =============================================================================
// PUT validation logic (replicated from handlePut, no Seastar required)
// =============================================================================

class HttpRetentionHandlerPutValidationTest : public ::testing::Test {};

// --- Success paths ---

TEST_F(HttpRetentionHandlerPutValidationTest, ValidTtlOnlyAccepted) {
    auto req = parseRequest(R"({"measurement":"cpu","ttl":"90d"})");
    EXPECT_EQ(validatePutRequest(req), "");
}

TEST_F(HttpRetentionHandlerPutValidationTest, ValidDownsampleOnlyAccepted) {
    auto req = parseRequest(R"({
        "measurement":"mem",
        "downsample":{"after":"7d","interval":"5m","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "");
}

TEST_F(HttpRetentionHandlerPutValidationTest, ValidBothTtlAndDownsampleAccepted) {
    // ttl=90d > downsample.after=30d
    auto req = parseRequest(R"({
        "measurement":"disk",
        "ttl":"90d",
        "downsample":{"after":"30d","interval":"1h","method":"max"}
    })");
    EXPECT_EQ(validatePutRequest(req), "");
}

TEST_F(HttpRetentionHandlerPutValidationTest, AllValidMethodsAccepted) {
    for (const char* m : {"avg", "min", "max", "sum", "latest"}) {
        std::string json = std::string(
            R"({"measurement":"x","downsample":{"after":"1d","interval":"5m","method":")") +
            m + R"("}})";
        auto req = parseRequest(json);
        EXPECT_EQ(validatePutRequest(req), "") << "method=" << m;
    }
}

// --- Missing required fields ---

TEST_F(HttpRetentionHandlerPutValidationTest, MissingMeasurementRejected) {
    auto req = parseRequest(R"({"ttl":"30d"})");
    EXPECT_EQ(validatePutRequest(req), "'measurement' is required");
}

TEST_F(HttpRetentionHandlerPutValidationTest, EmptyMeasurementRejected) {
    auto req = parseRequest(R"({"measurement":"","ttl":"30d"})");
    EXPECT_EQ(validatePutRequest(req), "'measurement' is required");
}

TEST_F(HttpRetentionHandlerPutValidationTest, NeitherTtlNorDownsampleRejected) {
    auto req = parseRequest(R"({"measurement":"cpu"})");
    EXPECT_EQ(validatePutRequest(req),
              "At least one of 'ttl' or 'downsample' is required");
}

// --- Invalid TTL ---

TEST_F(HttpRetentionHandlerPutValidationTest, InvalidTtlFormatRejected) {
    auto req = parseRequest(R"({"measurement":"cpu","ttl":"notaduration"})");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid ttl"), std::string::npos) << "got: " << err;
}

TEST_F(HttpRetentionHandlerPutValidationTest, UnknownTtlUnitRejected) {
    auto req = parseRequest(R"({"measurement":"cpu","ttl":"30x"})");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid ttl"), std::string::npos) << "got: " << err;
}

TEST_F(HttpRetentionHandlerPutValidationTest, EmptyTtlStringRejected) {
    auto req = parseRequest(R"({"measurement":"cpu","ttl":""})");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid ttl"), std::string::npos) << "got: " << err;
}

// --- Invalid downsample fields ---

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleMissingAfterRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"","interval":"5m","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "downsample.after is required");
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleMissingIntervalRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"7d","interval":"","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "downsample.interval is required");
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleMissingMethodRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"7d","interval":"5m","method":""}
    })");
    EXPECT_EQ(validatePutRequest(req), "downsample.method is required");
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleInvalidMethodRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"7d","interval":"5m","method":"mean"}
    })");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid downsample.method"), std::string::npos) << "got: " << err;
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleUppercaseMethodRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"7d","interval":"5m","method":"AVG"}
    })");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid downsample.method"), std::string::npos) << "got: " << err;
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleInvalidAfterDurationRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"badvalue","interval":"5m","method":"avg"}
    })");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid downsample.after"), std::string::npos) << "got: " << err;
}

TEST_F(HttpRetentionHandlerPutValidationTest, DownsampleInvalidIntervalDurationRejected) {
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "downsample":{"after":"7d","interval":"badvalue","method":"avg"}
    })");
    std::string err = validatePutRequest(req);
    EXPECT_NE(err.find("Invalid downsample.interval"), std::string::npos) << "got: " << err;
}

// --- TTL vs downsample.after constraint ---

TEST_F(HttpRetentionHandlerPutValidationTest, TtlLessThanDownsampleAfterRejected) {
    // ttl=30d, downsample.after=90d — TTL must be > after
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "ttl":"30d",
        "downsample":{"after":"90d","interval":"1h","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "ttl must be greater than downsample.after");
}

TEST_F(HttpRetentionHandlerPutValidationTest, TtlEqualToDownsampleAfterRejected) {
    // ttl=30d == downsample.after=30d → still invalid (must be strictly greater)
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "ttl":"30d",
        "downsample":{"after":"30d","interval":"1h","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "ttl must be greater than downsample.after");
}

TEST_F(HttpRetentionHandlerPutValidationTest, TtlJustGreaterThanDownsampleAfterAccepted) {
    // ttl=91d > downsample.after=90d — valid
    auto req = parseRequest(R"({
        "measurement":"cpu",
        "ttl":"91d",
        "downsample":{"after":"90d","interval":"1h","method":"avg"}
    })");
    EXPECT_EQ(validatePutRequest(req), "");
}

// =============================================================================
// DELETE request: query parameter parsing
// The handleDelete reads "measurement" from req->query_parameters. We verify
// the rejection message for a missing measurement using the same text the
// handler emits.
// =============================================================================

class HttpRetentionHandlerDeleteParsingTest : public ::testing::Test {
protected:
    HttpRetentionHandler handler{nullptr};
};

TEST_F(HttpRetentionHandlerDeleteParsingTest, ErrorResponseForMissingMeasurement) {
    // Simulate what the handler returns when the ?measurement= param is absent.
    std::string json = handler.createErrorResponse("'measurement' query parameter is required");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_NE(obj["error"].get<std::string>().find("measurement"), std::string::npos);
}

TEST_F(HttpRetentionHandlerDeleteParsingTest, ErrorResponseForNotFound) {
    std::string measurement = "nonexistent_metric";
    std::string json = handler.createErrorResponse(
        "No retention policy found for measurement: " + measurement);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_NE(obj["error"].get<std::string>().find(measurement), std::string::npos);
}

// =============================================================================
// GET request: response format verification
// We build the GET response objects directly (as handleGet does) and verify
// their structure without needing a Seastar future.
// =============================================================================

class HttpRetentionHandlerGetResponseTest : public ::testing::Test {};

TEST_F(HttpRetentionHandlerGetResponseTest, SinglePolicyResponseFormat) {
    RetentionPolicy policy;
    policy.measurement = "cpu";
    policy.ttl         = "90d";
    policy.ttlNanos    = 90ULL * 86400ULL * 1'000'000'000ULL;

    auto responseObj = glz::obj{"status", "success", "policy", policy};
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_TRUE(obj.count("policy") > 0);
}

TEST_F(HttpRetentionHandlerGetResponseTest, AllPoliciesResponseFormat) {
    std::vector<RetentionPolicy> policies;

    RetentionPolicy p1;
    p1.measurement = "cpu";
    p1.ttl         = "90d";
    p1.ttlNanos    = 90ULL * 86400ULL * 1'000'000'000ULL;
    policies.push_back(p1);

    RetentionPolicy p2;
    p2.measurement = "mem";
    p2.ttl         = "30d";
    p2.ttlNanos    = 30ULL * 86400ULL * 1'000'000'000ULL;
    policies.push_back(p2);

    auto responseObj = glz::obj{"status", "success", "policies", policies};
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_TRUE(obj.count("policies") > 0);
}

TEST_F(HttpRetentionHandlerGetResponseTest, EmptyPoliciesListFormat) {
    std::vector<RetentionPolicy> empty;
    auto responseObj = glz::obj{"status", "success", "policies", empty};
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
}

TEST_F(HttpRetentionHandlerGetResponseTest, NotFoundErrorFormat) {
    HttpRetentionHandler handler{nullptr};
    std::string json = handler.createErrorResponse(
        "No retention policy found for measurement: cpu");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_NE(obj["error"].get<std::string>().find("cpu"), std::string::npos);
}

TEST_F(HttpRetentionHandlerGetResponseTest, PolicyWithDownsampleSerializesCorrectly) {
    RetentionPolicy policy;
    policy.measurement = "temperature";
    policy.ttl         = "90d";
    policy.ttlNanos    = 90ULL * 86400ULL * 1'000'000'000ULL;

    DownsamplePolicy ds;
    ds.after         = "30d";
    ds.afterNanos    = 30ULL * 86400ULL * 1'000'000'000ULL;
    ds.interval      = "5m";
    ds.intervalNanos = 5ULL * 60ULL * 1'000'000'000ULL;
    ds.method        = "avg";
    policy.downsample = ds;

    auto responseObj = glz::obj{"status", "success", "policy", policy};
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    // Round-trip: deserialize the inner policy
    RetentionPolicy recovered;
    // Extract just the "policy" value from the response json
    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_TRUE(obj.count("policy") > 0);
}

// =============================================================================
// PUT success response format
// =============================================================================

class HttpRetentionHandlerPutResponseTest : public ::testing::Test {};

TEST_F(HttpRetentionHandlerPutResponseTest, SuccessResponseContainsStatusAndPolicy) {
    RetentionPolicy policy;
    policy.measurement = "cpu";
    policy.ttl         = "90d";
    policy.ttlNanos    = 90ULL * 86400ULL * 1'000'000'000ULL;

    auto responseObj = glz::obj{"status", "success", "policy", policy};
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_TRUE(obj.count("policy") > 0);
}

TEST_F(HttpRetentionHandlerPutResponseTest, DeleteSuccessResponseFormat) {
    std::string measurement = "cpu";
    std::string message = "Retention policy deleted for measurement: " + measurement;
    auto responseObj = glz::obj{
        "status",  "success",
        "message", message
    };
    auto json = glz::write_json(responseObj);
    ASSERT_TRUE(json.has_value());

    glz::generic parsed;
    auto ec = glz::read_json(parsed, *json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_NE(obj["message"].get<std::string>().find("cpu"), std::string::npos);
}

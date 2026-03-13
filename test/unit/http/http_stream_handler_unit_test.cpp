// =============================================================================
// Unit tests for HttpStreamHandler pure/static functions.
//
// The Seastar reactor is NOT required for any of these tests.
//
// Tested functions:
//   - HttpStreamHandler::formatSSEEvent()
//   - HttpStreamHandler::formatSSEBackfillEvent()
//   - HttpStreamHandler::queryResponseToBatches()
//   - buildSSEJsonPayload() (indirectly, via format* functions)
//   - jsonEscape() (indirectly, via format* functions that call buildSSEJsonPayload)
//
// formatSSEEvent / formatSSEBackfillEvent are public static methods declared in
// http_stream_handler.hpp and implemented in http_stream_handler.cpp.
//
// buildSSEJsonPayload and jsonEscape are file-static helpers in
// http_stream_handler.cpp.  They are exercised here through the public
// static API, giving full indirect coverage without needing to expose them.
// =============================================================================

#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_stream_handler.hpp"
#include "../../../lib/query/expression_parser.hpp"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <variant>
#include <vector>

using namespace timestar;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Build a minimal StreamingBatch from a single double data point.
static StreamingBatch makeBatch(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                const std::string& field, uint64_t timestamp, double value, uint64_t seqId = 0,
                                const std::string& label = "") {
    StreamingBatch batch;
    batch.sequenceId = seqId;
    batch.label = label;

    StreamingDataPoint pt;
    pt.measurement = measurement;
    pt.tags = tags;
    pt.field = field;
    pt.timestamp = timestamp;
    pt.value = value;
    batch.points.push_back(std::move(pt));
    return batch;
}

// ---------------------------------------------------------------------------
// SSE event format: outer envelope
// ---------------------------------------------------------------------------

class SSEEventFormatTest : public ::testing::Test {};

TEST_F(SSEEventFormatTest, EventStartsWithId) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 42);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_EQ(event.substr(0, 4), "id: ") << "SSE event must start with 'id: '";
}

TEST_F(SSEEventFormatTest, EventContainsSequenceId) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 7);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("id: 7\n"), std::string::npos) << "SSE event must contain the sequence id";
}

TEST_F(SSEEventFormatTest, EventTypeIsData) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 0);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("\nevent: data\n"), std::string::npos) << "formatSSEEvent must emit 'event: data'";
}

TEST_F(SSEEventFormatTest, EventEndsWithDoubleNewline) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 0);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_EQ(event.substr(event.size() - 2), "\n\n") << "SSE event must end with double newline";
}

TEST_F(SSEEventFormatTest, EventContainsDataPrefix) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 0);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("\ndata: "), std::string::npos) << "SSE event must contain 'data: ' line";
}

TEST_F(SSEEventFormatTest, BackfillEventTypeIsBackfill) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 3);
    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batch);
    EXPECT_NE(event.find("\nevent: backfill\n"), std::string::npos)
        << "formatSSEBackfillEvent must emit 'event: backfill'";
}

TEST_F(SSEEventFormatTest, BackfillEventStartsWithId) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 9);
    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batch);
    EXPECT_EQ(event.substr(0, 4), "id: ");
    EXPECT_NE(event.find("id: 9\n"), std::string::npos);
}

TEST_F(SSEEventFormatTest, BackfillEventEndsWithDoubleNewline) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 50.0, 0);
    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batch);
    EXPECT_EQ(event.substr(event.size() - 2), "\n\n");
}

TEST_F(SSEEventFormatTest, DataAndBackfillDifferOnlyInEventType) {
    auto batchA = makeBatch("cpu", {}, "usage", 1000, 50.0, 5);
    auto batchB = makeBatch("cpu", {}, "usage", 1000, 50.0, 5);

    std::string evData = HttpStreamHandler::formatSSEEvent(batchA);
    std::string evBackfill = HttpStreamHandler::formatSSEBackfillEvent(batchB);

    // Replace the differing event type keyword and compare the rest
    auto replaceAll = [](std::string s, const std::string& from, const std::string& to) {
        size_t pos = 0;
        while ((pos = s.find(from, pos)) != std::string::npos) {
            s.replace(pos, from.size(), to);
            pos += to.size();
        }
        return s;
    };
    std::string normalizedData = replaceAll(evData, "event: data", "event: X");
    std::string normalizedBackfill = replaceAll(evBackfill, "event: backfill", "event: X");

    EXPECT_EQ(normalizedData, normalizedBackfill)
        << "formatSSEEvent and formatSSEBackfillEvent should differ only in event type";
}

// ---------------------------------------------------------------------------
// JSON payload content (tested via formatSSEEvent)
// ---------------------------------------------------------------------------

class SSEPayloadContentTest : public ::testing::Test {
protected:
    // Extract the JSON payload from a formatted SSE event string.
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }
};

TEST_F(SSEPayloadContentTest, PayloadIsBraceDelimitedJSON) {
    auto batch = makeBatch("temperature", {}, "value", 1000, 23.5);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_EQ(payload.front(), '{');
    EXPECT_EQ(payload.back(), '}');
}

TEST_F(SSEPayloadContentTest, PayloadContainsSeriesKey) {
    auto batch = makeBatch("temperature", {}, "value", 1000, 23.5);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"series\""), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadContainsMeasurementName) {
    auto batch = makeBatch("temperature", {}, "value", 1000, 23.5);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"measurement\":\"temperature\""), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadContainsTagsObject) {
    auto batch = makeBatch("cpu", {{"host", "server01"}, {"dc", "us-east"}}, "usage", 2000, 80.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"tags\""), std::string::npos);
    EXPECT_NE(payload.find("\"host\":\"server01\""), std::string::npos);
    EXPECT_NE(payload.find("\"dc\":\"us-east\""), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadContainsEmptyTagsObjectWhenNoTags) {
    auto batch = makeBatch("cpu", {}, "usage", 2000, 80.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"tags\":{}"), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadContainsFieldsObject) {
    auto batch = makeBatch("temperature", {}, "value", 1000, 23.5);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"fields\""), std::string::npos);
    EXPECT_NE(payload.find("\"value\""), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadContainsTimestampsAndValues) {
    auto batch = makeBatch("temperature", {}, "value", 1000000000ULL, 23.5);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"timestamps\""), std::string::npos);
    EXPECT_NE(payload.find("\"values\""), std::string::npos);
    EXPECT_NE(payload.find("1000000000"), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadWithLabelIncludesLabelField) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 90.0, 0, "cpu_label");
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"label\":\"cpu_label\""), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadWithoutLabelOmitsLabelField) {
    auto batch = makeBatch("cpu", {}, "usage", 1000, 90.0, 0, "");
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_EQ(payload.find("\"label\""), std::string::npos)
        << "Empty label should not produce a 'label' key in the JSON";
}

TEST_F(SSEPayloadContentTest, PayloadBoolTrueValue) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "switches";
    pt.field = "state";
    pt.timestamp = 5000;
    pt.value = true;
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("true"), std::string::npos);
    EXPECT_EQ(payload.find("false"), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadBoolFalseValue) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "switches";
    pt.field = "state";
    pt.timestamp = 5000;
    pt.value = false;
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("false"), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadInt64Value) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "counters";
    pt.field = "count";
    pt.timestamp = 1000;
    pt.value = int64_t(9876543210LL);
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("9876543210"), std::string::npos);
}

TEST_F(SSEPayloadContentTest, PayloadStringValueIsQuoted) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "logs";
    pt.field = "message";
    pt.timestamp = 1000;
    pt.value = std::string("hello world");
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"hello world\""), std::string::npos) << "String values must be JSON-quoted in the payload";
}

TEST_F(SSEPayloadContentTest, PayloadInfDoubleBecomesNull) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "temp";
    pt.field = "val";
    pt.timestamp = 1000;
    pt.value = std::numeric_limits<double>::infinity();
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("null"), std::string::npos) << "Inf double must be serialized as null in JSON";
}

TEST_F(SSEPayloadContentTest, PayloadNegInfDoubleBecomesNull) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "temp";
    pt.field = "val";
    pt.timestamp = 1000;
    pt.value = -std::numeric_limits<double>::infinity();
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("null"), std::string::npos) << "Negative Inf must be serialized as null";
}

TEST_F(SSEPayloadContentTest, PayloadNaNDoubleBecomesNull) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "temp";
    pt.field = "val";
    pt.timestamp = 1000;
    pt.value = std::numeric_limits<double>::quiet_NaN();
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("null"), std::string::npos) << "NaN must be serialized as null";
}

// ---------------------------------------------------------------------------
// JSON escaping (tested via buildSSEJsonPayload through formatSSEEvent)
// ---------------------------------------------------------------------------

class SSEJsonEscapeTest : public ::testing::Test {
protected:
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }
};

TEST_F(SSEJsonEscapeTest, DoubleQuoteInMeasurementIsEscaped) {
    auto batch = makeBatch("meas\"ure", {}, "value", 1000, 1.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("meas\\\"ure"), std::string::npos) << "Double-quote in measurement must be escaped as \\\"";
    // Raw unescaped double quote should not appear in the JSON key value
    EXPECT_EQ(payload.find("\"meas\"ure\""), std::string::npos)
        << "Unescaped double-quote should NOT appear in measurement name";
}

TEST_F(SSEJsonEscapeTest, BackslashInTagValueIsEscaped) {
    auto batch = makeBatch("cpu", {{"path", "C:\\Windows"}}, "usage", 1000, 50.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("C:\\\\Windows"), std::string::npos) << "Backslash must be escaped as \\\\";
}

TEST_F(SSEJsonEscapeTest, NewlineInStringValueIsEscaped) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "logs";
    pt.field = "msg";
    pt.timestamp = 1000;
    pt.value = std::string("line1\nline2");
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\\n"), std::string::npos) << "Newline in string value must be escaped as \\n";
    // The literal newline character must NOT appear inside the JSON value
    // (it would break SSE framing if it appeared in the data: line)
    std::string afterData = payload;
    // Strip the outer JSON wrapper to get the actual value region
    EXPECT_EQ(payload.find("line1\nline2"), std::string::npos)
        << "Literal newline must not appear unescaped in JSON payload";
}

TEST_F(SSEJsonEscapeTest, CarriageReturnIsEscaped) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "logs";
    pt.field = "msg";
    pt.timestamp = 1000;
    pt.value = std::string("val\r\nue");
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\\r"), std::string::npos) << "Carriage return must be escaped as \\r";
}

TEST_F(SSEJsonEscapeTest, TabIsEscaped) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "logs";
    pt.field = "msg";
    pt.timestamp = 1000;
    pt.value = std::string("col1\tcol2");
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\\t"), std::string::npos) << "Tab must be escaped as \\t";
}

TEST_F(SSEJsonEscapeTest, ControlCharacterBelowSpace) {
    // ASCII 0x01 (SOH) is a control character that must be \u0001
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "ctrl";
    pt.field = "val";
    pt.timestamp = 1000;
    pt.value = std::string("\x01hello");
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\\u0001"), std::string::npos) << "Control character 0x01 must be escaped as \\u0001";
}

TEST_F(SSEJsonEscapeTest, NullByteIsEscaped) {
    // A null byte (0x00) must become \u0000
    std::string withNull = "ab";
    withNull += '\0';
    withNull += "cd";

    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "test";
    pt.field = "val";
    pt.timestamp = 1000;
    pt.value = withNull;
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\\u0000"), std::string::npos) << "Null byte must be escaped as \\u0000";
}

TEST_F(SSEJsonEscapeTest, NormalAsciiNotEscaped) {
    auto batch = makeBatch("hello_world", {{"key", "value123"}}, "field_a", 1000, 42.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("hello_world"), std::string::npos) << "Normal ASCII must NOT be escaped";
    EXPECT_NE(payload.find("value123"), std::string::npos);
}

TEST_F(SSEJsonEscapeTest, DoubleQuoteInFieldNameIsEscaped) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    StreamingDataPoint pt;
    pt.measurement = "test";
    pt.field = "fi\"eld";
    pt.timestamp = 1000;
    pt.value = 1.0;
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("fi\\\"eld"), std::string::npos) << "Double-quote in field name must be escaped";
}

TEST_F(SSEJsonEscapeTest, LabelWithSpecialCharsIsEscaped) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    batch.label = "lab\"el";
    StreamingDataPoint pt;
    pt.measurement = "m";
    pt.field = "v";
    pt.timestamp = 1000;
    pt.value = 1.0;
    batch.points.push_back(std::move(pt));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("lab\\\"el"), std::string::npos) << "Double-quote in label must be escaped";
}

// ---------------------------------------------------------------------------
// Multiple data points grouped under same series
// ---------------------------------------------------------------------------

class SSEPayloadGroupingTest : public ::testing::Test {
protected:
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }
};

TEST_F(SSEPayloadGroupingTest, TwoPointsSameSeries) {
    StreamingBatch batch;
    batch.sequenceId = 0;
    for (uint64_t ts : {1000ULL, 2000ULL}) {
        StreamingDataPoint pt;
        pt.measurement = "temperature";
        pt.field = "value";
        pt.timestamp = ts;
        pt.value = 23.5;
        batch.points.push_back(std::move(pt));
    }
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));

    // Should be a single series entry with 2 timestamps
    // Count occurrences of "measurement" to verify single group
    size_t count = 0;
    size_t pos = 0;
    while ((pos = payload.find("\"measurement\"", pos)) != std::string::npos) {
        ++count;
        ++pos;
    }
    EXPECT_EQ(count, 1u) << "Two points from same series should produce one series entry";
}

TEST_F(SSEPayloadGroupingTest, TwoPointsDifferentSeries) {
    StreamingBatch batch;
    batch.sequenceId = 0;

    StreamingDataPoint pt1;
    pt1.measurement = "temperature";
    pt1.tags = {{"location", "us-west"}};
    pt1.field = "value";
    pt1.timestamp = 1000;
    pt1.value = 23.5;
    batch.points.push_back(std::move(pt1));

    StreamingDataPoint pt2;
    pt2.measurement = "temperature";
    pt2.tags = {{"location", "us-east"}};
    pt2.field = "value";
    pt2.timestamp = 2000;
    pt2.value = 25.0;
    batch.points.push_back(std::move(pt2));

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));

    // Two distinct series -> measurement key appears twice
    size_t count = 0;
    size_t pos = 0;
    while ((pos = payload.find("\"measurement\"", pos)) != std::string::npos) {
        ++count;
        ++pos;
    }
    EXPECT_EQ(count, 2u) << "Two different tag sets should produce two series entries";
    EXPECT_NE(payload.find("us-west"), std::string::npos);
    EXPECT_NE(payload.find("us-east"), std::string::npos);
}

TEST_F(SSEPayloadGroupingTest, TwoFieldsSameSeries) {
    StreamingBatch batch;
    batch.sequenceId = 0;

    for (const char* field : {"temperature", "humidity"}) {
        StreamingDataPoint pt;
        pt.measurement = "weather";
        pt.tags = {{"station", "s01"}};
        pt.field = field;
        pt.timestamp = 1000;
        pt.value = 20.0;
        batch.points.push_back(std::move(pt));
    }

    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));

    // Same (measurement, tags) -> single series entry, two field entries
    size_t measCount = 0;
    size_t pos = 0;
    while ((pos = payload.find("\"measurement\"", pos)) != std::string::npos) {
        ++measCount;
        ++pos;
    }
    EXPECT_EQ(measCount, 1u) << "Two fields from same (measurement, tags) should be in one series entry";
    EXPECT_NE(payload.find("\"temperature\""), std::string::npos);
    EXPECT_NE(payload.find("\"humidity\""), std::string::npos);
}

// ---------------------------------------------------------------------------
// queryResponseToBatches
// ---------------------------------------------------------------------------

class QueryResponseToBatchesTest : public ::testing::Test {};

TEST_F(QueryResponseToBatchesTest, EmptySeriesReturnEmptyBatches) {
    std::vector<SeriesResult> empty;
    auto batches = HttpStreamHandler::queryResponseToBatches(empty, "");
    EXPECT_TRUE(batches.empty());
}

TEST_F(QueryResponseToBatchesTest, SingleSeriesSingleFieldDoubleValues) {
    SeriesResult sr;
    sr.measurement = "cpu";
    sr.tags = {{"host", "srv1"}};

    std::vector<uint64_t> timestamps = {1000, 2000, 3000};
    std::vector<double> values = {80.0, 85.0, 90.0};
    sr.fields["usage"] = {timestamps, FieldValues{values}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].points.size(), 3u);

    EXPECT_EQ(batches[0].points[0].measurement, "cpu");
    EXPECT_EQ(batches[0].points[0].field, "usage");
    EXPECT_EQ(batches[0].points[0].timestamp, 1000u);
    EXPECT_DOUBLE_EQ(std::get<double>(batches[0].points[0].value), 80.0);

    EXPECT_EQ(batches[0].points[1].timestamp, 2000u);
    EXPECT_DOUBLE_EQ(std::get<double>(batches[0].points[1].value), 85.0);

    EXPECT_EQ(batches[0].points[2].timestamp, 3000u);
    EXPECT_DOUBLE_EQ(std::get<double>(batches[0].points[2].value), 90.0);
}

TEST_F(QueryResponseToBatchesTest, LabelIsPropagatedToBatch) {
    SeriesResult sr;
    sr.measurement = "cpu";
    sr.fields["usage"] = {{1000}, FieldValues{std::vector<double>{42.0}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "my_label");
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].label, "my_label");
}

TEST_F(QueryResponseToBatchesTest, EmptyLabelPropagated) {
    SeriesResult sr;
    sr.measurement = "cpu";
    sr.fields["usage"] = {{1000}, FieldValues{std::vector<double>{42.0}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_TRUE(batches[0].label.empty());
}

TEST_F(QueryResponseToBatchesTest, TagsArePropagatedToPoints) {
    SeriesResult sr;
    sr.measurement = "temperature";
    sr.tags = {{"location", "us-west"}, {"unit", "celsius"}};
    sr.fields["value"] = {{1000}, FieldValues{std::vector<double>{23.5}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].points.size(), 1u);
    EXPECT_EQ(batches[0].points[0].tags.at("location"), "us-west");
    EXPECT_EQ(batches[0].points[0].tags.at("unit"), "celsius");
}

TEST_F(QueryResponseToBatchesTest, BoolValuesConverted) {
    SeriesResult sr;
    sr.measurement = "switches";
    sr.fields["state"] = {{1000, 2000}, FieldValues{std::vector<bool>{true, false}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].points.size(), 2u);
    EXPECT_EQ(std::get<bool>(batches[0].points[0].value), true);
    EXPECT_EQ(std::get<bool>(batches[0].points[1].value), false);
}

TEST_F(QueryResponseToBatchesTest, StringValuesConverted) {
    SeriesResult sr;
    sr.measurement = "logs";
    sr.fields["message"] = {{1000}, FieldValues{std::vector<std::string>{"hello"}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].points.size(), 1u);
    EXPECT_EQ(std::get<std::string>(batches[0].points[0].value), "hello");
}

TEST_F(QueryResponseToBatchesTest, Int64ValuesConverted) {
    SeriesResult sr;
    sr.measurement = "counters";
    sr.fields["count"] = {{1000}, FieldValues{std::vector<int64_t>{42LL}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].points.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(batches[0].points[0].value), 42LL);
}

TEST_F(QueryResponseToBatchesTest, MultipleSeriesProduceSeparateBatches) {
    SeriesResult sr1;
    sr1.measurement = "cpu";
    sr1.tags = {{"host", "srv1"}};
    sr1.fields["usage"] = {{1000}, FieldValues{std::vector<double>{80.0}}};

    SeriesResult sr2;
    sr2.measurement = "memory";
    sr2.tags = {{"host", "srv2"}};
    sr2.fields["used"] = {{2000}, FieldValues{std::vector<double>{4096.0}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr1, sr2}, "lbl");
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(batches[0].points[0].measurement, "cpu");
    EXPECT_EQ(batches[1].points[0].measurement, "memory");
    // Both should inherit the label
    EXPECT_EQ(batches[0].label, "lbl");
    EXPECT_EQ(batches[1].label, "lbl");
}

TEST_F(QueryResponseToBatchesTest, SeriesWithNoFieldsProducesNoBatch) {
    // A SeriesResult with no fields should not produce a batch (no points)
    SeriesResult sr;
    sr.measurement = "empty";

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    EXPECT_TRUE(batches.empty()) << "A series with no fields should produce no batch";
}

TEST_F(QueryResponseToBatchesTest, MultipleFieldsInOneSeries) {
    SeriesResult sr;
    sr.measurement = "weather";
    sr.tags = {{"station", "s01"}};
    sr.fields["temperature"] = {{1000, 2000}, FieldValues{std::vector<double>{20.0, 21.0}}};
    sr.fields["humidity"] = {{1000, 2000}, FieldValues{std::vector<double>{60.0, 62.0}}};

    // Multiple fields are all folded into a single batch for the same series
    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    // 2 fields x 2 timestamps = 4 points total
    EXPECT_EQ(batches[0].points.size(), 4u);
}

// ---------------------------------------------------------------------------
// Round-trip: queryResponseToBatches -> formatSSEEvent
// ---------------------------------------------------------------------------

class SSERoundTripTest : public ::testing::Test {
protected:
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }
};

TEST_F(SSERoundTripTest, QueryResponseBecomesSSEEventWithCorrectStructure) {
    SeriesResult sr;
    sr.measurement = "temperature";
    sr.tags = {{"location", "us-west"}};
    sr.fields["value"] = {{1000000000ULL, 2000000000ULL}, FieldValues{std::vector<double>{23.5, 24.0}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "my_query");
    ASSERT_EQ(batches.size(), 1u);

    batches[0].sequenceId = 5;
    std::string event = HttpStreamHandler::formatSSEEvent(batches[0]);

    // SSE framing
    EXPECT_EQ(event.substr(0, 4), "id: ");
    EXPECT_NE(event.find("id: 5\n"), std::string::npos);
    EXPECT_NE(event.find("\nevent: data\n"), std::string::npos);
    EXPECT_EQ(event.substr(event.size() - 2), "\n\n");

    // JSON payload content
    std::string payload = extractPayload(event);
    EXPECT_NE(payload.find("\"label\":\"my_query\""), std::string::npos);
    EXPECT_NE(payload.find("\"measurement\":\"temperature\""), std::string::npos);
    EXPECT_NE(payload.find("\"location\":\"us-west\""), std::string::npos);
    EXPECT_NE(payload.find("\"value\""), std::string::npos);
    EXPECT_NE(payload.find("1000000000"), std::string::npos);
    EXPECT_NE(payload.find("2000000000"), std::string::npos);
}

TEST_F(SSERoundTripTest, BackfillRoundTrip) {
    SeriesResult sr;
    sr.measurement = "cpu";
    sr.tags = {{"host", "srv1"}};
    sr.fields["usage"] = {{500ULL}, FieldValues{std::vector<double>{75.5}}};

    auto batches = HttpStreamHandler::queryResponseToBatches({sr}, "");
    ASSERT_EQ(batches.size(), 1u);
    batches[0].sequenceId = 3;

    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batches[0]);
    EXPECT_NE(event.find("\nevent: backfill\n"), std::string::npos);
    EXPECT_NE(event.find("\"measurement\":\"cpu\""), std::string::npos);
    EXPECT_NE(event.find("\"host\":\"srv1\""), std::string::npos);
}

// ---------------------------------------------------------------------------
// Edge cases: empty batch
// ---------------------------------------------------------------------------

class SSEEdgeCasesTest : public ::testing::Test {
protected:
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }
};

TEST_F(SSEEdgeCasesTest, EmptyBatchProducesEmptySeriesArray) {
    StreamingBatch empty;
    empty.sequenceId = 0;

    std::string event = HttpStreamHandler::formatSSEEvent(empty);
    std::string payload = extractPayload(event);

    // Outer JSON must still be valid: {"series":[]}
    EXPECT_NE(payload.find("\"series\":[]"), std::string::npos) << "Empty batch should produce {\"series\":[]}";
}

TEST_F(SSEEdgeCasesTest, EmptyBatchBackfillProducesEmptySeriesArray) {
    StreamingBatch empty;
    empty.sequenceId = 1;

    std::string event = HttpStreamHandler::formatSSEBackfillEvent(empty);
    std::string payload = extractPayload(event);
    EXPECT_NE(payload.find("\"series\":[]"), std::string::npos);
}

TEST_F(SSEEdgeCasesTest, SequenceIdZeroIsIncluded) {
    auto batch = makeBatch("cpu", {}, "v", 1000, 1.0, 0);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("id: 0\n"), std::string::npos) << "Sequence id 0 must appear (not be omitted)";
}

TEST_F(SSEEdgeCasesTest, LargeSequenceIdFormattedCorrectly) {
    auto batch = makeBatch("cpu", {}, "v", 1000, 1.0, UINT64_MAX);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);
    EXPECT_NE(event.find("id: 18446744073709551615\n"), std::string::npos)
        << "UINT64_MAX sequence id must be formatted correctly";
}

TEST_F(SSEEdgeCasesTest, LargeTimestampFormattedCorrectly) {
    // Nanosecond timestamp for year ~2030
    uint64_t tsNs = 1893456000000000000ULL;
    auto batch = makeBatch("cpu", {}, "v", tsNs, 1.0, 0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("1893456000000000000"), std::string::npos)
        << "Large nanosecond timestamp must be formatted without truncation";
}

TEST_F(SSEEdgeCasesTest, ZeroTimestamp) {
    auto batch = makeBatch("cpu", {}, "v", 0ULL, 1.0, 0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"timestamps\":[0]"), std::string::npos) << "Timestamp 0 must appear as 0 in the JSON";
}

TEST_F(SSEEdgeCasesTest, NegativeDoubleValue) {
    auto batch = makeBatch("temp", {}, "val", 1000, -273.15, 0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("-273.15"), std::string::npos) << "Negative double must be serialized correctly";
}

TEST_F(SSEEdgeCasesTest, ZeroDoubleValue) {
    auto batch = makeBatch("temp", {}, "val", 1000, 0.0, 0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("\"values\":[0"), std::string::npos)
        << "Zero double must produce a value (0 or 0.0) in the JSON";
}

TEST_F(SSEEdgeCasesTest, UnicodeInTagValueIsPassedThrough) {
    // Non-ASCII bytes (valid UTF-8) should pass through unchanged
    auto batch = makeBatch("sensor", {{"region", "東京"}}, "temp", 1000, 25.0);
    std::string payload = extractPayload(HttpStreamHandler::formatSSEEvent(batch));
    EXPECT_NE(payload.find("東京"), std::string::npos) << "Valid UTF-8 non-ASCII characters must not be escaped";
}

// ---------------------------------------------------------------------------
// applyFormulaToBatch
// ---------------------------------------------------------------------------

class ApplyFormulaTest : public ::testing::Test {
protected:
    static std::string extractPayload(const std::string& event) {
        const std::string prefix = "\ndata: ";
        auto pos = event.find(prefix);
        if (pos == std::string::npos)
            return "";
        pos += prefix.size();
        auto end = event.rfind("\n\n");
        if (end == std::string::npos)
            return event.substr(pos);
        return event.substr(pos, end - pos);
    }

    // Build a batch that has double values for formula evaluation
    StreamingBatch makeDoubleBatch(const std::string& measurement, const std::string& field,
                                   const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                                   uint64_t seqId = 0) {
        StreamingBatch batch;
        batch.sequenceId = seqId;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            StreamingDataPoint pt;
            pt.measurement = measurement;
            pt.field = field;
            pt.timestamp = timestamps[i];
            pt.value = values[i];
            batch.points.push_back(std::move(pt));
        }
        return batch;
    }
};

TEST_F(ApplyFormulaTest, IdentityFormulaPreservesPoints) {
    // Formula "a" (just the query reference itself) should preserve points unchanged
    ExpressionParser parser("a");
    auto ast = parser.parse();

    auto batch = makeDoubleBatch("cpu", "usage", {1000, 2000}, {50.0, 60.0});
    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");

    ASSERT_EQ(result.points.size(), 2u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 50.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 60.0);
}

TEST_F(ApplyFormulaTest, ScalarMultiplyFormula) {
    // Formula "a * 2" should double every value
    ExpressionParser parser("a * 2");
    auto ast = parser.parse();

    auto batch = makeDoubleBatch("cpu", "usage", {1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");

    ASSERT_EQ(result.points.size(), 3u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 20.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[1].value), 40.0);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[2].value), 60.0);
}

TEST_F(ApplyFormulaTest, ScalarAddFormula) {
    ExpressionParser parser("a + 100");
    auto ast = parser.parse();

    auto batch = makeDoubleBatch("temp", "val", {1000}, {23.5});
    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");

    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 123.5);
}

TEST_F(ApplyFormulaTest, MetadataPreservedAfterFormula) {
    // measurement, tags, field, timestamp must be preserved after formula application
    ExpressionParser parser("a * 1");
    auto ast = parser.parse();

    StreamingBatch batch;
    batch.sequenceId = 7;
    batch.label = "test_label";
    StreamingDataPoint pt;
    pt.measurement = "weather";
    pt.tags = {{"station", "s01"}};
    pt.field = "temperature";
    pt.timestamp = 999000ULL;
    pt.value = 25.0;
    batch.points.push_back(std::move(pt));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");

    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_EQ(result.points[0].measurement, "weather");
    EXPECT_EQ(result.points[0].tags.at("station"), "s01");
    EXPECT_EQ(result.points[0].field, "temperature");
    EXPECT_EQ(result.points[0].timestamp, 999000ULL);
    EXPECT_EQ(result.label, "test_label");
    EXPECT_EQ(result.sequenceId, 7u);
}

TEST_F(ApplyFormulaTest, EmptyBatchReturnsEmptyResult) {
    ExpressionParser parser("a * 2");
    auto ast = parser.parse();

    StreamingBatch empty;
    auto result = HttpStreamHandler::applyFormulaToBatch(empty, *ast, "a");
    EXPECT_TRUE(result.points.empty());
}

TEST_F(ApplyFormulaTest, BoolValuesConvertedToDoubleForFormula) {
    // Bool values: true->1.0, false->0.0
    ExpressionParser parser("a * 10");
    auto ast = parser.parse();

    StreamingBatch batch;
    StreamingDataPoint pt;
    pt.measurement = "switch";
    pt.field = "state";
    pt.timestamp = 1000;
    pt.value = true;
    batch.points.push_back(std::move(pt));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");
    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 10.0);
}

TEST_F(ApplyFormulaTest, Int64ValuesConvertedToDoubleForFormula) {
    ExpressionParser parser("a + 0.5");
    auto ast = parser.parse();

    StreamingBatch batch;
    StreamingDataPoint pt;
    pt.measurement = "counter";
    pt.field = "count";
    pt.timestamp = 1000;
    pt.value = int64_t(100LL);
    batch.points.push_back(std::move(pt));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");
    ASSERT_EQ(result.points.size(), 1u);
    EXPECT_DOUBLE_EQ(std::get<double>(result.points[0].value), 100.5);
}

TEST_F(ApplyFormulaTest, MultipleSeriesEvaluatedIndependently) {
    // Two (measurement, tags, field) combos in one batch — each gets its own evaluation
    ExpressionParser parser("a * 2");
    auto ast = parser.parse();

    StreamingBatch batch;

    StreamingDataPoint pt1;
    pt1.measurement = "cpu";
    pt1.tags = {{"host", "srv1"}};
    pt1.field = "usage";
    pt1.timestamp = 1000;
    pt1.value = 50.0;
    batch.points.push_back(std::move(pt1));

    StreamingDataPoint pt2;
    pt2.measurement = "cpu";
    pt2.tags = {{"host", "srv2"}};
    pt2.field = "usage";
    pt2.timestamp = 1000;
    pt2.value = 75.0;
    batch.points.push_back(std::move(pt2));

    auto result = HttpStreamHandler::applyFormulaToBatch(batch, *ast, "a");
    ASSERT_EQ(result.points.size(), 2u);

    // Values should be doubled regardless of which point is which
    std::vector<double> resultVals;
    for (const auto& p : result.points) {
        resultVals.push_back(std::get<double>(p.value));
    }
    std::sort(resultVals.begin(), resultVals.end());
    EXPECT_DOUBLE_EQ(resultVals[0], 100.0);
    EXPECT_DOUBLE_EQ(resultVals[1], 150.0);
}

// ---------------------------------------------------------------------------
// SSE format structural invariants
// ---------------------------------------------------------------------------

class SSEStructuralTest : public ::testing::Test {};

TEST_F(SSEStructuralTest, EventFormatFollowsSSESpec) {
    // SSE spec: each field is "field: value\n", event ends with "\n"
    // So the full event must end with "\n\n"
    auto batch = makeBatch("cpu", {}, "v", 1000, 1.0, 1);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);

    // Must have id line
    EXPECT_NE(event.find("id: "), std::string::npos);
    // Must have event line
    EXPECT_NE(event.find("event: "), std::string::npos);
    // Must have data line
    EXPECT_NE(event.find("data: "), std::string::npos);
    // Must end with \n\n
    ASSERT_GE(event.size(), 2u);
    EXPECT_EQ(event[event.size() - 1], '\n');
    EXPECT_EQ(event[event.size() - 2], '\n');
}

TEST_F(SSEStructuralTest, BackfillEventFormatFollowsSSESpec) {
    auto batch = makeBatch("cpu", {}, "v", 1000, 1.0, 2);
    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batch);

    EXPECT_NE(event.find("id: "), std::string::npos);
    EXPECT_NE(event.find("event: "), std::string::npos);
    EXPECT_NE(event.find("data: "), std::string::npos);
    ASSERT_GE(event.size(), 2u);
    EXPECT_EQ(event[event.size() - 1], '\n');
    EXPECT_EQ(event[event.size() - 2], '\n');
}

TEST_F(SSEStructuralTest, DataLineContainsValidJSONObject) {
    auto batch = makeBatch("m", {}, "f", 1000, 1.0, 0);
    std::string event = HttpStreamHandler::formatSSEEvent(batch);

    const std::string prefix = "\ndata: ";
    auto pos = event.find(prefix);
    ASSERT_NE(pos, std::string::npos);
    pos += prefix.size();
    auto endPos = event.rfind("\n\n");
    ASSERT_NE(endPos, std::string::npos);

    std::string json = event.substr(pos, endPos - pos);
    EXPECT_EQ(json.front(), '{') << "data line must start with '{'";
    EXPECT_EQ(json.back(), '}') << "data line must end with '}'";
}

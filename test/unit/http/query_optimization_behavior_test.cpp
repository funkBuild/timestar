#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../test_helpers.hpp"
#include "request_body.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;

class QueryOptimizationBehaviorTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }

    static void write(HttpWriteHandler& handler, const std::string& body) {
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Content-Type"] = "application/json";
        timestar::http::injectedBody(*req) = body;
        auto reply = handler.handleWrite(std::move(req)).get();
        ASSERT_EQ(reply->_status, seastar::http::reply::status_type::ok) << reply->_content;
    }

    static QueryResponse query(HttpQueryHandler& handler, const std::string& text) {
        auto request = QueryParser::parseQueryString(text);
        request.startTime = 0;
        request.endTime = UINT64_MAX;
        auto response = handler.executeQuery(std::move(request)).get();
        EXPECT_TRUE(response.success) << response.errorMessage;
        return response;
    }
};

TEST_F(QueryOptimizationBehaviorTest, GroupedRawMergeAndDiscoveryInvalidation) {
    seastar::thread([] {
        ScopedShardedEngine engine;
        engine.start();
        HttpWriteHandler writer(&engine.eng);
        HttpQueryHandler reader(&engine.eng);
        write(
            writer,
            R"({"measurement":"opt_query","tags":{"host":"a","rack":"r"},"timestamps":[1,2],"fields":{"v":[1.0,2.0]}})");
        write(
            writer,
            R"({"measurement":"opt_query","tags":{"host":"b","rack":"r"},"timestamps":[2,3],"fields":{"v":[6.0,9.0]}})");
        for (int repeat = 0; repeat < 2; ++repeat) {
            auto response = query(reader, "avg:opt_query(v) by {rack}");
            ASSERT_EQ(response.series.size(), 1u);
            const auto& field = response.series[0].fields.at("v");
            EXPECT_EQ(field.first, (std::vector<uint64_t>{1, 2, 3}));
            EXPECT_EQ(std::get<std::vector<double>>(field.second), (std::vector<double>{1, 4, 9}));
        }
        auto count = query(reader, "count:opt_query(v) by {rack}");
        ASSERT_EQ(count.series.size(), 1u);
        EXPECT_EQ(std::get<std::vector<double>>(count.series[0].fields.at("v").second), (std::vector<double>{1, 2, 1}));

        // Invalidate a previously cached discovery snapshot and populate a
        // second group. Existing metadata must remain owned until readers finish.
        write(writer,
              R"({"measurement":"opt_query","tags":{"host":"c","rack":"s"},"timestamps":[4],"fields":{"v":[10.0]}})");
        auto expanded = query(reader, "avg:opt_query(v) by {rack}");
        EXPECT_EQ(expanded.series.size(), 2u);
        auto unknown = query(reader, "avg:opt_query(v) by {misspelled_tag}");
        EXPECT_TRUE(unknown.series.empty());
        auto pattern = query(reader, "avg:opt_query(v){host:a*} by {rack}");
        ASSERT_EQ(pattern.series.size(), 1u);
        EXPECT_EQ(pattern.series[0].fields.at("v").first, (std::vector<uint64_t>{1, 2}));
    })
        .join()
        .get();
}

TEST_F(QueryOptimizationBehaviorTest, LargeZeroFloatArraysAndExactIntegerTokens) {
    seastar::thread([] {
        ScopedShardedEngine engine;
        engine.start();
        HttpWriteHandler writer(&engine.eng);
        HttpQueryHandler reader(&engine.eng);
        std::string timestamps, values;
        for (int i = 0; i < 100; ++i) {
            if (i) {
                timestamps += ',';
                values += ',';
            }
            timestamps += std::to_string(i + 1);
            values += i == 0 ? "0.0" : "1.5";
        }
        write(writer, "{\"measurement\":\"opt_float\",\"timestamps\":[" + timestamps + "],\"fields\":{\"v\":[" +
                          values + "]}}");
        auto floats = query(reader, "avg:opt_float(v)");
        ASSERT_EQ(floats.series.size(), 1u);
        const auto& result = std::get<std::vector<double>>(floats.series[0].fields.at("v").second);
        ASSERT_EQ(result.size(), 100u);
        EXPECT_DOUBLE_EQ(result.front(), 0);
        EXPECT_DOUBLE_EQ(result.back(), 1.5);

        // Padding ensures this request attempts the same >256-byte fast path.
        const std::string padding(300, 'x');
        write(writer, "{\"measurement\":\"opt_int\",\"tags\":{\"padding\":\"" + padding +
                          "\"},\"timestamps\":[1],\"fields\":{\"v\":[9007199254740993]}}");
        const auto id = SeriesId128::fromSeriesKey("opt_int,padding=" + padding + " v");
        const auto shard = routeToCore(id);
        auto raw = engine.eng.invoke_on(shard, [id](Engine& e) { return e.query("opt_int", id, 0, UINT64_MAX); }).get();
        ASSERT_TRUE(raw.has_value());
        ASSERT_TRUE(std::holds_alternative<QueryResult<int64_t>>(*raw));
        EXPECT_EQ(std::get<QueryResult<int64_t>>(*raw).values.front(), 9007199254740993LL);
    })
        .join()
        .get();
}

TEST_F(QueryOptimizationBehaviorTest, ShortAlignedGroupsReduceAcrossOwningShards) {
    seastar::thread([] {
        ScopedShardedEngine engine;
        engine.start();
        HttpWriteHandler writer(&engine.eng);
        HttpQueryHandler reader(&engine.eng);
        for (size_t i = 0; i < 128; ++i) {
            const std::string value = std::to_string(static_cast<double>(i));
            write(writer, "{\"measurement\":\"aligned_shards\",\"tags\":{\"host\":\"h" + std::to_string(i) +
                              "\",\"rack\":\"r\"},\"timestamps\":[10,20,30],\"fields\":{\"v\":[" + value + "," + value +
                              "," + value + "]}}");
        }
        for (const auto& [method, expected] :
             std::vector<std::pair<std::string, double>>{{"avg", 63.5},
                                                         {"count", 128},
                                                         {"sum", 8128},
                                                         {"min", 0},
                                                         {"max", 127},
                                                         {"spread", 127},
                                                         {"stdvar", 1365.25},
                                                         {"stddev", std::sqrt(1365.25)}}) {
            auto response = query(reader, method + ":aligned_shards(v) by {rack}");
            ASSERT_EQ(response.series.size(), 1u);
            const auto& field = response.series[0].fields.at("v");
            EXPECT_EQ(field.first, (std::vector<uint64_t>{10, 20, 30}));
            const auto& values = std::get<std::vector<double>>(field.second);
            ASSERT_EQ(values.size(), 3u);
            for (double value : values)
                EXPECT_NEAR(value, expected, 1e-10) << method;
        }
    })
        .join()
        .get();
}

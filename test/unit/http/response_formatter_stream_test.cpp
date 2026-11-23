// ResponseFormatter::streamBody() writes the same JSON as formatBody(), but in
// segments handed to an output stream as they fill instead of one string. The
// segment boundaries are an implementation detail the client never sees, so the
// contract is simple and total: for every response, the concatenated stream is
// byte-for-byte formatBody()'s output — and a large one really is delivered in
// pieces rather than assembled whole.

#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/response_formatter.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>
#include <random>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <span>
#include <string>
#include <vector>

using namespace timestar;
using timestar::http::ResponseFormatter;

namespace {

struct Collected {
    std::string bytes;
    std::vector<size_t> writes;  // size of each buffer the sink was handed
};

class CollectingSink final : public seastar::data_sink_impl {
public:
    explicit CollectingSink(Collected& out) : out_(out) {}

    seastar::future<> put(std::span<seastar::temporary_buffer<char>> data) override {
        for (auto& buf : data) {
            out_.bytes.append(buf.get(), buf.size());
            out_.writes.push_back(buf.size());
        }
        return seastar::make_ready_future<>();
    }
    seastar::future<> close() override { return seastar::make_ready_future<>(); }

private:
    Collected& out_;
};

Collected streamed(QueryResponse& response) {
    Collected collected;
    seastar::output_stream<char> out(seastar::data_sink(std::make_unique<CollectingSink>(collected)), 8192);
    ResponseFormatter::streamBody(response, out).get();
    out.close().get();
    return collected;
}

std::string buffered(QueryResponse& response) {
    const auto body = ResponseFormatter::formatBody(response).get();
    return std::string(body.data(), body.size());
}

std::vector<uint64_t> timestamps(size_t n, uint64_t start = 1'700'000'000'000'000'000ULL) {
    std::vector<uint64_t> ts(n);
    for (size_t i = 0; i < n; ++i) {
        ts[i] = start + i * 1'000'000'000ULL;
    }
    return ts;
}

SeriesResult doubleSeries(const std::string& measurement, size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    SeriesResult s;
    s.measurement = measurement;
    s.tags = {{"host", "h" + std::to_string(seed)}, {"rack", "r1"}};
    std::vector<double> values(n);
    for (size_t i = 0; i < n; ++i) {
        switch (rng() % 6) {
            case 0:
                values[i] = static_cast<double>(static_cast<int64_t>(rng() % 2000000) - 1000000) / 1000.0;
                break;
            case 1:
                values[i] = std::bit_cast<double>(rng());  // any bit pattern, NaN/Inf included
                break;
            case 2:
                values[i] = -1.5131402663111935e-222;  // the longest text a double produces
                break;
            default:
                values[i] = static_cast<double>(rng() % 100000) * 0.25;
        }
    }
    s.fields["value"] = std::make_pair(timestamps(n), FieldValues(std::move(values)));
    return s;
}

QueryResponse responseOf(std::vector<SeriesResult> series) {
    QueryResponse r;
    r.success = true;
    size_t points = 0;
    for (const auto& s : series) {
        for (const auto& [name, data] : s.fields) {
            points += data.first.size();
        }
    }
    r.series = std::move(series);
    r.statistics.seriesCount = r.series.size();
    r.statistics.pointCount = points;
    r.statistics.executionTimeMs = 12.345;
    return r;
}

// Build the same response twice: formatting takes it by non-const reference.
template <class Make>
void expectSameBytes(Make make, const char* what) {
    QueryResponse a = make();
    QueryResponse b = make();
    const std::string whole = buffered(a);
    const Collected pieces = streamed(b);
    ASSERT_EQ(pieces.bytes.size(), whole.size()) << what;
    ASSERT_TRUE(pieces.bytes == whole) << what;
}

}  // namespace

TEST(ResponseFormatterStreamTest, EmptyAndTinyResponsesMatch) {
    expectSameBytes([] { return responseOf({}); }, "no series");
    expectSameBytes([] { return responseOf({doubleSeries("m", 0, 1)}); }, "empty field");
    expectSameBytes([] { return responseOf({doubleSeries("m", 1, 1)}); }, "one point");
    expectSameBytes([] { return responseOf({doubleSeries("m", 7, 1), doubleSeries("m", 3, 2)}); }, "two series");
}

TEST(ResponseFormatterStreamTest, DoubleSeriesMatchAtEverySliceAndSegmentBoundary) {
    // 8192 is the slice size; ~26k timestamps or ~21k longest doubles fill a segment.
    for (size_t n : {size_t{8191}, size_t{8192}, size_t{8193}, size_t{16384}, size_t{21000}, size_t{26214},
                     size_t{26215}, size_t{65536}, size_t{70001}, size_t{300000}}) {
        expectSameBytes([n] { return responseOf({doubleSeries("metric", n, n)}); }, std::to_string(n).c_str());
    }
}

TEST(ResponseFormatterStreamTest, EveryValueTypeMatches) {
    auto make = [] {
        const size_t n = 40000;
        SeriesResult s;
        s.measurement = "mixed \"quoted\" \\ name\n";
        s.tags = {{"k\"ey", "va\tlue"}, {"plain", "tag"}};
        std::vector<int64_t> ints(n);
        std::vector<bool> bools(n);
        std::vector<std::string> strings(n);
        std::vector<double> doubles(n);
        for (size_t i = 0; i < n; ++i) {
            ints[i] = (i % 3 == 0) ? std::numeric_limits<int64_t>::min() : static_cast<int64_t>(i) - 20000;
            bools[i] = (i % 5) < 2;
            strings[i] = (i % 7 == 0) ? std::string("needs \"escaping\"\n\x01") : "state_" + std::to_string(i % 13);
            doubles[i] = (i % 11 == 0) ? std::numeric_limits<double>::quiet_NaN() : static_cast<double>(i) / 8.0;
        }
        s.fields["i"] = std::make_pair(timestamps(n), FieldValues(std::move(ints)));
        s.fields["b"] = std::make_pair(timestamps(n), FieldValues(std::move(bools)));
        s.fields["s"] = std::make_pair(timestamps(n), FieldValues(std::move(strings)));
        s.fields["d"] = std::make_pair(timestamps(n), FieldValues(std::move(doubles)));
        return responseOf({std::move(s)});
    };
    expectSameBytes(make, "int/bool/string/double");
}

TEST(ResponseFormatterStreamTest, StringsLargerThanASegmentMatch) {
    auto make = [] {
        SeriesResult s;
        s.measurement = "blobs";
        std::vector<std::string> values;
        values.emplace_back(700 * 1024, 'a');  // longer than a whole segment
        values.emplace_back(300 * 1024, '"');  // every byte doubles when escaped
        values.emplace_back("short");
        values.emplace_back(1'200'000, '\x02');  // every byte becomes \u0002
        s.fields["payload"] = std::make_pair(timestamps(values.size()), FieldValues(std::move(values)));
        return responseOf({std::move(s)});
    };
    expectSameBytes(make, "oversized strings");
}

TEST(ResponseFormatterStreamTest, ManySmallSeriesMatch) {
    // No array ever reaches a slice boundary; segments fill from metadata and
    // short arrays alone.
    auto make = [] {
        std::vector<SeriesResult> all;
        for (size_t i = 0; i < 6000; ++i) {
            all.push_back(doubleSeries("fleet.metric.with.a.long.name", 12, i));
        }
        return responseOf(std::move(all));
    };
    expectSameBytes(make, "6000 series x 12 points");
}

TEST(ResponseFormatterStreamTest, LargeBodyIsDeliveredInBoundedPieces) {
    QueryResponse response = responseOf({doubleSeries("m", 1'000'000, 3)});
    const Collected pieces = streamed(response);

    ASSERT_GT(pieces.bytes.size(), size_t{20} * 1024 * 1024);
    // Delivered as many buffers, none anywhere near the whole body: this is
    // what keeps a large result from being held in memory at once.
    size_t largest = 0;
    for (size_t w : pieces.writes) {
        largest = std::max(largest, w);
    }
    EXPECT_GT(pieces.writes.size(), size_t{30});
    EXPECT_LE(largest, size_t{600} * 1024);
    EXPECT_EQ(pieces.bytes.front(), '{');
    EXPECT_EQ(pieces.bytes.back(), '}');
}

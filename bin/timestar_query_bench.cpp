/**
 * timestar_query_bench — HTTP query benchmark for TimeStar.
 *
 * Exercises a wide range of query patterns for profiling:
 *   - Raw point retrieval (no aggregation)
 *   - Aggregation methods: avg, min, max, sum, count, latest, median, stddev
 *   - Tag-filtered queries (scopes)
 *   - Group-by queries
 *   - Time-bucketed aggregation (various intervals)
 *   - Single-field vs all-fields
 *   - Narrow vs wide time ranges
 *   - Metadata queries (/measurements, /tags, /fields)
 *
 * Prerequisite: the server must already contain data.  Run the insert
 * benchmark first:
 *   ./timestar_insert_bench -c 4 --batches 100 --batch-size 10000 \
 *       --warmup 10 --connections 8 --hosts 10 --racks 2
 *
 * Usage:
 *   ./timestar_query_bench --server-host 127.0.0.1 --server-port 8086
 */

#include <boost/range/irange.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/inet_address.hh>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fmt/core.h>
#include <fmt/format.h>
#include <numeric>
#include <string>
#include <vector>

using namespace seastar;
using clk = std::chrono::steady_clock;

// Time range matching the insert benchmark's data.
// The insert bench uses BASE_TS = 1'000'000'000'000'000'000 (~2001-09-09)
// with MINUTE_NS spacing per batch of 10K timestamps.
static constexpr uint64_t BASE_TS    = 1'000'000'000'000'000'000ULL;
static constexpr uint64_t MINUTE_NS  = 60'000'000'000ULL;
// For 100 batches * 10K timestamps * 1 minute spacing:
// end = BASE_TS + 100 * 10000 * MINUTE_NS
static constexpr uint64_t END_TS     = BASE_TS + 100ULL * 10000ULL * MINUTE_NS;

// Narrower time ranges for focused queries
static constexpr uint64_t NARROW_START = BASE_TS;
static constexpr uint64_t NARROW_END   = BASE_TS + 100ULL * MINUTE_NS;  // 100 minutes
static constexpr uint64_t MED_START    = BASE_TS;
static constexpr uint64_t MED_END      = BASE_TS + 10000ULL * MINUTE_NS; // ~7 days

// ──────────────────────────────────────────────────────────────────────
// Query definition
// ──────────────────────────────────────────────────────────────────────

struct QueryDef {
    std::string name;         // human label
    std::string method;       // HTTP method (GET or POST)
    std::string path;         // URL path
    std::string body;         // request body (empty for GET)
    int         iterations;   // how many times to run
};

static std::vector<QueryDef> buildQuerySuite() {
    std::vector<QueryDef> qs;

    // Helper to build POST /query JSON
    auto q = [](const std::string& query, uint64_t start, uint64_t end,
                const std::string& interval = "") -> std::string {
        std::string json = fmt::format(
            R"({{"query":"{}","startTime":{},"endTime":{}}})", query, start, end);
        if (!interval.empty()) {
            // Insert aggregationInterval before closing brace
            json.pop_back(); // remove '}'
            json += fmt::format(R"(,"aggregationInterval":"{}"}})", interval);
        }
        return json;
    };

    // ── 1. Raw / Latest point queries ───────────────────────────────

    qs.push_back({"latest: single field",
        "POST", "/query",
        q("latest:server.metrics(cpu_usage)", BASE_TS, END_TS),
        20});

    qs.push_back({"latest: all fields",
        "POST", "/query",
        q("latest:server.metrics()", BASE_TS, END_TS),
        20});

    qs.push_back({"latest: single field, tag filter",
        "POST", "/query",
        q("latest:server.metrics(cpu_usage){host:host-01}", BASE_TS, END_TS),
        20});

    qs.push_back({"latest: all fields, two tag filters",
        "POST", "/query",
        q("latest:server.metrics(){host:host-01,rack:rack-1}", BASE_TS, END_TS),
        20});

    // ── 2. Aggregation methods (narrow range) ───────────────────────

    qs.push_back({"avg: narrow range, single field",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        20});

    qs.push_back({"min: narrow range, single field",
        "POST", "/query",
        q("min:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        20});

    qs.push_back({"max: narrow range, single field",
        "POST", "/query",
        q("max:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        20});

    qs.push_back({"sum: narrow range, single field",
        "POST", "/query",
        q("sum:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        20});

    qs.push_back({"count: narrow range, single field",
        "POST", "/query",
        q("count:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        20});

    qs.push_back({"median: narrow range, single field",
        "POST", "/query",
        q("median:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        10});

    qs.push_back({"stddev: narrow range, single field",
        "POST", "/query",
        q("stddev:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        10});

    // ── 3. Field cardinality ────────────────────────────────────────

    qs.push_back({"avg: narrow, all 10 fields",
        "POST", "/query",
        q("avg:server.metrics()", NARROW_START, NARROW_END),
        10});

    qs.push_back({"avg: narrow, 3 fields",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage,memory_usage,temperature)", NARROW_START, NARROW_END),
        10});

    // ── 4. Time range scaling ───────────────────────────────────────

    qs.push_back({"avg: medium range (~7d), single field",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", MED_START, MED_END),
        10});

    qs.push_back({"avg: full range, single field",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", BASE_TS, END_TS),
        5});

    qs.push_back({"avg: full range, all fields",
        "POST", "/query",
        q("avg:server.metrics()", BASE_TS, END_TS),
        5});

    // ── 5. Tag-filtered queries (scopes) ────────────────────────────

    qs.push_back({"avg: single host filter",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage){host:host-01}", NARROW_START, NARROW_END),
        20});

    qs.push_back({"avg: host + rack filter",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage){host:host-01,rack:rack-1}", NARROW_START, NARROW_END),
        20});

    qs.push_back({"avg: rack filter, all fields",
        "POST", "/query",
        q("avg:server.metrics(){rack:rack-2}", NARROW_START, NARROW_END),
        10});

    // ── 6. Group-by queries ─────────────────────────────────────────

    qs.push_back({"avg: group by host",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage) by {host}", NARROW_START, NARROW_END),
        10});

    qs.push_back({"avg: group by rack",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage) by {rack}", NARROW_START, NARROW_END),
        10});

    qs.push_back({"avg: group by host+rack",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage) by {host,rack}", NARROW_START, NARROW_END),
        10});

    qs.push_back({"avg: group by host, full range",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage) by {host}", BASE_TS, END_TS),
        5});

    // ── 7. Time-bucketed aggregation ────────────────────────────────

    qs.push_back({"avg: 1h buckets, medium range",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", MED_START, MED_END, "1h"),
        10});

    qs.push_back({"avg: 5m buckets, narrow range",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", NARROW_START, NARROW_END, "5m"),
        20});

    qs.push_back({"avg: 1d buckets, full range",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", BASE_TS, END_TS, "1d"),
        5});

    qs.push_back({"max: 1h buckets, tag filter",
        "POST", "/query",
        q("max:server.metrics(cpu_usage){host:host-05}", MED_START, MED_END, "1h"),
        10});

    qs.push_back({"avg: 1h buckets, group by host",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage) by {host}", MED_START, MED_END, "1h"),
        5});

    qs.push_back({"avg: 5m buckets, all fields, tag filter",
        "POST", "/query",
        q("avg:server.metrics(){rack:rack-1}", MED_START, MED_END, "5m"),
        5});

    // ── 8. Metadata queries ─────────────────────────────────────────

    qs.push_back({"GET /measurements",
        "GET", "/measurements", "", 20});

    qs.push_back({"GET /tags?measurement=server.metrics",
        "GET", "/tags?measurement=server.metrics", "", 20});

    qs.push_back({"GET /fields?measurement=server.metrics",
        "GET", "/fields?measurement=server.metrics", "", 20});

    // ── 9. Stress patterns ──────────────────────────────────────────

    qs.push_back({"avg: narrow, single field (repeated)",
        "POST", "/query",
        q("avg:server.metrics(cpu_usage)", NARROW_START, NARROW_END),
        50});

    qs.push_back({"latest: tag filter (repeated)",
        "POST", "/query",
        q("latest:server.metrics(temperature){host:host-03}", BASE_TS, END_TS),
        50});

    return qs;
}

// ──────────────────────────────────────────────────────────────────────
// Per-query result
// ──────────────────────────────────────────────────────────────────────

struct QueryResult {
    std::string name;
    int         iterations    = 0;
    int         successes     = 0;
    int         failures      = 0;
    double      total_ms      = 0.0;
    double      min_ms        = 1e18;
    double      max_ms        = 0.0;
    std::vector<double> latencies_ms;
    size_t      response_bytes = 0;   // from first successful response
    int         point_count    = -1;  // parsed from response, -1 if unknown
    int         series_count   = -1;
    std::string first_error;
};

// ──────────────────────────────────────────────────────────────────────
// Extract statistics from response body
// ──────────────────────────────────────────────────────────────────────

static int extractJsonInt(std::string_view body, std::string_view key) {
    auto pos = body.find(key);
    if (pos == std::string_view::npos) return -1;
    auto numStart = pos + key.size();
    // skip ':' and whitespace
    while (numStart < body.size() && (body[numStart] == ':' || body[numStart] == ' '))
        ++numStart;
    auto numEnd = body.find_first_of(",}", numStart);
    if (numEnd == std::string_view::npos) return -1;
    try {
        return std::stoi(std::string(body.substr(numStart, numEnd - numStart)));
    } catch (...) {
        return -1;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Run a single query definition
// ──────────────────────────────────────────────────────────────────────

static future<QueryResult>
runQuery(socket_address addr, const QueryDef& def) {
    using http_client = http::experimental::client;

    auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
    auto client  = std::make_unique<http_client>(std::move(factory), 1);

    QueryResult result;
    result.name       = def.name;
    result.iterations = def.iterations;

    for (int i = 0; i < def.iterations; ++i) {
        auto req = http::request::make(
            sstring(def.method), sstring("localhost"), sstring(def.path));
        if (!def.body.empty()) {
            req.write_body("json", sstring(def.body));
        }

        auto t0 = clk::now();
        bool ok = false;
        sstring respBody;

        try {
            co_await client->make_request(
                std::move(req),
                [&ok, &respBody](const http::reply& rep,
                                 input_stream<char>&& body_in) -> future<> {
                    auto body = std::move(body_in);
                    ok = (static_cast<int>(rep._status) >= 200 &&
                          static_cast<int>(rep._status) < 300);
                    sstring acc;
                    auto buf = co_await body.read();
                    while (!buf.empty()) {
                        acc.append(buf.get(), buf.size());
                        buf = co_await body.read();
                    }
                    respBody = std::move(acc);
                });
        } catch (const std::exception& e) {
            if (result.first_error.empty()) {
                result.first_error = fmt::format("Exception: {}", e.what());
            }
            result.failures++;
            continue;
        }

        auto t1 = clk::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if (ok) {
            result.successes++;
            result.total_ms += ms;
            result.min_ms = std::min(result.min_ms, ms);
            result.max_ms = std::max(result.max_ms, ms);
            result.latencies_ms.push_back(ms);

            // Parse stats from first success
            if (result.response_bytes == 0) {
                result.response_bytes = respBody.size();
                auto sv = std::string_view(respBody.data(), respBody.size());
                result.point_count  = extractJsonInt(sv, "\"point_count\"");
                result.series_count = extractJsonInt(sv, "\"series_count\"");
            }
        } else {
            result.failures++;
            if (result.first_error.empty()) {
                result.first_error = std::string(
                    respBody.data(),
                    std::min(respBody.size(), size_t(256)));
            }
        }
    }

    co_await client->close();

    if (result.successes == 0) {
        result.min_ms = 0;
    }
    co_return result;
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    app_template app;

    namespace bpo = boost::program_options;
    app.add_options()
        ("server-host", bpo::value<std::string>()->default_value("127.0.0.1"),
         "TimeStar server host")
        ("server-port", bpo::value<uint16_t>()->default_value(8086),
         "TimeStar server port")
        ("warmup-iterations", bpo::value<int>()->default_value(3),
         "Warmup iterations per query (not timed)")
        ("filter", bpo::value<std::string>()->default_value(""),
         "Run only queries whose name contains this substring");

    return app.run(argc, argv, [&]() -> future<> {
        auto& cfg = app.configuration();

        const auto host    = cfg["server-host"].as<std::string>();
        const auto port    = cfg["server-port"].as<uint16_t>();
        const auto warmup  = cfg["warmup-iterations"].as<int>();
        const auto filter  = cfg["filter"].as<std::string>();

        auto addr = socket_address(net::inet_address(host), port);

        // ── Header ─────────────────────────────────────────────────
        fmt::print("{:=<80}\n", "");
        fmt::print(" TimeStar Query Benchmark\n");
        fmt::print("{:=<80}\n", "");
        fmt::print("  Server:   {}:{}\n", host, port);
        fmt::print("  Warmup:   {} iterations per query\n", warmup);
        if (!filter.empty()) {
            fmt::print("  Filter:   \"{}\"\n", filter);
        }
        fmt::print("{:=<80}\n\n", "");

        // ── Health check ───────────────────────────────────────────
        {
            auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
            auto hclient = std::make_unique<http::experimental::client>(std::move(factory));
            auto req = http::request::make("GET", sstring("localhost"), sstring("/health"));
            bool healthy = false;
            try {
                co_await hclient->make_request(
                    std::move(req),
                    [&healthy](const http::reply& rep,
                               input_stream<char>&& body_in) -> future<> {
                        auto body = std::move(body_in);
                        healthy = (rep._status == http::reply::status_type::ok);
                        auto buf = co_await body.read();
                        while (!buf.empty()) buf = co_await body.read();
                    });
            } catch (...) {}
            co_await hclient->close();

            if (!healthy) {
                fmt::print("ERROR: server health check failed at {}:{}\n", host, port);
                co_return;
            }
            fmt::print("Server health check: OK\n\n");
        }

        // ── Build query suite ──────────────────────────────────────
        auto suite = buildQuerySuite();

        // Apply filter
        if (!filter.empty()) {
            std::erase_if(suite, [&](const QueryDef& d) {
                return d.name.find(filter) == std::string::npos;
            });
            fmt::print("Running {} queries (filtered)\n\n", suite.size());
        } else {
            fmt::print("Running {} queries\n\n", suite.size());
        }

        // ── Execute ────────────────────────────────────────────────
        std::vector<QueryResult> results;
        results.reserve(suite.size());

        double totalBenchMs = 0;

        for (size_t qi = 0; qi < suite.size(); ++qi) {
            auto& def = suite[qi];
            fmt::print("[{}/{}] {} ({}x) ... ",
                       qi + 1, suite.size(), def.name, def.iterations);

            // Warmup (not timed)
            if (warmup > 0) {
                QueryDef warmDef = def;
                warmDef.iterations = warmup;
                co_await runQuery(addr, warmDef);
            }

            // Timed run
            auto res = co_await runQuery(addr, def);

            if (res.successes > 0) {
                double avg = res.total_ms / res.successes;

                // Compute p50, p95, p99
                auto sorted = res.latencies_ms;
                std::sort(sorted.begin(), sorted.end());
                auto pct = [&](double p) -> double {
                    size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
                    return sorted[idx];
                };

                fmt::print("avg={:.2f}ms  min={:.2f}  p50={:.2f}  p95={:.2f}  "
                           "max={:.2f}ms",
                           avg, res.min_ms, pct(0.50), pct(0.95), res.max_ms);

                if (res.point_count >= 0) {
                    fmt::print("  pts={}", res.point_count);
                }
                if (res.series_count >= 0) {
                    fmt::print("  series={}", res.series_count);
                }
                fmt::print("  resp={}B", res.response_bytes);
                fmt::print("\n");

                totalBenchMs += res.total_ms;
            } else {
                fmt::print("FAILED");
                if (!res.first_error.empty()) {
                    fmt::print(": {}", res.first_error.substr(0, 120));
                }
                fmt::print("\n");
            }

            results.push_back(std::move(res));
        }

        // ── Summary ────────────────────────────────────────────────
        fmt::print("\n{:=<80}\n", "");
        fmt::print(" SUMMARY\n");
        fmt::print("{:=<80}\n\n", "");

        // Group results by category
        struct CategorySummary {
            std::string name;
            int queries = 0;
            int total_iters = 0;
            int total_ok = 0;
            int total_fail = 0;
            double total_ms = 0;
        };

        // Categorize by prefix before first ':'
        std::vector<CategorySummary> categories;
        auto getCategory = [](const std::string& name) -> std::string {
            // Use the aggregation method or GET as the category
            auto colon = name.find(':');
            if (colon != std::string::npos) {
                return std::string(name.substr(0, colon));
            }
            if (name.starts_with("GET")) return "metadata";
            return "other";
        };

        for (auto& r : results) {
            auto cat = getCategory(r.name);
            auto it = std::find_if(categories.begin(), categories.end(),
                [&](const CategorySummary& c) { return c.name == cat; });
            if (it == categories.end()) {
                categories.push_back({cat});
                it = categories.end() - 1;
            }
            it->queries++;
            it->total_iters += r.iterations;
            it->total_ok    += r.successes;
            it->total_fail  += r.failures;
            it->total_ms    += r.total_ms;
        }

        fmt::print("  {:<12s}  {:>6s}  {:>6s}  {:>6s}  {:>10s}  {:>10s}\n",
                   "Category", "Queries", "Iters", "Fails", "Total(ms)", "Avg(ms)");
        fmt::print("  {:-<12s}  {:-<6s}  {:-<6s}  {:-<6s}  {:-<10s}  {:-<10s}\n",
                   "", "", "", "", "", "");
        for (auto& c : categories) {
            double avg = c.total_ok > 0 ? c.total_ms / c.total_ok : 0;
            fmt::print("  {:<12s}  {:>6d}  {:>6d}  {:>6d}  {:>10.1f}  {:>10.2f}\n",
                       c.name, c.queries, c.total_iters, c.total_fail,
                       c.total_ms, avg);
        }

        int totalOk = 0, totalFail = 0;
        for (auto& r : results) { totalOk += r.successes; totalFail += r.failures; }

        fmt::print("\n  Total: {} queries, {} iterations, {} OK, {} failed, "
                   "{:.1f}ms total\n",
                   results.size(), totalOk + totalFail, totalOk, totalFail,
                   totalBenchMs);

        // ── Slowest queries ────────────────────────────────────────
        fmt::print("\n  Top 5 slowest (by average latency):\n");
        auto byAvg = results;
        std::sort(byAvg.begin(), byAvg.end(), [](const QueryResult& a, const QueryResult& b) {
            double aa = a.successes > 0 ? a.total_ms / a.successes : 0;
            double ba = b.successes > 0 ? b.total_ms / b.successes : 0;
            return aa > ba;
        });
        for (size_t i = 0; i < std::min(size_t(5), byAvg.size()); ++i) {
            auto& r = byAvg[i];
            double avg = r.successes > 0 ? r.total_ms / r.successes : 0;
            fmt::print("    {:>8.2f}ms  {}\n", avg, r.name);
        }

        // ── Fastest queries ────────────────────────────────────────
        fmt::print("\n  Top 5 fastest (by average latency):\n");
        for (size_t i = 0; i < std::min(size_t(5), byAvg.size()); ++i) {
            auto& r = byAvg[byAvg.size() - 1 - i];
            double avg = r.successes > 0 ? r.total_ms / r.successes : 0;
            fmt::print("    {:>8.2f}ms  {}\n", avg, r.name);
        }

        fmt::print("\n{:=<80}\n", "");
        fmt::print(" Benchmark complete.\n");
        fmt::print("{:=<80}\n", "");
    });
}

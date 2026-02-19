/**
 * tsdb_insert_bench — High-performance HTTP insert benchmark for TSDB.
 *
 * Uses Seastar's async HTTP client with connection pooling.  All data is
 * deterministically generated from a fixed seed so results are reproducible
 * across runs.
 *
 * Usage:
 *   ./tsdb_insert_bench --server-host 127.0.0.1 --server-port 8086 \
 *       --connections 8 --batch-size 10000 --batches 100 --hosts 10
 */

#include <boost/range/irange.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
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
#include <iomanip>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using namespace seastar;
using clk = std::chrono::steady_clock;

// ──────────────────────────────────────────────────────────────────────
// Deterministic data generation
// ──────────────────────────────────────────────────────────────────────

static constexpr uint64_t MINUTE_NS  = 60'000'000'000ULL;
static constexpr uint64_t SECOND_NS  =  1'000'000'000ULL;

static const std::array<const char*, 10> FIELD_NAMES = {
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature"
};

/**
 * Build a single array-format JSON payload.
 *
 * The format is:
 *   { "measurement": "...",
 *     "tags": { "host": "...", "rack": "..." },
 *     "timestamps": [ ... ],
 *     "fields": { "cpu_usage": [ ... ], ... } }
 *
 * Every value is deterministic given (seed, hostId, rackId, startTs, count).
 */
static std::string buildPayload(uint64_t seed, int hostId, int rackId,
                                uint64_t startTs, size_t count) {
    // Seed a fast xoshiro128+ from the combined seed.
    std::mt19937_64 rng(seed ^ (static_cast<uint64_t>(hostId) << 32) ^ startTs);
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    // Pre-size the string; typical payload ~28 bytes/field/point.
    std::string buf;
    buf.reserve(count * FIELD_NAMES.size() * 30 + 512);

    buf += R"({"measurement":"server.metrics","tags":{"host":"host-)";
    buf += fmt::format("{:02d}", hostId);
    buf += R"(","rack":"rack-)";
    buf += fmt::format("{}", rackId);

    // timestamps array
    buf += R"("},"timestamps":[)";
    for (size_t i = 0; i < count; ++i) {
        if (i) buf += ',';
        buf += std::to_string(startTs + i * MINUTE_NS);
    }

    // fields object
    buf += R"(],"fields":{)";
    for (size_t f = 0; f < FIELD_NAMES.size(); ++f) {
        if (f) buf += ',';
        buf += '"';
        buf += FIELD_NAMES[f];
        buf += R"(":[)";
        for (size_t i = 0; i < count; ++i) {
            if (i) buf += ',';
            // 6 significant digits — enough for float64 round-trip at this range.
            buf += fmt::format("{:.6g}", dist(rng));
        }
        buf += ']';
    }
    buf += "}}";
    return buf;
}

// ──────────────────────────────────────────────────────────────────────
// Latency histogram
// ──────────────────────────────────────────────────────────────────────

struct LatencyStats {
    std::vector<double> samples_ms;

    void add(clk::duration d) {
        double ms = std::chrono::duration<double, std::milli>(d).count();
        samples_ms.push_back(ms);
    }

    void merge(LatencyStats&& other) {
        samples_ms.insert(samples_ms.end(),
                          std::make_move_iterator(other.samples_ms.begin()),
                          std::make_move_iterator(other.samples_ms.end()));
    }

    void print(const char* label) const {
        if (samples_ms.empty()) return;
        auto sorted = samples_ms;          // copy for sorting
        std::sort(sorted.begin(), sorted.end());
        auto pct = [&](double p) {
            size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
            return sorted[idx];
        };
        double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
        fmt::print("  {:<22s}  min={:>8.2f}  avg={:>8.2f}  med={:>8.2f}"
                   "  p95={:>8.2f}  p99={:>8.2f}  max={:>8.2f}  (ms, n={})\n",
                   label,
                   sorted.front(),
                   sum / sorted.size(),
                   pct(0.50),
                   pct(0.95),
                   pct(0.99),
                   sorted.back(),
                   sorted.size());
    }
};

// ──────────────────────────────────────────────────────────────────────
// Per-shard worker
// ──────────────────────────────────────────────────────────────────────

struct ShardResult {
    LatencyStats latency;
    size_t       requests_ok      = 0;
    size_t       requests_fail    = 0;
    size_t       requests_http_err = 0;   // non-2xx responses
    size_t       total_points     = 0;
    clk::duration wall_time{};
    std::string  first_error;              // first error body for debugging
};

/**
 * Each Seastar shard runs this function concurrently.
 *
 * It creates its own HTTP client (with `maxConn` pooled connections) and
 * fires `batchCount` requests, with up to `maxConn` in-flight at a time
 * using a semaphore.
 */
static future<ShardResult>
runShardWorker(socket_address addr, unsigned maxConn,
               size_t batchSize, size_t batchCount,
               int numHosts, int numRacks, uint64_t globalSeed) {
    using http_client = http::experimental::client;

    const unsigned thisShard = this_shard_id();
    const unsigned shardCount = smp::count;

    // Partition work: each shard handles ceil(batchCount / shardCount) batches,
    // but we assign sequentially so shard 0 takes batches 0..k-1, shard 1 takes k..2k-1, etc.
    const size_t perShard  = (batchCount + shardCount - 1) / shardCount;
    const size_t myStart   = thisShard * perShard;
    const size_t myEnd     = std::min(myStart + perShard, batchCount);
    const size_t myBatches = (myEnd > myStart) ? (myEnd - myStart) : 0;

    if (myBatches == 0) {
        co_return ShardResult{};
    }

    // Build HTTP client with connection pool.
    auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
    auto client = std::make_unique<http_client>(std::move(factory), maxConn);

    // Pre-generate all payloads so timing measures only server throughput.
    std::vector<std::string> payloads;
    payloads.reserve(myBatches);

    // Deterministic per-batch assignment: batch i → host/rack based on seed.
    std::mt19937 rng(globalSeed + myStart);
    std::uniform_int_distribution<int> hostDist(1, numHosts);
    std::uniform_int_distribution<int> rackDist(1, numRacks);

    // Base timestamp: 1 year ago in nanoseconds (deterministic from epoch 0).
    constexpr uint64_t BASE_TS = 1'000'000'000'000'000'000ULL; // ~2001

    for (size_t i = 0; i < myBatches; ++i) {
        int hid = hostDist(rng);
        int rid = rackDist(rng);
        uint64_t startTs = BASE_TS + (myStart + i) * batchSize * MINUTE_NS;
        payloads.push_back(buildPayload(globalSeed, hid, rid, startTs, batchSize));
    }

    // Fire requests with bounded concurrency via semaphore + gate.
    auto res = make_lw_shared<ShardResult>();
    auto sem = make_lw_shared<semaphore>(maxConn);

    auto wallStart = clk::now();

    // Use parallel_for_each with semaphore for bounded concurrency.
    co_await parallel_for_each(
        boost::irange<size_t>(0, myBatches),
        [&, res, sem](size_t i) -> future<> {
            auto units = co_await get_units(*sem, 1);
            auto t0 = clk::now();

            auto req = http::request::make("POST", sstring("localhost"), sstring("/write"));
            req.write_body("json", sstring(payloads[i]));

            try {
                bool httpOk = false;
                sstring errBody;
                co_await client->make_request(
                    std::move(req),
                    [&httpOk, &errBody](const http::reply& rep,
                                        input_stream<char>&& body_in) -> future<> {
                        // CRITICAL: move body into coroutine-frame-local variable.
                        // Rvalue-ref params become dangling after first co_await
                        // because the caller's stack frame is destroyed.
                        auto body = std::move(body_in);
                        httpOk = (static_cast<int>(rep._status) >= 200 &&
                                  static_cast<int>(rep._status) < 300);
                        // Drain body (required to reuse connection).
                        sstring acc;
                        auto buf = co_await body.read();
                        while (!buf.empty()) {
                            if (!httpOk && acc.size() < 512) {
                                acc.append(buf.get(), std::min(buf.size(), size_t(512) - acc.size()));
                            }
                            buf = co_await body.read();
                        }
                        if (!httpOk) {
                            errBody = fmt::format("HTTP {}: {}",
                                static_cast<int>(rep._status),
                                std::string_view(acc.data(), acc.size()));
                        }
                    });
                if (httpOk) {
                    res->requests_ok++;
                    res->total_points += batchSize * FIELD_NAMES.size();
                } else {
                    res->requests_http_err++;
                    if (res->first_error.empty()) {
                        res->first_error = std::move(errBody);
                    }
                }
            } catch (const std::exception& e) {
                res->requests_fail++;
                if (res->first_error.empty()) {
                    res->first_error = fmt::format("Exception: {}", e.what());
                }
            } catch (...) {
                res->requests_fail++;
            }

            auto t1 = clk::now();
            res->latency.add(t1 - t0);
        });

    res->wall_time = clk::now() - wallStart;

    co_await client->close();
    co_return std::move(*res);
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    app_template app;

    namespace bpo = boost::program_options;
    app.add_options()
        ("server-host",  bpo::value<std::string>()->default_value("127.0.0.1"),
         "TSDB server host")
        ("server-port",  bpo::value<uint16_t>()->default_value(8086),
         "TSDB server port")
        ("connections",  bpo::value<unsigned>()->default_value(8),
         "Max concurrent HTTP connections per shard")
        ("batch-size",   bpo::value<size_t>()->default_value(10000),
         "Timestamps per array-format request")
        ("batches",      bpo::value<size_t>()->default_value(100),
         "Total number of batches to send")
        ("hosts",        bpo::value<int>()->default_value(10),
         "Number of simulated hosts")
        ("racks",        bpo::value<int>()->default_value(2),
         "Number of simulated racks")
        ("seed",         bpo::value<uint64_t>()->default_value(42),
         "Global PRNG seed for reproducibility")
        ("warmup",       bpo::value<size_t>()->default_value(5),
         "Number of warmup batches (not timed)")
        ("verify",       bpo::value<bool>()->default_value(true),
         "Query server after insert to verify data was persisted");

    return app.run(argc, argv, [&]() -> future<> {
        auto& cfg = app.configuration();

        const auto host      = cfg["server-host"].as<std::string>();
        const auto port      = cfg["server-port"].as<uint16_t>();
        const auto maxConn   = cfg["connections"].as<unsigned>();
        const auto batchSize = cfg["batch-size"].as<size_t>();
        const auto batches   = cfg["batches"].as<size_t>();
        const auto numHosts  = cfg["hosts"].as<int>();
        const auto numRacks  = cfg["racks"].as<int>();
        const auto seed      = cfg["seed"].as<uint64_t>();
        const auto warmup    = cfg["warmup"].as<size_t>();
        const auto verify    = cfg["verify"].as<bool>();

        const size_t fieldsPerRow = FIELD_NAMES.size();
        const size_t totalPoints  = batches * batchSize * fieldsPerRow;

        auto addr = socket_address(net::inet_address(host), port);

        // ── Print header ────────────────────────────────────────────
        fmt::print("{:=<70}\n", "");
        fmt::print(" TSDB C++ Insert Benchmark (Seastar HTTP client)\n");
        fmt::print("{:=<70}\n", "");
        fmt::print("  Server:         {}:{}\n", host, port);
        fmt::print("  Shards:         {}\n", smp::count);
        fmt::print("  Connections:    {} per shard ({} total)\n",
                   maxConn, maxConn * smp::count);
        fmt::print("  Batch size:     {} timestamps x {} fields = {} pts\n",
                   batchSize, fieldsPerRow, batchSize * fieldsPerRow);
        fmt::print("  Batches:        {} ({} warmup + {} timed)\n",
                   warmup + batches, warmup, batches);
        fmt::print("  Total points:   {} (timed)\n", totalPoints);
        fmt::print("  Hosts/Racks:    {} / {}\n", numHosts, numRacks);
        fmt::print("  Seed:           {}\n", seed);
        fmt::print("{:=<70}\n\n", "");

        // ── Health check ────────────────────────────────────────────
        {
            auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
            auto client = std::make_unique<http::experimental::client>(std::move(factory));
            auto req = http::request::make("GET", sstring("localhost"), sstring("/health"));
            bool healthy = false;
            try {
                co_await client->make_request(
                    std::move(req),
                    [&](const http::reply& rep, input_stream<char>&& body_in) -> future<> {
                        auto body = std::move(body_in);
                        healthy = (rep._status == http::reply::status_type::ok);
                        auto buf = co_await body.read();
                        while (!buf.empty()) buf = co_await body.read();
                    });
            } catch (...) {}
            co_await client->close();

            if (!healthy) {
                fmt::print("ERROR: server health check failed at {}:{}\n", host, port);
                co_return;
            }
            fmt::print("Server health check: OK\n\n");
        }

        // ── Warmup ──────────────────────────────────────────────────
        if (warmup > 0) {
            fmt::print("Running {} warmup batches...\n", warmup);
            // Use seed + 1 billion so warmup data is different from timed data.
            std::vector<future<ShardResult>> warmupFuts;
            warmupFuts.reserve(smp::count);
            for (unsigned s = 0; s < smp::count; ++s) {
                warmupFuts.push_back(
                    smp::submit_to(s, [=] {
                        return runShardWorker(addr, maxConn, batchSize, warmup,
                                              numHosts, numRacks, seed + 1'000'000'000ULL);
                    }));
            }
            for (auto& f : warmupFuts) {
                co_await std::move(f);
            }
            fmt::print("Warmup complete.\n\n");
        }

        // ── Timed run ───────────────────────────────────────────────
        fmt::print("Running {} timed batches across {} shards...\n",
                   batches, smp::count);

        auto globalStart = clk::now();

        // Launch workers on each shard via submit_to (returns future on shard 0).
        std::vector<future<ShardResult>> futs;
        futs.reserve(smp::count);
        for (unsigned s = 0; s < smp::count; ++s) {
            futs.push_back(
                smp::submit_to(s, [=] {
                    return runShardWorker(addr, maxConn, batchSize, batches,
                                          numHosts, numRacks, seed);
                }));
        }

        std::vector<ShardResult> shardResults;
        shardResults.reserve(smp::count);
        for (auto& f : futs) {
            shardResults.push_back(co_await std::move(f));
        }

        auto globalEnd = clk::now();
        double wallSec = std::chrono::duration<double>(globalEnd - globalStart).count();

        // ── Aggregate ───────────────────────────────────────────────
        LatencyStats combined;
        size_t totalOk      = 0;
        size_t totalFail    = 0;
        size_t totalHttpErr = 0;
        size_t totalPts     = 0;
        std::string firstError;

        for (auto& sr : shardResults) {
            totalOk      += sr.requests_ok;
            totalFail    += sr.requests_fail;
            totalHttpErr += sr.requests_http_err;
            totalPts     += sr.total_points;
            combined.merge(std::move(sr.latency));
            if (firstError.empty() && !sr.first_error.empty()) {
                firstError = std::move(sr.first_error);
            }
        }

        // ── Results ─────────────────────────────────────────────────
        fmt::print("\n{:=<70}\n", "");
        fmt::print(" RESULTS\n");
        fmt::print("{:=<70}\n", "");

        fmt::print("\n  Requests:       {} OK, {} HTTP errors, {} connection failures\n",
                   totalOk, totalHttpErr, totalFail);
        if (!firstError.empty()) {
            fmt::print("  First error:    {}\n", firstError);
        }
        fmt::print("  Points written: {}\n", totalPts);
        fmt::print("  Wall time:      {:.3f} s\n", wallSec);
        fmt::print("  Throughput:     {:.0f} pts/sec\n",
                   totalPts / wallSec);
        fmt::print("  Batch rate:     {:.1f} batches/sec\n",
                   totalOk / wallSec);

        fmt::print("\n  Latency per batch:\n");
        combined.print("all shards");

        // Per-shard breakdown.
        fmt::print("\n  Per-shard throughput:\n");
        for (unsigned s = 0; s < smp::count; ++s) {
            auto& sr = shardResults[s];
            if (sr.requests_ok == 0 && sr.requests_fail == 0 && sr.requests_http_err == 0) continue;
            double sec = std::chrono::duration<double>(sr.wall_time).count();
            fmt::print("    shard {:>2}: {:>8} pts in {:.2f}s = {:.0f} pts/sec"
                       "  ({} ok, {} http_err, {} fail)\n",
                       s, sr.total_points, sec,
                       sec > 0 ? sr.total_points / sec : 0.0,
                       sr.requests_ok, sr.requests_http_err, sr.requests_fail);
        }

        // ── Verification ─────────────────────────────────────────
        if (verify && totalOk > 0) {
            fmt::print("\n  Verifying data persistence...\n");

            // Query the measurement to see how many points the server reports.
            auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
            auto vclient = std::make_unique<http::experimental::client>(std::move(factory));

            // Build a query that covers the full time range we inserted.
            constexpr uint64_t BASE_TS = 1'000'000'000'000'000'000ULL;
            uint64_t endTs = BASE_TS + (batches + warmup + 1) * batchSize * MINUTE_NS;

            auto queryJson = fmt::format(
                R"json({{"query":"latest:server.metrics()","startTime":{},"endTime":{}}})json",
                BASE_TS, endTs);

            auto req = http::request::make("POST", sstring("localhost"), sstring("/query"));
            req.write_body("json", sstring(queryJson));

            try {
                auto respStatus = make_lw_shared<uint16_t>(0);
                auto respBody = make_lw_shared<sstring>();
                co_await vclient->make_request(
                    std::move(req),
                    [respStatus, respBody](const http::reply& rep,
                                           input_stream<char>&& body_in) -> future<> {
                        auto body = std::move(body_in);
                        *respStatus = static_cast<uint16_t>(rep._status);
                        sstring acc;
                        auto buf = co_await body.read();
                        while (!buf.empty()) {
                            if (acc.size() < 4096) {
                                acc.append(buf.get(), std::min(buf.size(), size_t(4096) - acc.size()));
                            }
                            buf = co_await body.read();
                        }
                        *respBody = std::move(acc);
                    });

                if (*respStatus >= 200 && *respStatus < 300) {
                    // Try to extract point_count from the JSON response.
                    auto body = std::string_view(respBody->data(), respBody->size());
                    auto pos = body.find("\"point_count\":");
                    if (pos != std::string_view::npos) {
                        auto numStart = pos + 14;
                        auto numEnd = body.find_first_of(",}", numStart);
                        auto countStr = body.substr(numStart, numEnd - numStart);
                        // Trim whitespace
                        while (!countStr.empty() && countStr.front() == ' ') countStr.remove_prefix(1);
                        fmt::print("  Server reports point_count: {}\n", countStr);
                    }

                    auto spos = body.find("\"series_count\":");
                    if (spos != std::string_view::npos) {
                        auto numStart = spos + 15;
                        auto numEnd = body.find_first_of(",}", numStart);
                        auto countStr = body.substr(numStart, numEnd - numStart);
                        while (!countStr.empty() && countStr.front() == ' ') countStr.remove_prefix(1);
                        fmt::print("  Server reports series_count: {}\n", countStr);
                    }

                    if (pos == std::string_view::npos && spos == std::string_view::npos) {
                        // Print truncated response for debugging.
                        fmt::print("  Verification response ({}): {}\n",
                                   *respStatus,
                                   body.substr(0, std::min(body.size(), size_t(512))));
                    }
                } else {
                    fmt::print("  Verification query returned HTTP {}: {}\n",
                               *respStatus,
                               std::string_view(respBody->data(),
                                   std::min(respBody->size(), size_t(256))));
                }
            } catch (const std::exception& e) {
                fmt::print("  Verification query failed: {}\n", e.what());
            }

            co_await vclient->close();
        }

        fmt::print("\n{:=<70}\n", "");
        fmt::print(" Benchmark complete.\n");
        fmt::print("{:=<70}\n", "");
    });
}

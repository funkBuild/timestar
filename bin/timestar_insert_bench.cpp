/**
 * timestar_insert_bench — High-performance HTTP insert benchmark for TimeStar.
 *
 * Uses Seastar's async HTTP client with connection pooling.  All data is
 * deterministically generated from a fixed seed so results are reproducible
 * across runs.
 *
 * Usage:
 *   ./timestar_insert_bench --server-host 127.0.0.1 --server-port 8086 \
 *       --connections 8 --batch-size 10000 --batches 100 --hosts 10
 *
 *   # Compare JSON vs Protobuf:
 *   ./timestar_insert_bench --format json     --batch-size 10000 --batches 100
 *   ./timestar_insert_bench --format protobuf --batch-size 10000 --batches 100
 */

#include "bench_common.hpp"

#include "timestar.pb.h"

#include <boost/range/irange.hpp>

#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <numeric>
#include <random>
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
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/net/inet_address.hh>
#include <string>
#include <vector>

using namespace seastar;
using timestar::bench::clk;
using timestar::bench::LatencyStats;
using timestar::bench::WireFormat;

// ──────────────────────────────────────────────────────────────────────
// Deterministic data generation
// ──────────────────────────────────────────────────────────────────────

static constexpr uint64_t MINUTE_NS = 60'000'000'000ULL;
static constexpr uint64_t SECOND_NS = 1'000'000'000ULL;

static const std::array<const char*, 10> FIELD_NAMES = {"cpu_usage",    "memory_usage", "disk_io_read", "disk_io_write",
                                                        "network_in",   "network_out",  "load_avg_1m",  "load_avg_5m",
                                                        "load_avg_15m", "temperature"};

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
static std::string buildPayload(uint64_t seed, int hostId, int rackId, uint64_t startTs, size_t count) {
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
        if (i)
            buf += ',';
        buf += std::to_string(startTs + i * MINUTE_NS);
    }

    // fields object
    buf += R"(],"fields":{)";
    for (size_t f = 0; f < FIELD_NAMES.size(); ++f) {
        if (f)
            buf += ',';
        buf += '"';
        buf += FIELD_NAMES[f];
        buf += R"(":[)";
        for (size_t i = 0; i < count; ++i) {
            if (i)
                buf += ',';
            // Fixed decimals, NEVER "{:.6g}": %g drops the decimal point on
            // whole values ("42"), the server types the token shape (42 ->
            // integer), and first-write-wins binding then REFUSES every float
            // write to that series forever. A .6g run poisoned ~thousands of
            // series and measured the speed of rejecting work.
            buf += fmt::format("{:.3f}", dist(rng));
        }
        buf += ']';
    }
    buf += "}}";
    return buf;
}

/**
 * Build a {"writes":[...]} batch payload of SCALAR write points.
 *
 * Exercises the DOM batch path in http_write_handler.cpp (coalesceWrites +
 * MultiWritePoint dispatch), unlike buildPayload() which hits the single
 * array-format fast path.
 *
 * Each element looks like:
 *   { "measurement": "server.metrics",
 *     "tags": { "host": "host-NN", "rack": "rack-M" },
 *     "fields": { "cpu_usage": X },
 *     "timestamp": T }
 *
 * Points cycle through hosts×racks tag sets and the 10 field names, so a
 * batch of `count` scalar writes coalesces server-side into
 * ~hosts×racks×|fields| MultiWritePoints (O(100-1000) for typical settings).
 * Every value is deterministic given (seed, startTs, count).
 */
static std::string buildPayloadJsonBatch(uint64_t seed, int numHosts, int numRacks, uint64_t startTs, size_t count,
                                         size_t batchIdx) {
    std::mt19937_64 rng(seed ^ startTs);
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    // ~130 bytes per scalar write point.
    std::string buf;
    buf.reserve(count * 140 + 64);

    const size_t tagSets = static_cast<size_t>(numHosts) * static_cast<size_t>(numRacks);
    const size_t numFields = FIELD_NAMES.size();
    // Tagsets covered by one batch (each with all of its fields).
    const size_t tagsetsPerBatch = std::max<size_t>(1, count / numFields);

    buf += R"({"writes":[)";
    for (size_t i = 0; i < count; ++i) {
        if (i)
            buf += ',';
        // FIELD-MAJOR series mapping with a per-batch phase rotation.
        //
        // The old mapping picked the field from (i / tagSets), with i resetting
        // every batch -- so unless one batch held tagSets * numFields points
        // (over the server's 100k batch cap at high cardinality), fields beyond
        // the first few were NEVER written and the intended series count was
        // silently unreachable. Field-major keeps every batch full-fielded, and
        // the phase walks the tagset window forward so consecutive batches
        // cover the whole fleet in waves: full series coverage every
        // ceil(tagSets / tagsetsPerBatch) batches regardless of batch size.
        const size_t tagsetIdx = (i / numFields + batchIdx * tagsetsPerBatch) % tagSets;
        const int hostId = static_cast<int>(tagsetIdx % static_cast<size_t>(numHosts)) + 1;
        const int rackId =
            static_cast<int>((tagsetIdx / static_cast<size_t>(numHosts)) % static_cast<size_t>(numRacks)) + 1;
        const char* fieldName = FIELD_NAMES[i % numFields];
        const uint64_t ts = startTs + i * MINUTE_NS;

        buf += R"({"measurement":"server.metrics","tags":{"host":"host-)";
        buf += fmt::format("{:02d}", hostId);
        buf += R"(","rack":"rack-)";
        buf += fmt::format("{}", rackId);
        buf += R"("},"fields":{")";
        buf += fieldName;
        buf += R"(":)";
        // Fixed decimals -- see the array-format comment: "{:.6g}" emits
        // "42" for whole values and integer-poisons the series binding.
        buf += fmt::format("{:.3f}", dist(rng));
        buf += R"(},"timestamp":)";
        buf += std::to_string(ts);
        buf += '}';
    }
    buf += "]}";
    return buf;
}

/**
 * Build a serialized WriteRequest protobuf payload.
 *
 * Produces the same data as buildPayload() but in protobuf wire format.
 * The WriteRequest contains a single WritePoint with array-format fields.
 */
static std::string buildPayloadProto(uint64_t seed, int hostId, int rackId, uint64_t startTs, size_t count) {
    std::mt19937_64 rng(seed ^ (static_cast<uint64_t>(hostId) << 32) ^ startTs);
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("server.metrics");
    (*wp->mutable_tags())[std::string("host")] = fmt::format("host-{:02d}", hostId);
    (*wp->mutable_tags())[std::string("rack")] = fmt::format("rack-{}", rackId);

    // Timestamps
    for (size_t i = 0; i < count; ++i) {
        wp->add_timestamps(startTs + i * MINUTE_NS);
    }

    // Fields: same 10 fields as JSON, same RNG sequence
    for (size_t f = 0; f < FIELD_NAMES.size(); ++f) {
        ::timestar_pb::WriteField wf;
        auto* dv = wf.mutable_double_values();
        for (size_t i = 0; i < count; ++i) {
            dv->add_values(dist(rng));
        }
        (*wp->mutable_fields())[std::string(FIELD_NAMES[f])] = wf;
    }

    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

/**
 * Build a fleet-shaped protobuf WriteRequest: many scalar WritePoints, one per
 * tagset, each carrying ONE timestamp and all 10 fields (10 points).
 *
 * Series mapping mirrors buildPayloadJsonBatch (same tagset window phase per
 * batch), so json-batch and protobuf-batch runs are directly comparable: same
 * cardinality, same coverage cadence, different wire format. This is the
 * differential that separates parse cost from storage cost at high
 * cardinality.
 */
static std::string buildPayloadProtoBatch(uint64_t seed, int numHosts, int numRacks, uint64_t startTs, size_t count,
                                          size_t batchIdx) {
    std::mt19937_64 rng(seed ^ startTs);
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    const size_t tagSets = static_cast<size_t>(numHosts) * static_cast<size_t>(numRacks);
    const size_t numFields = FIELD_NAMES.size();
    const size_t writePoints = std::max<size_t>(1, count / numFields);
    const size_t tagsetsPerBatch = writePoints;

    ::timestar_pb::WriteRequest req;
    for (size_t w = 0; w < writePoints; ++w) {
        const size_t tagsetIdx = (w + batchIdx * tagsetsPerBatch) % tagSets;
        const int hostId = static_cast<int>(tagsetIdx % static_cast<size_t>(numHosts)) + 1;
        const int rackId =
            static_cast<int>((tagsetIdx / static_cast<size_t>(numHosts)) % static_cast<size_t>(numRacks)) + 1;

        auto* wp = req.add_writes();
        wp->set_measurement("server.metrics");
        (*wp->mutable_tags())[std::string("host")] = fmt::format("host-{:02d}", hostId);
        (*wp->mutable_tags())[std::string("rack")] = fmt::format("rack-{}", rackId);
        wp->add_timestamps(startTs + w * MINUTE_NS);
        for (size_t f = 0; f < numFields; ++f) {
            ::timestar_pb::WriteField wf;
            wf.mutable_double_values()->add_values(dist(rng));
            (*wp->mutable_fields())[std::string(FIELD_NAMES[f])] = wf;
        }
    }

    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

// ──────────────────────────────────────────────────────────────────────
// Latency histogram
// ──────────────────────────────────────────────────────────────────────

// LatencyStats lives in bench_common.hpp (shared with timestar_query_bench).

// ──────────────────────────────────────────────────────────────────────
// Per-shard worker
// ──────────────────────────────────────────────────────────────────────

struct ShardResult {
    LatencyStats latency;
    size_t requests_ok = 0;
    size_t requests_fail = 0;
    size_t requests_http_err = 0;  // non-2xx responses
    size_t total_points = 0;
    clk::duration wall_time{};
    std::string first_error;  // first error body for debugging
};

/**
 * Runs the insert benchmark on shard 0 only.
 *
 * All parallelism comes from the HTTP connection pool (`maxConn` in-flight
 * requests).  This avoids competing with the server for CPU cores when both
 * run on the same machine.  Payloads are pre-generated before timing starts.
 */
static future<ShardResult> runBenchmark(socket_address addr, unsigned maxConn, size_t batchSize, size_t batchCount,
                                        int numHosts, int numRacks, uint64_t globalSeed,
                                        WireFormat format = WireFormat::Json) {
    using http_client = http::experimental::client;

    // Build HTTP client with connection pool.
    auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
    auto client = std::make_unique<http_client>(std::move(factory), maxConn);

    // Pre-generate payloads for the array formats so timing measures only
    // server throughput. json-batch payloads are generated LAZILY per request
    // instead: high-cardinality endurance runs (30k batches x ~14MB) would
    // need hundreds of GB pre-generated, which is exactly the client-side
    // bad_alloc this loop produced at 130k batches. Lazy generation costs
    // client CPU during the run, which only matters when benchmarking the
    // server's peak rate -- use the array formats for that.
    std::vector<std::string> payloads;

    std::mt19937 rng(globalSeed);
    std::uniform_int_distribution<int> hostDist(1, numHosts);
    std::uniform_int_distribution<int> rackDist(1, numRacks);

    constexpr uint64_t BASE_TS = 1'000'000'000'000'000'000ULL;

    const bool lazyPayloads = (format == WireFormat::JsonBatch || format == WireFormat::ProtobufBatch);
    if (!lazyPayloads) {
        payloads.reserve(batchCount);
        for (size_t i = 0; i < batchCount; ++i) {
            int hid = hostDist(rng);
            int rid = rackDist(rng);
            uint64_t startTs = BASE_TS + i * batchSize * MINUTE_NS;
            if (format == WireFormat::Protobuf) {
                payloads.push_back(buildPayloadProto(globalSeed, hid, rid, startTs, batchSize));
            } else {
                payloads.push_back(buildPayload(globalSeed, hid, rid, startTs, batchSize));
            }
        }
    }

    // Points per successful request: json-batch sends `batchSize` scalar
    // write points (one field each); the array formats send batchSize
    // timestamps × |FIELD_NAMES| fields.
    const size_t pointsPerRequest = (format == WireFormat::JsonBatch || format == WireFormat::ProtobufBatch)
                                        ? batchSize
                                        : batchSize * FIELD_NAMES.size();

    // MIME type for HTTP requests.  Seastar's write_body() maps file-extension
    // strings to MIME types ("json" → "application/json").  For protobuf there
    // is no built-in mapping, so we call write_body("bin", ...) and then
    // override the Content-Type header directly via set_mime_type().
    const bool useProto = (format == WireFormat::Protobuf || format == WireFormat::ProtobufBatch);

    auto res = make_lw_shared<ShardResult>();
    auto sem = make_lw_shared<semaphore>(maxConn);

    auto wallStart = clk::now();

    co_await parallel_for_each(boost::irange<size_t>(0, batchCount), [&, res, sem, useProto](size_t i) -> future<> {
        auto units = co_await get_units(*sem, 1);
        auto t0 = clk::now();

        auto req = http::request::make("POST", sstring("localhost"), sstring("/write"));
        // Lazy check FIRST: ProtobufBatch is both lazy and proto, and the
        // pregenerated `payloads` vector is empty in lazy mode -- indexing it
        // here was an instant segfault.
        if (lazyPayloads) {
            const uint64_t startTs = BASE_TS + i * batchSize * MINUTE_NS;
            if (format == WireFormat::ProtobufBatch) {
                req.write_body("bin",
                               sstring(buildPayloadProtoBatch(globalSeed, numHosts, numRacks, startTs, batchSize, i)));
                req.set_mime_type("application/x-protobuf");
            } else {
                req.write_body("json",
                               sstring(buildPayloadJsonBatch(globalSeed, numHosts, numRacks, startTs, batchSize, i)));
            }
        } else if (useProto) {
            req.write_body("bin", sstring(payloads[i]));
            req.set_mime_type("application/x-protobuf");
        } else {
            req.write_body("json", sstring(payloads[i]));
        }

        try {
            bool httpOk = false;
            sstring errBody;
            co_await client->make_request(
                std::move(req), [&httpOk, &errBody](const http::reply& rep, input_stream<char>&& body_in) -> future<> {
                    auto body = std::move(body_in);
                    httpOk = (static_cast<int>(rep._status) >= 200 && static_cast<int>(rep._status) < 300);
                    sstring acc;
                    auto buf = co_await body.read();
                    while (!buf.empty()) {
                        if (!httpOk && acc.size() < 512) {
                            acc.append(buf.get(), std::min(buf.size(), size_t(512) - acc.size()));
                        }
                        buf = co_await body.read();
                    }
                    if (!httpOk) {
                        errBody = fmt::format("HTTP {}: {}", static_cast<int>(rep._status),
                                              std::string_view(acc.data(), acc.size()));
                    }
                });
            if (httpOk) {
                res->requests_ok++;
                res->total_points += pointsPerRequest;
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
    app.add_options()("server-host", bpo::value<std::string>()->default_value("127.0.0.1"), "TimeStar server host")(
        "server-port", bpo::value<uint16_t>()->default_value(8086), "TimeStar server port")(
        "connections", bpo::value<unsigned>()->default_value(8), "Max concurrent HTTP connections per shard")(
        "batch-size", bpo::value<size_t>()->default_value(10000), "Timestamps per array-format request")(
        "batches", bpo::value<size_t>()->default_value(100), "Total number of batches to send")(
        "hosts", bpo::value<int>()->default_value(10), "Number of simulated hosts")(
        "racks", bpo::value<int>()->default_value(2), "Number of simulated racks")(
        "seed", bpo::value<uint64_t>()->default_value(42), "Global PRNG seed for reproducibility")(
        "warmup", bpo::value<size_t>()->default_value(5), "Number of warmup batches (not timed)")(
        "verify", bpo::value<bool>()->default_value(true), "Query server after insert to verify data was persisted")(
        "format", bpo::value<std::string>()->default_value("json"),
        "Wire format: 'json' (default, single array-format point), 'json-batch' ({\"writes\":[...]} scalar batch), or "
        "'protobuf'");

    return app.run(argc, argv, [&]() -> future<> {
        auto& cfg = app.configuration();

        const auto host = cfg["server-host"].as<std::string>();
        const auto port = cfg["server-port"].as<uint16_t>();
        const auto maxConn = cfg["connections"].as<unsigned>();
        const auto batchSize = cfg["batch-size"].as<size_t>();
        const auto batches = cfg["batches"].as<size_t>();
        const auto numHosts = cfg["hosts"].as<int>();
        const auto numRacks = cfg["racks"].as<int>();
        const auto seed = cfg["seed"].as<uint64_t>();
        const auto warmup = cfg["warmup"].as<size_t>();
        const auto verify = cfg["verify"].as<bool>();
        const auto formatStr = cfg["format"].as<std::string>();

        WireFormat format = WireFormat::Json;
        if (formatStr == "protobuf" || formatStr == "proto" || formatStr == "pb") {
            format = WireFormat::Protobuf;
        } else if (formatStr == "json-batch" || formatStr == "jsonbatch") {
            format = WireFormat::JsonBatch;
        } else if (formatStr == "protobuf-batch" || formatStr == "proto-batch") {
            format = WireFormat::ProtobufBatch;
        } else if (formatStr != "json") {
            fmt::print("ERROR: unknown format '{}'. Use 'json', 'json-batch', 'protobuf' or 'protobuf-batch'.\n",
                       formatStr);
            co_return;
        }

        const size_t fieldsPerRow = FIELD_NAMES.size();
        const size_t pointsPerBatch = (format == WireFormat::JsonBatch || format == WireFormat::ProtobufBatch)
                                          ? batchSize
                                          : batchSize * fieldsPerRow;
        const size_t totalPoints = batches * pointsPerBatch;

        auto addr = socket_address(net::inet_address(host), port);

        // ── Print header ────────────────────────────────────────────
        fmt::print("{:=<70}\n", "");
        fmt::print(" TimeStar C++ Insert Benchmark (Seastar HTTP client)\n");
        fmt::print("{:=<70}\n", "");
        fmt::print("  Server:         {}:{}\n", host, port);
        fmt::print("  Format:         {}\n", format == WireFormat::Protobuf        ? "protobuf"
                                             : format == WireFormat::ProtobufBatch ? "protobuf-batch"
                                             : format == WireFormat::JsonBatch     ? "json-batch"
                                                                                   : "json");
        fmt::print("  Connections:    {} (concurrent HTTP connections)\n", maxConn);
        if (format == WireFormat::JsonBatch || format == WireFormat::ProtobufBatch) {
            fmt::print("  Batch size:     {} scalar write points per request\n", batchSize);
        } else {
            fmt::print("  Batch size:     {} timestamps x {} fields = {} pts\n", batchSize, fieldsPerRow,
                       batchSize * fieldsPerRow);
        }
        fmt::print("  Batches:        {} ({} warmup + {} timed)\n", warmup + batches, warmup, batches);
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
                co_await client->make_request(std::move(req),
                                              [&](const http::reply& rep, input_stream<char>&& body_in) -> future<> {
                                                  auto body = std::move(body_in);
                                                  healthy = (rep._status == http::reply::status_type::ok);
                                                  auto buf = co_await body.read();
                                                  while (!buf.empty())
                                                      buf = co_await body.read();
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
            co_await runBenchmark(addr, maxConn, batchSize, warmup, numHosts, numRacks, seed + 1'000'000'000ULL,
                                  format);
            fmt::print("Warmup complete.\n\n");
        }

        // ── Timed run ───────────────────────────────────────────────
        fmt::print("Running {} timed batches with {} connections...\n", batches, maxConn);

        auto globalStart = clk::now();

        auto result = co_await runBenchmark(addr, maxConn, batchSize, batches, numHosts, numRacks, seed, format);

        auto globalEnd = clk::now();
        double wallSec = std::chrono::duration<double>(globalEnd - globalStart).count();

        // ── Aggregate ───────────────────────────────────────────────
        LatencyStats combined = std::move(result.latency);
        size_t totalOk = result.requests_ok;
        size_t totalFail = result.requests_fail;
        size_t totalHttpErr = result.requests_http_err;
        size_t totalPts = result.total_points;
        std::string firstError = std::move(result.first_error);

        // ── Results ─────────────────────────────────────────────────
        fmt::print("\n{:=<70}\n", "");
        fmt::print(" RESULTS\n");
        fmt::print("{:=<70}\n", "");

        fmt::print("\n  Requests:       {} OK, {} HTTP errors, {} connection failures\n", totalOk, totalHttpErr,
                   totalFail);
        if (!firstError.empty()) {
            fmt::print("  First error:    {}\n", firstError);
        }
        fmt::print("  Points written: {}\n", totalPts);
        fmt::print("  Wall time:      {:.3f} s\n", wallSec);
        fmt::print("  Throughput:     {:.0f} pts/sec\n", totalPts / wallSec);
        fmt::print("  Batch rate:     {:.1f} batches/sec\n", totalOk / wallSec);

        fmt::print("\n  Latency per batch:\n");
        combined.print("batch latency");

        // ── Verification ─────────────────────────────────────────
        if (verify && totalOk > 0) {
            fmt::print("\n  Verifying data persistence...\n");

            // Query the measurement to see how many points the server reports.
            auto factory = std::make_unique<http::experimental::basic_connection_factory>(addr);
            auto vclient = std::make_unique<http::experimental::client>(std::move(factory));

            // Build a query that covers the full time range we inserted.
            constexpr uint64_t BASE_TS = 1'000'000'000'000'000'000ULL;
            uint64_t endTs = BASE_TS + (batches + warmup + 1) * batchSize * MINUTE_NS;

            auto queryJson = fmt::format(R"json({{"query":"latest:server.metrics()","startTime":{},"endTime":{}}})json",
                                         BASE_TS, endTs);

            auto req = http::request::make("POST", sstring("localhost"), sstring("/query"));
            req.write_body("json", sstring(queryJson));

            try {
                auto respStatus = make_lw_shared<uint16_t>(0);
                auto respBody = make_lw_shared<sstring>();
                co_await vclient->make_request(
                    std::move(req),
                    [respStatus, respBody](const http::reply& rep, input_stream<char>&& body_in) -> future<> {
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
                        while (!countStr.empty() && countStr.front() == ' ')
                            countStr.remove_prefix(1);
                        fmt::print("  Server reports point_count: {}\n", countStr);
                    }

                    auto spos = body.find("\"series_count\":");
                    if (spos != std::string_view::npos) {
                        auto numStart = spos + 15;
                        auto numEnd = body.find_first_of(",}", numStart);
                        auto countStr = body.substr(numStart, numEnd - numStart);
                        while (!countStr.empty() && countStr.front() == ' ')
                            countStr.remove_prefix(1);
                        fmt::print("  Server reports series_count: {}\n", countStr);
                    }

                    if (pos == std::string_view::npos && spos == std::string_view::npos) {
                        // Print truncated response for debugging.
                        fmt::print("  Verification response ({}): {}\n", *respStatus,
                                   body.substr(0, std::min(body.size(), size_t(512))));
                    }
                } else {
                    fmt::print("  Verification query returned HTTP {}: {}\n", *respStatus,
                               std::string_view(respBody->data(), std::min(respBody->size(), size_t(256))));
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

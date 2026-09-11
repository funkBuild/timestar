// TSM compaction benchmark.
//
// Measures the cost of TSMCompactor::compact() over a synthetic tier-0 file
// set, with and without a retention policy installed.
//
// Why the retention dimension exists: processSeriesForCompaction() carries
// compressed blocks straight through to the output when a series' blocks do
// not overlap and need no per-point transformation ("zero-copy carry").
// `hasPerPointRetention = (ttlCutoff > 0 || dsStageCount > 0)` disables that
// carry, so once a measurement has ANY retention policy every compaction of
// its series decodes and re-encodes every block. This benchmark quantifies
// that, per data shape.
//
// Usage (from the build directory):
//   ./test/benchmark/tsm_compaction_benchmark --smp 1 \
//       --data-dir /path/with/room --repeats 5 [--scenarios recent,scada7,...]

#include "../../lib/core/timestar_value.hpp"
#include "../../lib/retention/retention_policy.hpp"
#include "../../lib/storage/tsm.hpp"
#include "../../lib/storage/tsm_compactor.hpp"
#include "../../lib/storage/tsm_file_manager.hpp"
#include "../../lib/storage/tsm_writer.hpp"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace bpo = boost::program_options;

static constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;
static constexpr uint64_t NS_PER_MIN = 60ULL * NS_PER_SEC;
static constexpr uint64_t NS_PER_DAY = 24ULL * 3600ULL * NS_PER_SEC;

static uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

// ---------------------------------------------------------------------------
// Dataset description
// ---------------------------------------------------------------------------

// How stored values are generated. The two models exercise very different
// encode/decode costs: ALP compresses a rounded, slowly varying signal well and
// falls back to raw-bit exceptions on full-precision noise.
enum class ValueModel {
    ScadaAnalog,  // slow sinusoid + small noise, rounded to 2 decimals
    UniformNoise  // full-precision uniform doubles (the original generator)
};

struct DatasetSpec {
    std::string name;
    std::string measurement = "metrics";
    size_t numSeries = 500;
    size_t pointsPerSeriesPerFile = 20000;
    size_t numFiles = 4;
    uint64_t sampleIntervalNs = NS_PER_SEC;
    // Age of the NEWEST point, relative to wall-clock now. Retention thresholds
    // are wall-clock derived, so this is what places the data relative to them.
    uint64_t newestAgeNs = 0;
    ValueModel valueModel = ValueModel::ScadaAnalog;
    std::string note;

    size_t totalPoints() const { return numSeries * pointsPerSeriesPerFile * numFiles; }
    uint64_t spanNs() const { return static_cast<uint64_t>(pointsPerSeriesPerFile * numFiles) * sampleIntervalNs; }
};

// ---------------------------------------------------------------------------
// Retention policy cases
// ---------------------------------------------------------------------------

struct PolicyCase {
    std::string name;
    bool enabled = false;  // false => no policy at all (zero-copy carry active)
    RetentionPolicy policy;
};

static PolicyCase noPolicy() {
    return PolicyCase{"no-policy", false, {}};
}

// TTL only, with a cutoff far older than any generated point: the carry is
// disabled but not a single point is removed. Pure overhead.
static PolicyCase ttlOnlyPolicy(const std::string& measurement) {
    RetentionPolicy p;
    p.measurement = measurement;
    p.ttl = "3650d";
    p.ttlNanos = 3650ULL * NS_PER_DAY;
    return PolicyCase{"ttl-only", true, p};
}

// The SCADA cascade from docs/downsampling-cascade-plan.md.
static PolicyCase cascadePolicy(const std::string& measurement) {
    RetentionPolicy p;
    p.measurement = measurement;
    p.ttl = "730d";
    p.ttlNanos = 730ULL * NS_PER_DAY;

    DownsamplePolicy t0;
    t0.after = "7d";
    t0.afterNanos = 7ULL * NS_PER_DAY;
    t0.interval = "1m";
    t0.intervalNanos = NS_PER_MIN;
    t0.method = "avg";

    DownsamplePolicy t1;
    t1.after = "90d";
    t1.afterNanos = 90ULL * NS_PER_DAY;
    t1.interval = "15m";
    t1.intervalNanos = 15ULL * NS_PER_MIN;
    t1.method = "avg";

    p.downsampleTiers = {t0, t1};
    timestar::retention::normalizeRetentionTiers(p);
    return PolicyCase{"cascade", true, p};
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

struct GeneratedData {
    std::vector<std::string> paths;
    std::vector<SeriesId128> seriesIds;
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurement;
    uint64_t inputBytes = 0;
    size_t totalPoints = 0;
    uint64_t oldestTs = 0;
    uint64_t newestTs = 0;
};

static seastar::future<GeneratedData> generateDataset(const DatasetSpec& spec, const std::string& tsmDir) {
    GeneratedData out;

    // Series ids are stable across files.
    out.seriesIds.reserve(spec.numSeries);
    for (size_t s = 0; s < spec.numSeries; ++s) {
        TimeStarInsert<double> insert(spec.measurement, "value");
        insert.addTag("location", "datacenter_" + std::to_string(s / 100));
        insert.addTag("host", "server_" + std::to_string(s % 100));
        SeriesId128 sid = insert.seriesId128();
        out.seriesIds.push_back(sid);
        out.seriesMeasurement.emplace(sid, spec.measurement);
    }

    const uint64_t newest = nowNs() - spec.newestAgeNs;
    const uint64_t start = newest - spec.spanNs();
    out.oldestTs = start;
    out.newestTs = newest;

    for (size_t f = 0; f < spec.numFiles; ++f) {
        char filename[512];
        snprintf(filename, sizeof(filename), "%s/%02zu_%010zu.tsm", tsmDir.c_str(), size_t{0}, f + 1);
        out.paths.emplace_back(filename);

        TSMWriter writer(filename);
        std::mt19937_64 rng(0x9E3779B97F4A7C15ULL ^ (f * 1315423911ULL));
        std::uniform_real_distribution<double> noise(-0.5, 0.5);
        std::uniform_real_distribution<double> uniform(0.0, 100.0);

        const uint64_t fileStart =
            start + static_cast<uint64_t>(f * spec.pointsPerSeriesPerFile) * spec.sampleIntervalNs;

        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        timestamps.reserve(spec.pointsPerSeriesPerFile);
        values.reserve(spec.pointsPerSeriesPerFile);

        for (size_t s = 0; s < spec.numSeries; ++s) {
            timestamps.clear();
            values.clear();
            const double phase = static_cast<double>(s) * 0.017;
            for (size_t i = 0; i < spec.pointsPerSeriesPerFile; ++i) {
                timestamps.push_back(fileStart + static_cast<uint64_t>(i) * spec.sampleIntervalNs);
                double v;
                if (spec.valueModel == ValueModel::ScadaAnalog) {
                    const double t = static_cast<double>(f * spec.pointsPerSeriesPerFile + i);
                    v = 50.0 + 20.0 * std::sin(t / 3600.0 + phase) + noise(rng);
                    v = std::round(v * 100.0) / 100.0;
                } else {
                    v = uniform(rng);
                }
                values.push_back(v);
            }
            writer.writeSeries(TSMValueType::Float, out.seriesIds[s], timestamps, values);
            out.totalPoints += timestamps.size();
        }

        writer.writeIndex();
        writer.close();
        out.inputBytes += fs::file_size(filename);
        co_await seastar::yield();
    }

    co_return out;
}

// ---------------------------------------------------------------------------
// One measured compaction run
// ---------------------------------------------------------------------------

struct RunResult {
    double wallMs = 0;
    uint64_t outputBytes = 0;
    uint64_t pointsRead = 0;
    uint64_t pointsWritten = 0;
};

static seastar::future<RunResult> runOnce(TSMCompactor& compactor, const GeneratedData& data,
                                          const PolicyCase& policyCase) {
    // Reopen the source files each run so no decoded state carries over.
    std::vector<seastar::shared_ptr<TSM>> files;
    files.reserve(data.paths.size());
    for (size_t i = 0; i < data.paths.size(); ++i) {
        auto tsm = seastar::make_shared<TSM>(data.paths[i]);
        co_await tsm->open();
        tsm->tierNum = 0;
        tsm->seqNum = i + 1;
        files.push_back(tsm);
    }

    std::unordered_map<std::string, RetentionPolicy> policies;
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurement;
    if (policyCase.enabled) {
        policies.emplace(policyCase.policy.measurement, policyCase.policy);
        seriesMeasurement = data.seriesMeasurement;
    }

    const auto t0 = std::chrono::steady_clock::now();
    auto result = co_await compactor.compact(files, policies, seriesMeasurement);
    const auto t1 = std::chrono::steady_clock::now();

    RunResult rr;
    rr.wallMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
    rr.pointsRead = result.stats.pointsRead;
    rr.pointsWritten = result.stats.pointsWritten;
    if (fs::exists(result.outputPath)) {
        rr.outputBytes = fs::file_size(result.outputPath);
        fs::remove(result.outputPath);
    }

    files.clear();
    co_return rr;
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

struct CaseSummary {
    std::string dataset;
    std::string policy;
    double medianMs = 0;
    double minMs = 0;
    double maxMs = 0;
    uint64_t outputBytes = 0;
    uint64_t pointsRead = 0;
    uint64_t pointsWritten = 0;
    size_t inputPoints = 0;
};

static double median(std::vector<double> v) {
    if (v.empty()) {
        return 0;
    }
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

static std::string mb(uint64_t bytes) {
    std::ostringstream os;
    os << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MB";
    return os.str();
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

static std::vector<DatasetSpec> allDatasets() {
    std::vector<DatasetSpec> out;

    // "recent": 4 x 500 series x 20k points at 1 Hz, ending now. Every series'
    // blocks are non-overlapping and full, so with no policy EVERY series is
    // carried zero-copy. This is the ideal-carry synthetic (upper bound).
    DatasetSpec recent;
    recent.name = "recent";
    recent.note = "1 Hz, 22 h span ending now; ideal zero-copy carry";
    out.push_back(recent);

    // Same shape, uniform-random values (the original generator) so the value
    // model's effect on decode/encode cost is visible.
    DatasetSpec recentRand = recent;
    recentRand.name = "recent-rand";
    recentRand.valueModel = ValueModel::UniformNoise;
    recentRand.note = "as 'recent' but full-precision uniform values";
    out.push_back(recentRand);

    // "scada7": identical shape, shifted so the 22 h span straddles the 7 d
    // (1m) threshold: the older ~57% of points fold, the rest pass through raw.
    DatasetSpec scada7 = recent;
    scada7.name = "scada7";
    scada7.newestAgeNs = static_cast<uint64_t>(6.6 * static_cast<double>(NS_PER_DAY));
    scada7.note = "1 Hz, 22 h span straddling the 7d/1m threshold";
    out.push_back(scada7);

    // "scada90": same, straddling the 90 d (15m) threshold, so BOTH cascade
    // stages are live within one merge.
    DatasetSpec scada90 = recent;
    scada90.name = "scada90";
    scada90.newestAgeNs = static_cast<uint64_t>(89.6 * static_cast<double>(NS_PER_DAY));
    scada90.note = "1 Hz, 22 h span straddling the 90d/15m threshold";
    out.push_back(scada90);

    // "folded": already at final resolution (15 m) and entirely older than the
    // 90 d threshold but inside the 730 d TTL. Re-folding is a value no-op, and
    // no minTime-based block skip can help: every block IS older than the
    // threshold.
    DatasetSpec folded;
    folded.name = "folded";
    folded.numSeries = 500;
    folded.pointsPerSeriesPerFile = 15000;
    folded.numFiles = 4;
    folded.sampleIntervalNs = 15ULL * NS_PER_MIN;
    folded.newestAgeNs = 100ULL * NS_PER_DAY;
    folded.note = "15 m resolution, 625 d span ending 100 d ago (already folded)";
    out.push_back(folded);

    // "tier0real": the shape a real high-cardinality tier-0 merge has -- many
    // series, a few hundred points each per flush. Under-full block coalescing
    // takes these off the zero-copy path even with NO policy, so retention adds
    // nothing here.
    DatasetSpec tier0;
    tier0.name = "tier0real";
    tier0.numSeries = 20000;
    tier0.pointsPerSeriesPerFile = 200;
    tier0.numFiles = 4;
    tier0.sampleIntervalNs = NS_PER_SEC;
    tier0.note = "20k series x 200 pts/flush; under-full blocks";
    out.push_back(tier0);

    // "tier0big": the same merge shape but with per-series flush blocks large
    // enough to clear the under-full coalescing threshold (avg compressed block
    // >= 512 B * blockCapForTier/MaxPointsPerBlock), so these series DO qualify
    // for the zero-copy carry. The contrast with "tier0real" is what locates
    // the boundary at which retention starts costing anything at all.
    DatasetSpec tier0big = tier0;
    tier0big.name = "tier0big";
    tier0big.numSeries = 2500;
    tier0big.pointsPerSeriesPerFile = 1600;
    tier0big.note = "2.5k series x 1600 pts/flush; blocks above the coalescing threshold";
    out.push_back(tier0big);

    return out;
}

// Parameters are taken BY VALUE: this is a coroutine whose caller (the
// app.run() lambda) returns as soon as the first co_await suspends, so any
// reference parameter would dangle for every dataset after the first.
static seastar::future<int> runBenchmark(std::string dataDir, size_t repeats, size_t warmups,
                                         std::vector<std::string> wanted) {
    const std::string shardDir = "shard_0";
    const std::string tsmDir = shardDir + "/tsm";

    std::vector<CaseSummary> summaries;

    for (const auto& spec : allDatasets()) {
        if (!wanted.empty() && std::find(wanted.begin(), wanted.end(), spec.name) == wanted.end()) {
            continue;
        }

        fs::remove_all(shardDir);
        fs::create_directories(tsmDir);

        std::cout << "\n=== dataset '" << spec.name << "' ===\n"
                  << "  " << spec.note << "\n"
                  << "  files=" << spec.numFiles << " series=" << spec.numSeries
                  << " pts/series/file=" << spec.pointsPerSeriesPerFile << " total points=" << spec.totalPoints()
                  << "\n";

        auto genStart = std::chrono::steady_clock::now();
        auto data = co_await generateDataset(spec, tsmDir);
        auto genEnd = std::chrono::steady_clock::now();
        std::cout << "  generated " << mb(data.inputBytes) << " in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(genEnd - genStart).count() << " ms ("
                  << std::fixed << std::setprecision(2)
                  << (static_cast<double>(data.inputBytes) / static_cast<double>(data.totalPoints)) << " B/pt)\n";

        std::vector<PolicyCase> cases;
        cases.push_back(noPolicy());
        cases.push_back(ttlOnlyPolicy(spec.measurement));
        cases.push_back(cascadePolicy(spec.measurement));

        // One compactor per case, kept alive for the whole dataset: the cases
        // are run ROUND-ROBIN rather than one case to completion. This machine
        // shows occasional multi-second degradation episodes (background I/O)
        // that last tens of seconds; run sequentially, such an episode lands
        // entirely inside one case and corrupts the ratio. Interleaved, it hits
        // every case roughly equally and the medians stay comparable.
        std::vector<std::unique_ptr<TSMFileManager>> managers;
        std::vector<std::unique_ptr<TSMCompactor>> compactors;
        for (size_t c = 0; c < cases.size(); ++c) {
            managers.push_back(std::make_unique<TSMFileManager>());
            compactors.push_back(std::make_unique<TSMCompactor>(managers.back().get()));
        }

        // Settle: generation wrote the input files through the page cache, and
        // the compaction reads them O_DIRECT.
        ::sync();
        co_await seastar::sleep(std::chrono::seconds(3));

        std::vector<std::vector<double>> times(cases.size());
        std::vector<RunResult> lastResult(cases.size());
        for (size_t r = 0; r < warmups + repeats; ++r) {
            for (size_t c = 0; c < cases.size(); ++c) {
                auto rr = co_await runOnce(*compactors[c], data, cases[c]);
                if (r >= warmups) {
                    times[c].push_back(rr.wallMs);
                    lastResult[c] = rr;
                }
            }
        }

        for (size_t c = 0; c < cases.size(); ++c) {
            CaseSummary cs;
            cs.dataset = spec.name;
            cs.policy = cases[c].name;
            cs.medianMs = median(times[c]);
            cs.minMs = *std::min_element(times[c].begin(), times[c].end());
            cs.maxMs = *std::max_element(times[c].begin(), times[c].end());
            cs.outputBytes = lastResult[c].outputBytes;
            cs.pointsRead = lastResult[c].pointsRead;
            cs.pointsWritten = lastResult[c].pointsWritten;
            cs.inputPoints = data.totalPoints;
            summaries.push_back(cs);

            std::cout << "  [" << std::setw(9) << cs.policy << "] runs:";
            for (double t : times[c]) {
                std::cout << " " << std::fixed << std::setprecision(1) << t;
            }
            std::cout << "\n";
            std::cout << "  [" << std::setw(9) << cs.policy << "] median " << std::fixed << std::setprecision(1)
                      << cs.medianMs << " ms  (min " << cs.minMs << ", max " << cs.maxMs << ")   out "
                      << mb(cs.outputBytes) << "  pts_written " << cs.pointsWritten << "  " << std::setprecision(2)
                      << (static_cast<double>(cs.inputPoints) / (cs.medianMs / 1000.0) / 1e6) << " Mpts/s\n";
        }

        compactors.clear();
        managers.clear();
        fs::remove_all(shardDir);
    }

    std::cout << "\n=== SUMMARY ===\n";
    std::cout << std::left << std::setw(12) << "dataset" << std::setw(11) << "policy" << std::right << std::setw(11)
              << "median_ms" << std::setw(10) << "min_ms" << std::setw(10) << "max_ms" << std::setw(12) << "Mpts/s"
              << std::setw(12) << "out_MB" << std::setw(14) << "pts_written" << std::setw(9) << "vs_none"
              << "\n";
    double baseline = 0;
    std::string baselineDataset;
    for (const auto& s : summaries) {
        if (s.policy == "no-policy") {
            baseline = s.medianMs;
            baselineDataset = s.dataset;
        }
        const double ratio = (baselineDataset == s.dataset && baseline > 0) ? s.medianMs / baseline : 0.0;
        std::cout << std::left << std::setw(12) << s.dataset << std::setw(11) << s.policy << std::right << std::fixed
                  << std::setprecision(1) << std::setw(11) << s.medianMs << std::setw(10) << s.minMs << std::setw(10)
                  << s.maxMs << std::setprecision(2) << std::setw(12)
                  << (static_cast<double>(s.inputPoints) / (s.medianMs / 1000.0) / 1e6) << std::setw(12)
                  << (static_cast<double>(s.outputBytes) / (1024.0 * 1024.0)) << std::setw(14) << s.pointsWritten
                  << std::setw(9) << ratio << "\n";
    }

    (void)dataDir;
    co_return 0;
}

int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()("data-dir", bpo::value<std::string>()->default_value("./compaction_bench_data"),
                      "Directory to generate TSM files under (needs several GB)")(
        "repeats", bpo::value<size_t>()->default_value(5), "Timed runs per case")(
        "warmups", bpo::value<size_t>()->default_value(1), "Untimed warmup runs per case")(
        "scenarios", bpo::value<std::string>()->default_value(""),
        "Comma-separated dataset names to run (default: all)");

    std::cout << "TSM Compaction Benchmark (retention on/off)\n";
    std::cout << "==========================================\n";

    try {
        return app.run(argc, argv, [&app]() -> seastar::future<int> {
            auto& cfg = app.configuration();
            const std::string dataDir = cfg["data-dir"].as<std::string>();
            const size_t repeats = cfg["repeats"].as<size_t>();
            const size_t warmups = cfg["warmups"].as<size_t>();
            const std::string scen = cfg["scenarios"].as<std::string>();

            std::vector<std::string> wanted;
            if (!scen.empty()) {
                std::stringstream ss(scen);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    if (!item.empty()) {
                        wanted.push_back(item);
                    }
                }
            }

            fs::create_directories(dataDir);
            fs::current_path(dataDir);
            std::cout << "Data directory: " << fs::current_path() << "\n";
            std::cout << "Repeats: " << repeats << " (plus " << warmups << " warmup)\n";

            return runBenchmark(dataDir, repeats, warmups, wanted);
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}

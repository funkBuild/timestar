// Per-field downsample METHODS at the fold (Phase 4 of
// docs/downsampling-cascade-plan.md).
//
// The SCADA case this exists for: one method per measurement silently destroys
// data. A flow totalizer averaged over a 15-minute bucket is meaningless, and a
// status word needs its last value, not its mean. These tests fold an analog, a
// totalizer and a status word IN ONE PASS and assert each was reduced by its
// own method.
//
// The other half is the non-numeric decision. Boolean and String pass through
// unfolded by DEFAULT — storage-side folding is destructive and a status word's
// transition history is usually why it is retained. `fieldMethods: {"x":
// "latest"}` opts one in, and the folded values MUST come back in the type they
// were written in: a boolean is never 1.0/0.0 anywhere in this engine.
//
// Schema/validation/downgrade coverage lives in
// test/unit/retention/retention_field_methods_test.cpp.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_reader.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;
constexpr uint64_t ONE_MINUTE_NS = 60ULL * NS_PER_SEC;
constexpr uint64_t FIFTEEN_MINUTES_NS = 15ULL * ONE_MINUTE_NS;
constexpr uint64_t ONE_DAY_NS = 24ULL * 3600ULL * NS_PER_SEC;

uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

DownsamplePolicy fieldTier(uint64_t afterNanos, uint64_t intervalNanos, const std::string& method,
                           std::map<std::string, std::string> fieldMethods = {}) {
    DownsamplePolicy ds;
    ds.after = std::to_string(afterNanos) + "ns";
    ds.afterNanos = afterNanos;
    ds.interval = std::to_string(intervalNanos) + "ns";
    ds.intervalNanos = intervalNanos;
    ds.method = method;
    if (!fieldMethods.empty()) {
        ds.fieldMethods = std::move(fieldMethods);
    }
    return ds;
}

RetentionPolicy fieldPolicy(const std::string& measurement, std::vector<DownsamplePolicy> tiers) {
    RetentionPolicy p;
    p.measurement = measurement;
    p.downsampleTiers = std::move(tiers);
    timestar::retention::normalizeRetentionTiers(p);
    return p;
}

}  // namespace

// gtest's ASSERT_* expands to a bare `return;`, which a coroutine forbids —
// and SEASTAR_TEST_F bodies are coroutines. These are the coroutine-safe
// equivalents: record the failure, then unwind with co_return so the rest of
// the body (which would dereference the thing that just failed) never runs.
#define CO_ASSERT_TRUE(cond)                     \
    do {                                         \
        if (!(cond)) {                           \
            ADD_FAILURE() << "expected: " #cond; \
            co_return;                           \
        }                                        \
    } while (0)

#define CO_ASSERT_FALSE(cond) CO_ASSERT_TRUE(!(cond))

#define CO_ASSERT_EQ_MSG(lhs, rhs, msg)                                                                       \
    do {                                                                                                      \
        const auto coAssertLhs = (lhs);                                                                       \
        const auto coAssertRhs = (rhs);                                                                       \
        if (!(coAssertLhs == coAssertRhs)) {                                                                  \
            ADD_FAILURE() << #lhs " == " #rhs " (" << coAssertLhs << " vs " << coAssertRhs << "): " << (msg); \
            co_return;                                                                                        \
        }                                                                                                     \
    } while (0)

#define CO_ASSERT_EQ(lhs, rhs)                                                                     \
    do {                                                                                           \
        const auto coAssertLhs = (lhs);                                                            \
        const auto coAssertRhs = (rhs);                                                            \
        if (!(coAssertLhs == coAssertRhs)) {                                                       \
            ADD_FAILURE() << #lhs " == " #rhs " (" << coAssertLhs << " vs " << coAssertRhs << ")"; \
            co_return;                                                                             \
        }                                                                                          \
    } while (0)

class DownsampleFieldMethodsTest : public ::testing::Test {
public:
    std::string testDir = "./test_downsample_field_methods_files";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;
    uint64_t nextSeq = 0;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_downsample_field_methods_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);

        fileManager = std::make_unique<TSMFileManager>();
        compactor = std::make_unique<TSMCompactor>(fileManager.get());
    }

    void TearDown() override {
        compactor.reset();
        fileManager.reset();
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    // One TSM file holding an arbitrary set of typed series, written the way a
    // real memstore flush would: several series, several types, one file.
    class FileBuilder {
    public:
        explicit FileBuilder(std::string path) : writer_(path), path_(std::move(path)) {}

        template <typename T>
        FileBuilder& add(const std::string& seriesKey, TSMValueType type, const std::vector<uint64_t>& ts,
                         const std::vector<T>& vals) {
            writer_.writeSeries(type, SeriesId128::fromSeriesKey(seriesKey), ts, vals);
            return *this;
        }

        seastar::shared_ptr<TSM> finish(uint64_t seq) {
            writer_.writeIndex();
            writer_.close();
            auto tsm = seastar::make_shared<TSM>(path_);
            tsm->tierNum = 0;
            tsm->seqNum = seq;
            return tsm;
        }

    private:
        TSMWriter writer_;
        std::string path_;
    };

    FileBuilder newFile() {
        const uint64_t seq = nextSeq++;
        char filename[256];
        snprintf(filename, sizeof(filename), "shard_0/tsm/%02u_%010lu.tsm", 0u, seq);
        lastSeq = seq;
        return FileBuilder(filename);
    }
    uint64_t lastSeq = 0;

    template <typename T>
    static seastar::future<std::map<uint64_t, T>> readAll(const std::string& path, const std::string& seriesKey) {
        auto tsm = seastar::make_shared<TSM>(path);
        co_await tsm->open();
        co_await tsm->readSparseIndex();

        SeriesId128 sid = SeriesId128::fromSeriesKey(seriesKey);
        TSMResult<T> result(0);
        co_await tsm->readSeries(sid, 0, UINT64_MAX, result);

        std::map<uint64_t, T> out;
        for (const auto& blk : result.blocks) {
            for (size_t i = 0; i < blk->timestamps.size(); i++) {
                out[blk->timestamps.at(i)] = blk->values.at(i);
            }
        }
        co_await tsm->close();
        co_return out;
    }

    // Compact under `policy`, supplying the seriesId -> measurement and
    // seriesId -> field maps the Engine's provider builds in production.
    seastar::future<std::string> fold(std::vector<seastar::shared_ptr<TSM>> files, const RetentionPolicy& policy,
                                      const std::vector<std::pair<std::string, std::string>>& seriesKeyAndField) {
        std::unordered_map<std::string, RetentionPolicy> policies{{policy.measurement, policy}};
        std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMap;
        std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> fieldMap;
        for (const auto& [key, field] : seriesKeyAndField) {
            const auto sid = SeriesId128::fromSeriesKey(key);
            seriesMap.emplace(sid, policy.measurement);
            if (!field.empty()) {
                fieldMap.emplace(sid, field);
            }
        }
        auto result = co_await compactor->compact(files, policies, seriesMap, fieldMap);
        co_return result.outputPath;
    }
};

// ===========================================================================
// The SCADA case: three fields, three methods, ONE fold pass.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, EachFieldIsReducedByItsOwnMethod) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    // Everything sits inside ONE minute bucket, well past the threshold, so the
    // fold's answer is a single unambiguous value per series.
    const uint64_t bucket = ((now - 200 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    // Analog: avg (the tier default). Totalizer: max (monotonically rising, so
    // an average would understate it grossly). Status: latest, as a boolean.
    const std::vector<uint64_t> ts{bucket, bucket + NS_PER_SEC, bucket + 2 * NS_PER_SEC, bucket + 3 * NS_PER_SEC};
    const std::vector<double> level{10.0, 20.0, 30.0, 40.0};
    const std::vector<double> flow{1000.0, 1001.0, 1002.0, 1003.0};
    const std::vector<bool> status{false, false, true, true};

    const std::string levelKey = "scada|rtu=r1|level";
    const std::string flowKey = "scada|rtu=r1|flow_total";
    const std::string statusKey = "scada|rtu=r1|status";

    auto builder = self->newFile();
    builder.add(levelKey, TSMValueType::Float, ts, level);
    builder.add(flowKey, TSMValueType::Float, ts, flow);
    builder.add(statusKey, TSMValueType::Boolean, ts, status);
    auto file = builder.finish(self->lastSeq);
    co_await file->open();
    co_await file->readSparseIndex();

    auto policy = fieldPolicy(measurement,
                              {fieldTier(after, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}, {"status", "latest"}})});

    auto outPath =
        co_await self->fold({file}, policy, {{levelKey, "level"}, {flowKey, "flow_total"}, {statusKey, "status"}});
    co_await file->close();

    auto foldedLevel = co_await self->readAll<double>(outPath, levelKey);
    auto foldedFlow = co_await self->readAll<double>(outPath, flowKey);
    auto foldedStatus = co_await self->readAll<bool>(outPath, statusKey);

    CO_ASSERT_EQ_MSG(foldedLevel.size(), 1u, "the analog must collapse to one bucket");
    EXPECT_DOUBLE_EQ(foldedLevel.at(bucket), 25.0) << "level must use the TIER method (avg)";

    CO_ASSERT_EQ_MSG(foldedFlow.size(), 1u, "the totalizer must collapse to one bucket");
    EXPECT_DOUBLE_EQ(foldedFlow.at(bucket), 1003.0)
        << "flow_total must use its OVERRIDE (max); an avg here (1001.5) is the exact silent corruption "
           "per-field methods exist to prevent";

    CO_ASSERT_EQ_MSG(foldedStatus.size(), 1u, "the status word must collapse to one bucket");
    EXPECT_TRUE(foldedStatus.at(bucket)) << "status must be the LATEST value in the bucket";
}

// ===========================================================================
// The invariant: non-numeric folds keep their WRITTEN TYPE.
//
// `bool` and `std::string` here are not incidental — the values are read back
// through TSMResult<bool>/TSMResult<std::string>, so a value that had been
// coerced to 1.0/0.0 could not be produced at all. The type system carries
// half the assertion; the values carry the rest.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, NonNumericOptInKeepsTheWrittenType) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    // Two buckets, so "one point per bucket at the bucket start" is testable
    // rather than trivially true.
    std::vector<uint64_t> ts;
    std::vector<bool> bools;
    std::vector<std::string> strings;
    for (int bucketIdx = 0; bucketIdx < 2; ++bucketIdx) {
        for (int i = 0; i < 4; ++i) {
            ts.push_back(base + static_cast<uint64_t>(bucketIdx) * ONE_MINUTE_NS +
                         static_cast<uint64_t>(i) * NS_PER_SEC);
            // Bucket 0 ends false, bucket 1 ends true -> a MIN/MAX/AVG fold
            // could not produce both answers, so the assertion below really
            // pins LATEST rather than any fold that happens to agree.
            bools.push_back(bucketIdx == 0 ? (i < 3) : (i >= 1));
            strings.push_back("state-" + std::to_string(bucketIdx) + "-" + std::to_string(i));
        }
    }

    const std::string boolKey = "scada|rtu=r1|status";
    const std::string strKey = "scada|rtu=r1|mode";

    auto builder = self->newFile();
    builder.add(boolKey, TSMValueType::Boolean, ts, bools);
    builder.add(strKey, TSMValueType::String, ts, strings);
    auto file = builder.finish(self->lastSeq);
    co_await file->open();
    co_await file->readSparseIndex();

    auto policy =
        fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", {{"status", "latest"}, {"mode", "latest"}})});
    auto outPath = co_await self->fold({file}, policy, {{boolKey, "status"}, {strKey, "mode"}});
    co_await file->close();

    auto foldedBools = co_await self->readAll<bool>(outPath, boolKey);
    auto foldedStrings = co_await self->readAll<std::string>(outPath, strKey);

    CO_ASSERT_EQ(foldedBools.size(), 2u);
    CO_ASSERT_EQ(foldedStrings.size(), 2u);

    // Bucket starts, matching numeric fields and the query-time bucket layout.
    CO_ASSERT_TRUE(foldedBools.contains(base));
    CO_ASSERT_TRUE(foldedBools.contains(base + ONE_MINUTE_NS));

    // Bucket 0's last value is FALSE; bucket 1's last value is TRUE. Values are
    // booleans, not 1.0/0.0 — and the two buckets disagree, so no numeric fold
    // of the same points could reproduce this pair.
    EXPECT_FALSE(foldedBools.at(base));
    EXPECT_TRUE(foldedBools.at(base + ONE_MINUTE_NS));

    EXPECT_EQ(foldedStrings.at(base), "state-0-3");
    EXPECT_EQ(foldedStrings.at(base + ONE_MINUTE_NS), "state-1-3");
}

// ===========================================================================
// The DEFAULT: non-numeric passes through untouched.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, NonNumericPassesThroughWhenNotNamed) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    std::vector<uint64_t> ts;
    std::vector<bool> bools;
    std::vector<std::string> strings;
    for (int i = 0; i < 8; ++i) {
        ts.push_back(base + static_cast<uint64_t>(i) * NS_PER_SEC);
        bools.push_back(i % 2 == 0);
        strings.push_back("s" + std::to_string(i));
    }

    const std::string boolKey = "scada|rtu=r1|status";
    const std::string strKey = "scada|rtu=r1|mode";
    const std::string analogKey = "scada|rtu=r1|level";
    const std::vector<double> analog(8, 5.0);

    // Three policies that must ALL leave the non-numeric series untouched:
    //   (a) no fieldMethods at all;
    //   (b) fieldMethods naming a DIFFERENT field;
    //   (c) a measurement-wide `latest` method — explicitly NOT an opt-in,
    //       because a policy written before per-field methods existed must not
    //       start destroying status history on upgrade.
    struct Case {
        const char* name;
        RetentionPolicy policy;
    };
    std::vector<Case> cases{
        {"no fieldMethods", fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg")})},
        {"fieldMethods names another field",
         fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", {{"level", "max"}})})},
        {"measurement-wide latest", fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "latest")})},
    };

    for (auto& c : cases) {
        auto builder = self->newFile();
        builder.add(boolKey, TSMValueType::Boolean, ts, bools);
        builder.add(strKey, TSMValueType::String, ts, strings);
        builder.add(analogKey, TSMValueType::Float, ts, analog);
        auto file = builder.finish(self->lastSeq);
        co_await file->open();
        co_await file->readSparseIndex();

        auto outPath =
            co_await self->fold({file}, c.policy, {{boolKey, "status"}, {strKey, "mode"}, {analogKey, "level"}});
        co_await file->close();

        auto foldedBools = co_await self->readAll<bool>(outPath, boolKey);
        auto foldedStrings = co_await self->readAll<std::string>(outPath, strKey);
        auto foldedAnalog = co_await self->readAll<double>(outPath, analogKey);

        EXPECT_EQ(foldedBools.size(), ts.size()) << c.name << ": booleans must pass through UNFOLDED";
        EXPECT_EQ(foldedStrings.size(), ts.size()) << c.name << ": strings must pass through UNFOLDED";
        for (size_t i = 0; i < ts.size(); ++i) {
            EXPECT_EQ(foldedBools.count(ts[i]), 1u) << c.name << " ts index " << i;
            if (foldedStrings.contains(ts[i])) {
                EXPECT_EQ(foldedStrings.at(ts[i]), strings[i]) << c.name;
            }
        }
        // ...while the numeric field on the SAME measurement did fold, proving
        // the policy was in force rather than silently inert.
        EXPECT_EQ(foldedAnalog.size(), 1u) << c.name << ": the numeric field must still fold";
    }
}

// A hand-built policy that reaches compact() directly (no HTTP validation, no
// field-type index) must still not average a boolean. The compactor's own gate
// works from the series' real TSMValueType, so it needs no lookup.
SEASTAR_TEST_F(DownsampleFieldMethodsTest, NonLatestOverrideOnANonNumericSeriesFoldsNothing) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / ONE_MINUTE_NS) * ONE_MINUTE_NS;

    std::vector<uint64_t> ts;
    std::vector<bool> bools;
    for (int i = 0; i < 6; ++i) {
        ts.push_back(base + static_cast<uint64_t>(i) * NS_PER_SEC);
        bools.push_back(i % 2 == 0);
    }

    const std::string boolKey = "scada|rtu=r1|status";
    auto builder = self->newFile();
    builder.add(boolKey, TSMValueType::Boolean, ts, bools);
    auto file = builder.finish(self->lastSeq);
    co_await file->open();
    co_await file->readSparseIndex();

    // "max" on a boolean would be 1.0 under the old coercion.
    auto policy = fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", {{"status", "max"}})});
    auto outPath = co_await self->fold({file}, policy, {{boolKey, "status"}});
    co_await file->close();

    auto folded = co_await self->readAll<bool>(outPath, boolKey);
    EXPECT_EQ(folded.size(), ts.size()) << "a non-latest method on a boolean must fold NOTHING, not fold as 1/0";
    for (size_t i = 0; i < ts.size(); ++i) {
        CO_ASSERT_EQ(folded.count(ts[i]), 1u);
        EXPECT_EQ(folded.at(ts[i]), bools[i]);
    }
}

// ===========================================================================
// A policy WITHOUT fieldMethods must behave exactly as it did before Phase 4.
//
// Asserted by construction rather than by eyeballing: the same points folded
// under (a) a plain policy and (b) the same policy with an override for a field
// that is NOT this series' must produce identical output.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, PolicyWithoutFieldMethodsIsUnchanged) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (int i = 0; i < 600; ++i) {
        ts.push_back(base + static_cast<uint64_t>(i) * NS_PER_SEC);
        vals.push_back(1.0 + 0.25 * static_cast<double>(i));
    }

    const std::string keyA = "scada|rtu=r1|level";
    const std::string keyB = "scada|rtu=r2|level";

    auto builderA = self->newFile();
    builderA.add(keyA, TSMValueType::Float, ts, vals);
    auto fileA = builderA.finish(self->lastSeq);
    co_await fileA->open();
    co_await fileA->readSparseIndex();
    auto plainPath = co_await self->fold(
        {fileA},
        fieldPolicy(measurement,
                    {fieldTier(after, ONE_MINUTE_NS, "avg"), fieldTier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg")}),
        {{keyA, "level"}});
    co_await fileA->close();

    auto builderB = self->newFile();
    builderB.add(keyB, TSMValueType::Float, ts, vals);
    auto fileB = builderB.finish(self->lastSeq);
    co_await fileB->open();
    co_await fileB->readSparseIndex();
    const std::map<std::string, std::string> unrelated{{"some_other_field", "max"}};
    auto overriddenPath = co_await self->fold(
        {fileB},
        fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", unrelated),
                                  fieldTier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", unrelated)}),
        {{keyB, "level"}});
    co_await fileB->close();

    auto plain = co_await self->readAll<double>(plainPath, keyA);
    auto overridden = co_await self->readAll<double>(overriddenPath, keyB);

    CO_ASSERT_FALSE(plain.empty());
    CO_ASSERT_EQ(plain.size(), overridden.size());
    auto a = plain.begin();
    auto b = overridden.begin();
    for (; a != plain.end(); ++a, ++b) {
        EXPECT_EQ(a->first, b->first);
        EXPECT_DOUBLE_EQ(a->second, b->second);
    }
}

// ===========================================================================
// Composition across cascade stages for a NON-NUMERIC opt-in.
//
// `latest` composes exactly (the latest of per-bucket latests is the global
// latest), and that is the whole justification for offering it as the one
// non-numeric fold. The numeric test below proves it for `sum` on a Float; this
// proves it for the types that must never touch a double — a bool and a string
// folded raw->1m->15m must equal the same data folded raw->15m, VALUE AND TYPE.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, NonNumericLatestComposesAcrossStages) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    // 45 minutes (three 15m buckets) at an IRREGULAR per-minute rate, so the
    // last raw point of a 15m window is not the last point of its final minute
    // by construction — the composition has to actually carry it through.
    const std::vector<size_t> counts{7, 0, 3, 60, 0, 1, 12, 0, 0, 41};
    std::vector<uint64_t> ts;
    std::vector<bool> bools;
    std::vector<std::string> strings;
    size_t seq = 0;
    for (size_t m = 0; m < 45; ++m) {
        const size_t n = counts[m % counts.size()];
        for (size_t i = 0; i < n; ++i) {
            ts.push_back(base + m * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
            bools.push_back((seq % 3) == 0);
            strings.push_back("s" + std::to_string(seq));
            ++seq;
        }
    }
    CO_ASSERT_FALSE(ts.empty());

    const std::map<std::string, std::string> fm{{"status", "latest"}, {"mode", "latest"}};

    // Path A: raw --1m--> --15m-->.
    const std::string boolA = "scada|rtu=r1|status";
    const std::string strA = "scada|rtu=r1|mode";
    auto b1 = self->newFile();
    b1.add(boolA, TSMValueType::Boolean, ts, bools);
    b1.add(strA, TSMValueType::String, ts, strings);
    auto f1 = b1.finish(self->lastSeq);
    co_await f1->open();
    co_await f1->readSparseIndex();
    auto oneMinutePath =
        co_await self->fold({f1}, fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", fm)}),
                            {{boolA, "status"}, {strA, "mode"}});
    co_await f1->close();

    auto folded1m = seastar::make_shared<TSM>(oneMinutePath);
    co_await folded1m->open();
    co_await folded1m->readSparseIndex();
    auto cascadedPath =
        co_await self->fold({folded1m}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg", fm)}),
                            {{boolA, "status"}, {strA, "mode"}});
    co_await folded1m->close();
    auto cascadedBool = co_await self->readAll<bool>(cascadedPath, boolA);
    auto cascadedStr = co_await self->readAll<std::string>(cascadedPath, strA);

    // Path B: raw --15m--> directly.
    const std::string boolB = "scada|rtu=r2|status";
    const std::string strB = "scada|rtu=r2|mode";
    auto b2 = self->newFile();
    b2.add(boolB, TSMValueType::Boolean, ts, bools);
    b2.add(strB, TSMValueType::String, ts, strings);
    auto f2 = b2.finish(self->lastSeq);
    co_await f2->open();
    co_await f2->readSparseIndex();
    auto directPath =
        co_await self->fold({f2}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg", fm)}),
                            {{boolB, "status"}, {strB, "mode"}});
    co_await f2->close();
    auto directBool = co_await self->readAll<bool>(directPath, boolB);
    auto directStr = co_await self->readAll<std::string>(directPath, strB);

    CO_ASSERT_FALSE(directStr.empty());
    CO_ASSERT_EQ_MSG(cascadedStr.size(), directStr.size(), "stage-over-stage folding must not create or lose buckets");
    CO_ASSERT_EQ(cascadedBool.size(), directBool.size());

    auto cs = cascadedStr.begin();
    auto ds = directStr.begin();
    for (; cs != cascadedStr.end(); ++cs, ++ds) {
        EXPECT_EQ(cs->first, ds->first);
        EXPECT_EQ(cs->second, ds->second) << "`latest` on a string must compose exactly across stages";
    }
    auto cb = cascadedBool.begin();
    auto db = directBool.begin();
    for (; cb != cascadedBool.end(); ++cb, ++db) {
        EXPECT_EQ(cb->first, db->first);
        EXPECT_EQ(cb->second, db->second) << "`latest` on a boolean must compose exactly across stages";
    }

    // Bucket starts must be 15m-aligned and strictly ascending — the property
    // the streaming writer depends on when several fields with DIFFERENT methods
    // drain through one file.
    uint64_t prev = 0;
    for (const auto& [bucket, value] : directStr) {
        (void)value;
        EXPECT_EQ(bucket % FIFTEEN_MINUTES_NS, 0u);
        EXPECT_TRUE(prev == 0 || bucket > prev);
        prev = bucket;
    }
}

// ===========================================================================
// Composition across cascade stages, PER FIELD.
//
// The Phase 2 suite proved raw->1m->15m == raw->15m for a uniform method. The
// point of per-field methods is that a totalizer routed to `sum` keeps that
// exactness while the analog beside it takes the documented `avg`
// approximation — in the SAME policy, on the SAME merge.
// ===========================================================================
SEASTAR_TEST_F(DownsampleFieldMethodsTest, PerFieldMethodsComposeAcrossStages) {
    const std::string measurement = "scada";
    const uint64_t now = nowNs();
    const uint64_t after = 30 * ONE_DAY_NS;
    const uint64_t base = ((now - 200 * ONE_DAY_NS) / FIFTEEN_MINUTES_NS) * FIFTEEN_MINUTES_NS;

    // 30 minutes (two 15m buckets) with a deliberately IRREGULAR per-minute
    // count — comm gaps are exactly what makes avg-of-avgs inexact, and exactly
    // what a `sum` field must survive untouched.
    const std::vector<size_t> counts{60, 5, 60, 0, 17, 60, 1, 60, 0, 60};
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    double v = 3.0;
    for (size_t m = 0; m < 30; ++m) {
        const size_t n = counts[m % counts.size()];
        for (size_t i = 0; i < n; ++i) {
            ts.push_back(base + m * ONE_MINUTE_NS + static_cast<uint64_t>(i) * NS_PER_SEC);
            v += 1.0 + 0.37 * static_cast<double>(m);
            vals.push_back(v);
        }
    }
    CO_ASSERT_FALSE(ts.empty());

    const std::string totalKey = "scada|rtu=r1|flow_total";
    const std::map<std::string, std::string> fm{{"flow_total", "sum"}};

    // Path A: raw --1m--> --15m-->, with flow_total routed to `sum` at BOTH
    // stages (validation requires the method to agree across tiers).
    auto b1 = self->newFile();
    b1.add(totalKey, TSMValueType::Float, ts, vals);
    auto f1 = b1.finish(self->lastSeq);
    co_await f1->open();
    co_await f1->readSparseIndex();
    auto oneMinutePath = co_await self->fold(
        {f1}, fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg", fm)}), {{totalKey, "flow_total"}});
    co_await f1->close();

    auto folded1m = seastar::make_shared<TSM>(oneMinutePath);
    co_await folded1m->open();
    co_await folded1m->readSparseIndex();
    auto cascadedPath =
        co_await self->fold({folded1m}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg", fm)}),
                            {{totalKey, "flow_total"}});
    co_await folded1m->close();
    auto cascaded = co_await self->readAll<double>(cascadedPath, totalKey);

    // Path B: raw --15m--> directly.
    const std::string directKey = "scada|rtu=r2|flow_total";
    auto b2 = self->newFile();
    b2.add(directKey, TSMValueType::Float, ts, vals);
    auto f2 = b2.finish(self->lastSeq);
    co_await f2->open();
    co_await f2->readSparseIndex();
    auto directPath = co_await self->fold(
        {f2}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg", fm)}), {{directKey, "flow_total"}});
    co_await f2->close();
    auto direct = co_await self->readAll<double>(directPath, directKey);

    CO_ASSERT_FALSE(direct.empty());
    CO_ASSERT_EQ_MSG(cascaded.size(), direct.size(), "stage-over-stage folding must not create or lose buckets");
    auto ci = cascaded.begin();
    auto di = direct.begin();
    for (; ci != cascaded.end(); ++ci, ++di) {
        EXPECT_EQ(ci->first, di->first);
        EXPECT_DOUBLE_EQ(ci->second, di->second)
            << "a field routed to `sum` must compose EXACTLY across stages, gaps and all — that exactness "
               "is what makes the tier-wide avg approximation acceptable";
    }

    // The default average must also compose with the persisted sample counts.
    const std::string avgKey = "scada|rtu=r3|flow_total";
    auto b3 = self->newFile();
    b3.add(avgKey, TSMValueType::Float, ts, vals);
    auto f3 = b3.finish(self->lastSeq);
    co_await f3->open();
    co_await f3->readSparseIndex();
    auto avg1mPath = co_await self->fold({f3}, fieldPolicy(measurement, {fieldTier(after, ONE_MINUTE_NS, "avg")}),
                                         {{avgKey, "flow_total"}});
    co_await f3->close();
    auto avgFolded1m = seastar::make_shared<TSM>(avg1mPath);
    co_await avgFolded1m->open();
    co_await avgFolded1m->readSparseIndex();
    auto avgCascadedPath =
        co_await self->fold({avgFolded1m}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg")}),
                            {{avgKey, "flow_total"}});
    co_await avgFolded1m->close();
    auto avgCascaded = co_await self->readAll<double>(avgCascadedPath, avgKey);

    const std::string avgDirectKey = "scada|rtu=r4|flow_total";
    auto b4 = self->newFile();
    b4.add(avgDirectKey, TSMValueType::Float, ts, vals);
    auto f4 = b4.finish(self->lastSeq);
    co_await f4->open();
    co_await f4->readSparseIndex();
    auto avgDirectPath = co_await self->fold(
        {f4}, fieldPolicy(measurement, {fieldTier(after, FIFTEEN_MINUTES_NS, "avg")}), {{avgDirectKey, "flow_total"}});
    co_await f4->close();
    auto avgDirect = co_await self->readAll<double>(avgDirectPath, avgDirectKey);

    CO_ASSERT_EQ(avgCascaded.size(), avgDirect.size());
    auto ai = avgCascaded.begin();
    auto adi = avgDirect.begin();
    for (; ai != avgCascaded.end(); ++ai, ++adi) {
        EXPECT_NEAR(ai->second, adi->second, 1e-12 * std::max(1.0, std::abs(adi->second)))
            << "The default average must also retain sample weights across stages";
    }
}

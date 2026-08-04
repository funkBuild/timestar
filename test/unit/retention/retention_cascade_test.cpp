/*
 * Cascade schema tests: validation, persistence migration, and the protobuf
 * projection (Phase 2 of docs/downsampling-cascade-plan.md).
 *
 * The fold semantics themselves live in
 * test/unit/storage/downsample_cascade_test.cpp; this file is about the SHAPES
 * a policy can arrive in and the rules that decide whether it is accepted.
 */

#include "../../../lib/http/proto_converters.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../seastar_gtest.hpp"

#include "timestar.pb.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace {

constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;
constexpr uint64_t ONE_MINUTE_NS = 60ULL * NS_PER_SEC;
constexpr uint64_t FIFTEEN_MINUTES_NS = 15ULL * ONE_MINUTE_NS;
constexpr uint64_t ONE_DAY_NS = 24ULL * 3600ULL * NS_PER_SEC;

DownsamplePolicy tier(uint64_t afterNanos, uint64_t intervalNanos, const std::string& method = "avg") {
    DownsamplePolicy ds;
    ds.after = std::to_string(afterNanos) + "ns";
    ds.afterNanos = afterNanos;
    ds.interval = std::to_string(intervalNanos) + "ns";
    ds.intervalNanos = intervalNanos;
    ds.method = method;
    return ds;
}

RetentionPolicy policyOf(std::vector<DownsamplePolicy> tiers, uint64_t ttlNanos = 0) {
    RetentionPolicy p;
    p.measurement = "scada";
    p.ttlNanos = ttlNanos;
    if (ttlNanos > 0) {
        p.ttl = std::to_string(ttlNanos) + "ns";
    }
    p.downsampleTiers = std::move(tiers);
    timestar::retention::normalizeRetentionTiers(p);
    return p;
}

std::string validationError(const RetentionPolicy& p) {
    auto why = timestar::retention::validateRetentionPolicy(p);
    return why.value_or("");
}

}  // namespace

// ===========================================================================
// Validation
// ===========================================================================

class RetentionCascadeValidationTest : public ::testing::Test {};

// The target case from the plan: 1 Hz SCADA -> 1m after 7d -> 15m after 90d.
TEST_F(RetentionCascadeValidationTest, TargetScadaCascadeAccepted) {
    auto p =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 730 * ONE_DAY_NS);
    EXPECT_EQ(validationError(p), "");
}

TEST_F(RetentionCascadeValidationTest, FourTiersAcceptedFiveRejected) {
    auto four = policyOf({tier(1 * ONE_DAY_NS, ONE_MINUTE_NS), tier(7 * ONE_DAY_NS, FIFTEEN_MINUTES_NS),
                          tier(30 * ONE_DAY_NS, 30 * ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, 60 * ONE_MINUTE_NS)});
    EXPECT_EQ(validationError(four), "");

    auto five = policyOf({tier(1 * ONE_DAY_NS, ONE_MINUTE_NS), tier(7 * ONE_DAY_NS, FIFTEEN_MINUTES_NS),
                          tier(30 * ONE_DAY_NS, 30 * ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, 60 * ONE_MINUTE_NS),
                          tier(365 * ONE_DAY_NS, 120 * ONE_MINUTE_NS)});
    EXPECT_NE(validationError(five).find("at most 4 tiers"), std::string::npos) << validationError(five);
}

// DIVISIBILITY is the rule that keeps a stage-k bucket from straddling a
// stage-k+1 boundary under epoch alignment. 10m does not divide into 25m.
TEST_F(RetentionCascadeValidationTest, NonDivisibleIntervalsRejected) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, 10 * ONE_MINUTE_NS), tier(30 * ONE_DAY_NS, 25 * ONE_MINUTE_NS)});
    const std::string err = validationError(p);
    EXPECT_NE(err.find("exact multiple"), std::string::npos) << err;
}

TEST_F(RetentionCascadeValidationTest, DivisibleIntervalsAccepted) {
    // 15m = 15 x 1m, 60m = 4 x 15m.
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(30 * ONE_DAY_NS, FIFTEEN_MINUTES_NS),
                       tier(90 * ONE_DAY_NS, 60 * ONE_MINUTE_NS)});
    EXPECT_EQ(validationError(p), "");
}

TEST_F(RetentionCascadeValidationTest, NonIncreasingAfterRejected) {
    auto p = policyOf({tier(30 * ONE_DAY_NS, ONE_MINUTE_NS), tier(7 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)});
    EXPECT_NE(validationError(p).find("strictly greater than tier 0 after"), std::string::npos) << validationError(p);
}

TEST_F(RetentionCascadeValidationTest, EqualAfterRejected) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(7 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)});
    EXPECT_NE(validationError(p).find("strictly greater than tier 0 after"), std::string::npos) << validationError(p);
}

TEST_F(RetentionCascadeValidationTest, NonIncreasingIntervalRejected) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, FIFTEEN_MINUTES_NS), tier(30 * ONE_DAY_NS, ONE_MINUTE_NS)});
    EXPECT_NE(validationError(p).find("strictly greater than tier 0 interval"), std::string::npos)
        << validationError(p);
}

// A stage whose threshold is shorter than its own bucket can never hold a
// complete bucket.
TEST_F(RetentionCascadeValidationTest, AfterShorterThanOwnIntervalRejected) {
    auto p = policyOf({tier(30 * NS_PER_SEC, ONE_MINUTE_NS)});
    EXPECT_NE(validationError(p).find("must be at least"), std::string::npos) << validationError(p);
}

TEST_F(RetentionCascadeValidationTest, TtlMustExceedLastTierAfter) {
    auto tooShort =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 90 * ONE_DAY_NS);
    EXPECT_NE(validationError(tooShort).find("ttl must be greater than"), std::string::npos)
        << validationError(tooShort);

    auto ok =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 91 * ONE_DAY_NS);
    EXPECT_EQ(validationError(ok), "");

    // The single-tier message is preserved verbatim: existing clients assert on it.
    auto single = policyOf({tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 30 * ONE_DAY_NS);
    EXPECT_EQ(validationError(single), "ttl must be greater than downsample.after");
}

TEST_F(RetentionCascadeValidationTest, MixedMethodsRejectedInV1) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg"), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "max")});
    EXPECT_NE(validationError(p).find("same method"), std::string::npos) << validationError(p);
}

TEST_F(RetentionCascadeValidationTest, MethodSetIsExactlyTheFiveComposableOnes) {
    for (const char* m : {"avg", "min", "max", "sum", "latest"}) {
        EXPECT_TRUE(timestar::retention::isValidDownsampleMethod(m)) << m;
        EXPECT_EQ(validationError(policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, m)})), "") << m;
    }
    // COUNT is deliberately absent: count-of-counts is not count, so it cannot
    // survive a cascade. Neither can median or stddev.
    for (const char* m : {"count", "median", "stddev", "first", "spread", "AVG", "", "mean"}) {
        EXPECT_FALSE(timestar::retention::isValidDownsampleMethod(m)) << m;
    }
    EXPECT_NE(validationError(policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "count")})).find("Invalid downsample"),
              std::string::npos);
}

TEST_F(RetentionCascadeValidationTest, TtlOnlyPolicyIsValid) {
    RetentionPolicy p;
    p.measurement = "cpu";
    p.ttlNanos = 30 * ONE_DAY_NS;
    EXPECT_EQ(validationError(p), "");
    EXPECT_TRUE(timestar::retention::policyIsActionable(p));
}

TEST_F(RetentionCascadeValidationTest, PolicyWithNeitherTtlNorUsableTierIsInert) {
    RetentionPolicy p;
    p.measurement = "cpu";
    EXPECT_FALSE(timestar::retention::policyIsActionable(p));

    // A tier with zero nanos cannot change any stored point either.
    p.downsampleTiers.push_back(tier(0, 0));
    timestar::retention::normalizeRetentionTiers(p);
    EXPECT_FALSE(timestar::retention::policyIsActionable(p));
}

// ===========================================================================
// Migration: legacy object <-> tier list, both directions
// ===========================================================================

class RetentionCascadeMigrationTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }
    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

// The exact bytes a pre-cascade binary wrote: `downsample` object, no
// `downsampleTiers` key. This is the read path getRetentionPolicy performs.
TEST_F(RetentionCascadeMigrationTest, LegacyRecordPromotesToOneTier) {
    const std::string legacy = R"({
        "measurement":"temperature",
        "ttl":"90d",
        "ttlNanos":7776000000000000,
        "downsample":{"after":"7d","afterNanos":604800000000000,
                      "interval":"1h","intervalNanos":3600000000000,"method":"avg"}
    })";

    RetentionPolicy p;
    ASSERT_FALSE(static_cast<bool>(glz::read_json(p, legacy)));
    EXPECT_TRUE(p.downsampleTiers.empty()) << "the stored record genuinely has no tier list";

    timestar::retention::normalizeRetentionTiers(p);
    ASSERT_EQ(p.downsampleTiers.size(), 1u);
    EXPECT_EQ(p.downsampleTiers[0].interval, "1h");
    EXPECT_EQ(p.downsampleTiers[0].intervalNanos, 3600000000000ULL);
    EXPECT_EQ(p.downsampleTiers[0].method, "avg");
    // The legacy field is untouched, so a policy that round-trips through the
    // new binary is byte-identical to the old one in that field.
    ASSERT_TRUE(p.downsample.has_value());
    EXPECT_EQ(p.downsample->intervalNanos, 3600000000000ULL);
    EXPECT_EQ(validationError(p), "");
}

// A record with NO downsampling at all must not grow a phantom tier.
TEST_F(RetentionCascadeMigrationTest, LegacyTtlOnlyRecordStaysTierless) {
    RetentionPolicy p;
    ASSERT_FALSE(
        static_cast<bool>(glz::read_json(p, R"({"measurement":"cpu","ttl":"30d","ttlNanos":2592000000000000})")));
    timestar::retention::normalizeRetentionTiers(p);
    EXPECT_TRUE(p.downsampleTiers.empty());
    EXPECT_FALSE(p.downsample.has_value());
}

// The shape a binary that PREDATES the cascade compiles: no `downsampleTiers`
// member at all. Parsing the new record through THIS is the only honest
// simulation of a downgrade — reading it back into the current RetentionPolicy
// tests nothing, because that struct knows the field.
namespace {

struct PreCascadeRetentionPolicy {
    std::string measurement;
    std::string ttl;
    uint64_t ttlNanos = 0;
    std::optional<DownsamplePolicy> downsample;
};

}  // namespace

template <>
struct glz::meta<PreCascadeRetentionPolicy> {
    using T = PreCascadeRetentionPolicy;
    static constexpr auto value =
        object("measurement", &T::measurement, "ttl", &T::ttl, "ttlNanos", &T::ttlNanos, "downsample", &T::downsample);
};

// Forward direction: a new record must always carry the legacy mirror, so a
// reader that knows only the legacy field applies the FINEST tier — finer than
// intended, never coarser, never destructive.
//
// The mirror is necessary but NOT sufficient, and the boundary is pinned here:
// it only helps a reader that tolerates the unknown `downsampleTiers` key.
TEST_F(RetentionCascadeMigrationTest, NewRecordMirrorsFinestTierIntoLegacyField) {
    auto p =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 730 * ONE_DAY_NS);

    auto json = glz::write_json(p);
    ASSERT_TRUE(json.has_value());
    EXPECT_NE(json->find("downsampleTiers"), std::string::npos);
    EXPECT_NE(json->find("\"downsample\":"), std::string::npos)
        << "a persisted record with no legacy mirror is invisible to any legacy reader";

    // A STRICT legacy reader (Glaze's default) does not degrade gracefully: it
    // fails the whole document on the unknown key and loses the policy, TTL
    // included. This is what a downgrade to a build that predates the cascade
    // actually does — documented in docs/api-retention.md rather than papered
    // over, because no change to THIS binary can fix a binary that shipped.
    PreCascadeRetentionPolicy strict;
    EXPECT_TRUE(static_cast<bool>(glz::read_json(strict, *json)))
        << "if this ever starts succeeding, Glaze's unknown-key default changed and the downgrade "
           "caveat in docs/api-retention.md can be dropped";

    // A LENIENT legacy reader — which is what NativeIndex now is
    // (kRetentionReadOpts) — gets exactly the intended degradation: tier 0.
    static constexpr glz::opts lenient{.error_on_unknown_keys = false};
    PreCascadeRetentionPolicy rolledBack;
    ASSERT_FALSE(static_cast<bool>(glz::read<lenient>(rolledBack, *json)));
    EXPECT_EQ(rolledBack.ttlNanos, 730 * ONE_DAY_NS) << "the TTL must survive a downgrade";
    ASSERT_TRUE(rolledBack.downsample.has_value());
    EXPECT_EQ(rolledBack.downsample->intervalNanos, ONE_MINUTE_NS)
        << "the mirror must be the FINEST tier: applying it alone keeps MORE data than intended, "
           "which is the only safe degradation";

    // And that legacy view, normalized, is exactly a one-tier policy.
    RetentionPolicy legacyView;
    legacyView.measurement = rolledBack.measurement;
    legacyView.ttlNanos = rolledBack.ttlNanos;
    legacyView.downsample = rolledBack.downsample;
    timestar::retention::normalizeRetentionTiers(legacyView);
    ASSERT_EQ(legacyView.downsampleTiers.size(), 1u);
    EXPECT_EQ(legacyView.downsampleTiers[0].intervalNanos, ONE_MINUTE_NS);
}

TEST_F(RetentionCascadeMigrationTest, NormalizeIsIdempotent) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)});
    auto once = glz::write_json(p).value();
    timestar::retention::normalizeRetentionTiers(p);
    timestar::retention::normalizeRetentionTiers(p);
    EXPECT_EQ(glz::write_json(p).value(), once);
}

SEASTAR_TEST_F(RetentionCascadeMigrationTest, IndexRoundTripsACascade) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    auto p =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS)}, 730 * ONE_DAY_NS);
    co_await index.setRetentionPolicy(p);

    auto got = co_await index.getRetentionPolicy("scada");
    EXPECT_TRUE(got.has_value());
    if (got.has_value()) {
        EXPECT_EQ(got->downsampleTiers.size(), 2u);
        EXPECT_EQ(got->downsampleTiers[0].intervalNanos, ONE_MINUTE_NS);
        EXPECT_EQ(got->downsampleTiers[1].intervalNanos, FIFTEEN_MINUTES_NS);
        EXPECT_TRUE(got->downsample.has_value());
        EXPECT_EQ(got->downsample->intervalNanos, ONE_MINUTE_NS);
    }

    // A single-tier policy written the LEGACY way (only `downsample` set, as an
    // old in-memory struct would have it) must come back as one tier.
    RetentionPolicy legacy;
    legacy.measurement = "legacy";
    legacy.downsample = tier(7 * ONE_DAY_NS, ONE_MINUTE_NS);
    co_await index.setRetentionPolicy(legacy);
    auto gotLegacy = co_await index.getRetentionPolicy("legacy");
    EXPECT_TRUE(gotLegacy.has_value());
    if (gotLegacy.has_value()) {
        EXPECT_EQ(gotLegacy->downsampleTiers.size(), 1u);
        EXPECT_EQ(gotLegacy->downsampleTiers[0].intervalNanos, ONE_MINUTE_NS);
    }

    auto all = co_await index.getAllRetentionPolicies();
    EXPECT_EQ(all.size(), 2u);
    for (const auto& q : all) {
        EXPECT_FALSE(q.downsampleTiers.empty()) << "getAllRetentionPolicies must normalize too";
    }

    co_await index.close();
}

// A retention record written by a FUTURE version — one that grew a field this
// binary does not know (Phase 4's per-field methods are the next one) — must
// still load, applying everything it does understand. A strict read fails the
// whole document instead, silently dropping the TTL as well as the cascade;
// that is precisely the failure mode the legacy `downsample` mirror exists to
// avoid and cannot avoid on its own.
TEST_F(RetentionCascadeMigrationTest, RecordWithAnUnknownFutureFieldStillLoads) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS)}, 730 * ONE_DAY_NS);
    auto json = glz::write_json(p);
    ASSERT_TRUE(json.has_value());

    // A record left by a NEWER writer: same document plus one field this binary
    // does not know (Phase 4's per-field methods are the next such field).
    std::string doctored = *json;
    doctored.insert(1, R"("fieldMethods":{"flow_total":"max"},)");

    // Glaze's DEFAULT read fails the whole document — dropping the TTL as well
    // as the cascade. That is why NativeIndex does not use it.
    RetentionPolicy strict;
    EXPECT_TRUE(static_cast<bool>(glz::read_json(strict, doctored)));

    // These are the options NativeIndex reads retention records with
    // (kRetentionReadOpts, native_index.cpp). Keep them in step: this is the
    // only thing that stops the NEXT schema addition from silently disabling
    // retention on every binary one version behind.
    static constexpr glz::opts kRetentionReadOpts{.error_on_unknown_keys = false};
    RetentionPolicy reread;
    ASSERT_FALSE(static_cast<bool>(glz::read<kRetentionReadOpts>(reread, doctored)))
        << "a record carrying a field this binary does not know must still load";
    timestar::retention::normalizeRetentionTiers(reread);
    EXPECT_EQ(reread.ttlNanos, 730 * ONE_DAY_NS);
    EXPECT_EQ(reread.downsampleTiers.size(), 1u);
    EXPECT_EQ(reread.downsampleTiers[0].intervalNanos, ONE_MINUTE_NS);
}

// ===========================================================================
// Protobuf: cascade on the wire, with the same mirroring rules
// ===========================================================================

class RetentionCascadeProtoTest : public ::testing::Test {};

TEST_F(RetentionCascadeProtoTest, PutRequestCarriesTiers) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("scada");
    req.set_ttl("730d");
    for (auto [after, interval] :
         {std::pair<const char*, const char*>{"7d", "1m"}, std::pair<const char*, const char*>{"90d", "15m"}}) {
        auto* t = req.add_downsample_tiers();
        t->set_after(after);
        t->set_interval(interval);
        t->set_method("avg");
    }

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));

    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.measurement, "scada");
    ASSERT_EQ(parsed.downsampleTiers.size(), 2u);
    EXPECT_EQ(parsed.downsampleTiers[0].interval, "1m");
    EXPECT_EQ(parsed.downsampleTiers[1].interval, "15m");
    EXPECT_FALSE(parsed.downsample.has_value()) << "a tier-only request sets no legacy field";
}

// An OLD client sends only `downsample`. proto3 repeated-absent is empty, so it
// lands with no tiers and the handler promotes the legacy field.
TEST_F(RetentionCascadeProtoTest, LegacySingularRequestStillParses) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("cpu");
    auto* ds = req.mutable_downsample();
    ds->set_after("7d");
    ds->set_interval("1h");
    ds->set_method("max");

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));

    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());
    EXPECT_TRUE(parsed.downsampleTiers.empty());
    ASSERT_TRUE(parsed.downsample.has_value());
    EXPECT_EQ(parsed.downsample->method, "max");
}

TEST_F(RetentionCascadeProtoTest, GetResponseEmitsBothRepresentations) {
    timestar::proto::RetentionPolicyData data;
    data.measurement = "scada";
    data.ttl = "730d";
    data.ttlNanos = 730 * ONE_DAY_NS;
    for (auto [interval, nanos] : {std::pair<const char*, uint64_t>{"1m", ONE_MINUTE_NS},
                                   std::pair<const char*, uint64_t>{"15m", FIFTEEN_MINUTES_NS}}) {
        timestar::proto::ParsedRetentionPutRequest::DownsampleData t;
        t.interval = interval;
        t.intervalNanos = nanos;
        t.method = "avg";
        data.downsampleTiers.push_back(t);
    }
    data.downsample = data.downsampleTiers.front();

    auto bytes = timestar::proto::formatRetentionGetResponse(data);
    ::timestar_pb::RetentionGetResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.policy().downsample_tiers_size(), 2);
    EXPECT_EQ(resp.policy().downsample_tiers(1).interval(), "15m");
    // The legacy field must mirror the FINEST tier for pre-cascade readers.
    EXPECT_TRUE(resp.policy().has_downsample());
    EXPECT_EQ(resp.policy().downsample().interval(), "1m");
}

// Validation is transport-independent: the same rules must reject the same
// policy whether it arrived as JSON or protobuf.
TEST_F(RetentionCascadeProtoTest, ValidationRejectsTheSamePolicyOverProto) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("scada");
    for (auto [afterNs, intervalNs] : {std::pair<uint64_t, uint64_t>{7 * ONE_DAY_NS, 10 * ONE_MINUTE_NS},
                                       std::pair<uint64_t, uint64_t>{30 * ONE_DAY_NS, 25 * ONE_MINUTE_NS}}) {
        auto* t = req.add_downsample_tiers();
        t->set_after(std::to_string(afterNs) + "ns");
        t->set_after_nanos(afterNs);
        t->set_interval(std::to_string(intervalNs) + "ns");
        t->set_interval_nanos(intervalNs);
        t->set_method("avg");
    }

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));
    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());

    RetentionPolicy p;
    p.measurement = parsed.measurement;
    for (const auto& t : parsed.downsampleTiers) {
        DownsamplePolicy ds;
        ds.after = t.after;
        ds.afterNanos = t.afterNanos;
        ds.interval = t.interval;
        ds.intervalNanos = t.intervalNanos;
        ds.method = t.method;
        p.downsampleTiers.push_back(std::move(ds));
    }
    timestar::retention::normalizeRetentionTiers(p);

    EXPECT_NE(validationError(p).find("exact multiple"), std::string::npos) << validationError(p);
}

/*
 * Per-field downsample methods (Phase 4 of docs/downsampling-cascade-plan.md).
 *
 * This file covers the SCHEMA half: which policies are accepted, how a field's
 * method resolves, what goes on the wire, and what happens when a record
 * carrying `fieldMethods` is read by a binary that has never heard of it.
 *
 * The fold itself is in test/unit/storage/downsample_field_methods_test.cpp.
 */

#include "../../../lib/http/proto_converters.hpp"
#include "../../../lib/retention/retention_policy.hpp"

#include "timestar.pb.h"

#include <gtest/gtest.h>

#include <optional>
#include <set>
#include <string>
#include <vector>

namespace {

constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;
constexpr uint64_t ONE_MINUTE_NS = 60ULL * NS_PER_SEC;
constexpr uint64_t FIFTEEN_MINUTES_NS = 15ULL * ONE_MINUTE_NS;
constexpr uint64_t ONE_DAY_NS = 24ULL * 3600ULL * NS_PER_SEC;

DownsamplePolicy tier(uint64_t afterNanos, uint64_t intervalNanos, const std::string& method = "avg",
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

std::string validationError(const RetentionPolicy& p, const timestar::retention::FieldTypeProbe& probe = {}) {
    return timestar::retention::validateRetentionPolicy(p, probe).value_or("");
}

// A probe standing in for the field-type index: names listed here are stored as
// Boolean/String, anything else is a numeric field, and an unlisted-and-unknown
// name answers "no idea".
timestar::retention::FieldTypeProbe probeWith(std::vector<std::string> nonNumeric, std::vector<std::string> numeric) {
    return [nonNumeric = std::move(nonNumeric),
            numeric = std::move(numeric)](const std::string& field) -> std::optional<bool> {
        if (std::find(nonNumeric.begin(), nonNumeric.end(), field) != nonNumeric.end()) {
            return true;
        }
        if (std::find(numeric.begin(), numeric.end(), field) != numeric.end()) {
            return false;
        }
        return std::nullopt;  // never seen by the index
    };
}

}  // namespace

// ===========================================================================
// Resolution
// ===========================================================================

class FieldMethodResolutionTest : public ::testing::Test {};

TEST_F(FieldMethodResolutionTest, NamedFieldTakesItsOverrideOthersTakeTheTierMethod) {
    auto t = tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}, {"status", "latest"}});
    EXPECT_EQ(timestar::retention::tierMethodForField(t, "flow_total"), "max");
    EXPECT_EQ(timestar::retention::tierMethodForField(t, "status"), "latest");
    EXPECT_EQ(timestar::retention::tierMethodForField(t, "level"), "avg");
    EXPECT_EQ(timestar::retention::tierMethodForField(t, ""), "avg");
}

// The whole default population: nothing about a policy without `fieldMethods`
// may differ, including the cheap probes the compaction path gates on.
TEST_F(FieldMethodResolutionTest, AbsentFieldMethodsIsIndistinguishableFromEmpty) {
    auto plain = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS)});
    EXPECT_FALSE(plain.downsampleTiers[0].fieldMethods.has_value());
    EXPECT_TRUE(timestar::retention::fieldMethodsOf(plain.downsampleTiers[0]).empty());
    EXPECT_FALSE(timestar::retention::policyHasFieldMethods(plain));
    EXPECT_TRUE(timestar::retention::nonNumericFoldFields(plain).empty());
    EXPECT_EQ(timestar::retention::tierMethodForField(plain.downsampleTiers[0], "anything"), "avg");
}

// A policy with no overrides must SERIALISE exactly as it did before the field
// existed — both to the API and into the 0x0B record. If glaze ever starts
// emitting `"fieldMethods":{}` for a disengaged optional, every persisted
// record grows a key for no reason and this catches it.
TEST_F(FieldMethodResolutionTest, PolicyWithoutOverridesSerialisesWithoutTheKey) {
    auto plain = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS)}, 730 * ONE_DAY_NS);
    auto json = glz::write_json(plain);
    ASSERT_TRUE(json.has_value());
    EXPECT_EQ(json->find("fieldMethods"), std::string::npos) << *json;
}

TEST_F(FieldMethodResolutionTest, OverridesRoundTripThroughJson) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}})}, 730 * ONE_DAY_NS);
    auto json = glz::write_json(p);
    ASSERT_TRUE(json.has_value());

    RetentionPolicy back;
    ASSERT_FALSE(static_cast<bool>(glz::read_json(back, *json)));
    timestar::retention::normalizeRetentionTiers(back);
    ASSERT_EQ(back.downsampleTiers.size(), 1u);
    EXPECT_EQ(timestar::retention::tierMethodForField(back.downsampleTiers[0], "flow_total"), "max");
    // The legacy mirror carries them too, so a single-tier reader sees the same
    // per-field routing rather than averaging a totalizer.
    ASSERT_TRUE(back.downsample.has_value());
    EXPECT_EQ(timestar::retention::tierMethodForField(*back.downsample, "flow_total"), "max");
}

// ===========================================================================
// Validation
// ===========================================================================

class FieldMethodValidationTest : public ::testing::Test {};

TEST_F(FieldMethodValidationTest, TargetScadaPolicyAccepted) {
    const std::map<std::string, std::string> fm{{"flow_total", "max"}, {"status", "latest"}};
    auto p =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", fm), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", fm)},
                 730 * ONE_DAY_NS);
    EXPECT_EQ(validationError(p, probeWith({"status"}, {"flow_total", "level"})), "");
}

TEST_F(FieldMethodValidationTest, UnknownMethodRejected) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "p99"}})});
    EXPECT_NE(validationError(p).find("fieldMethods['flow_total']"), std::string::npos) << validationError(p);
    // COUNT is excluded from the allowed set for the same reason it is excluded
    // as a tier method: a count of counts is not a count.
    auto counted = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"n", "count"}})});
    EXPECT_NE(validationError(counted).find("fieldMethods['n']"), std::string::npos);
}

TEST_F(FieldMethodValidationTest, EmptyFieldNameRejected) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"", "max"}})});
    EXPECT_NE(validationError(p).find("empty field name"), std::string::npos) << validationError(p);
}

// A field named in one tier and omitted from another silently falls back to
// that tier's method there — `avg` composing over what the operator asked to
// keep as `max`. Rejected, not resolved by guesswork.
TEST_F(FieldMethodValidationTest, FieldMustResolveToTheSameMethodInEveryTier) {
    auto omitted = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}}),
                             tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg")});
    EXPECT_NE(validationError(omitted).find("same method for field 'flow_total'"), std::string::npos)
        << validationError(omitted);

    auto disagreeing = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}}),
                                 tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", {{"flow_total", "sum"}})});
    EXPECT_NE(validationError(disagreeing).find("same method for field 'flow_total'"), std::string::npos)
        << validationError(disagreeing);

    const std::map<std::string, std::string> fm{{"flow_total", "max"}};
    auto consistent = policyOf(
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", fm), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", fm)});
    EXPECT_EQ(validationError(consistent), "");
}

// REGRESSION. A field named in only SOME tiers, where the tier's own method
// happens to equal the override, used to VALIDATE — the cross-tier rule
// compared the RESOLVED methods, and `"method": "latest"` plus
// {"status": "latest"} in the second tier only resolves to "latest" in both.
//
// It is not a cosmetic acceptance. `nonNumericFold` — the ONLY route by which a
// Boolean/String series folds — has two derivations that this policy makes
// disagree:
//
//   * the compactor reads TIER 0's fieldMethods (tsm_compactor.cpp,
//     `resolvedByKey`), so "status" gets the measurement DEFAULT context and
//     does not fold;
//   * Engine::sweepDownsampleRewrites() uses nonNumericFoldFields(), which
//     unions EVERY tier, so it treats the series as opted in and nominates its
//     file for a downsample rewrite.
//
// The rewrite therefore folds nothing, the file comes back equivalent, and the
// next sweep nominates it again — an endless rewrite loop, two files per shard
// per sweep period, forever. docs/api-retention.md already states the rule as
// "named in all of them"; this is the code catching up to it.
TEST_F(FieldMethodValidationTest, FieldNamedInOnlySomeTiersIsRejectedEvenWhenTheMethodsCoincide) {
    auto laterTierOnly = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest"),
                                   tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "latest", {{"status", "latest"}})});
    EXPECT_NE(validationError(laterTierOnly).find("same method for field 'status'"), std::string::npos)
        << "accepted: '" << validationError(laterTierOnly) << "'";

    // ...and the mirror image (named in tier 0, omitted later), which the
    // resolved-method comparison already caught only because the methods
    // differed. Here they do not.
    auto firstTierOnly = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest", {{"status", "latest"}}),
                                   tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "latest")});
    EXPECT_NE(validationError(firstTierOnly).find("same method for field 'status'"), std::string::npos)
        << "accepted: '" << validationError(firstTierOnly) << "'";
}

// THE INVARIANT THE FIX BUYS, asserted directly rather than through one example:
// for every policy that validates, the compactor's view of the non-numeric
// opt-in set (tier 0's `latest` overrides) and the sweep's
// (nonNumericFoldFields, unioned over all tiers) must be the SAME SET. They are
// the two gates on the same destructive decision, and a disagreement is either
// a status word silently folded or a file rewritten forever.
TEST_F(FieldMethodValidationTest, SweepAndCompactorAgreeOnTheNonNumericOptInSet) {
    const std::vector<std::vector<DownsamplePolicy>> shapes{
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest")},
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest", {{"status", "latest"}})},
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest"),
         tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "latest", {{"status", "latest"}})},
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest", {{"status", "latest"}}),
         tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "latest")},
        {tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"status", "latest"}, {"flow", "max"}}),
         tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", {{"status", "latest"}, {"flow", "max"}})},
    };

    for (size_t i = 0; i < shapes.size(); ++i) {
        auto p = policyOf(shapes[i]);
        if (!validationError(p).empty()) {
            continue;  // a rejected policy never reaches either gate
        }
        // The compactor's view: tier 0's explicit `latest` overrides.
        std::set<std::string> compactorView;
        for (const auto& [field, method] : timestar::retention::fieldMethodsOf(p.downsampleTiers.front())) {
            if (method == "latest") {
                compactorView.insert(field);
            }
        }
        const auto swept = timestar::retention::nonNumericFoldFields(p);
        const std::set<std::string> sweepView(swept.begin(), swept.end());
        EXPECT_EQ(compactorView, sweepView) << "shape " << i
                                            << " validates but the two opt-in derivations disagree; "
                                               "the sweep would nominate a file the compactor refuses to fold";
    }
}

// The rule the plan calls out explicitly: a boolean/string field may only be
// given `latest`, mirroring how DerivedQueryExecutor refuses a non-numeric
// operand rather than coercing it to 1.0/0.0.
TEST_F(FieldMethodValidationTest, OnlyLatestMayDownsampleANonNumericField) {
    auto probe = probeWith({"status", "note"}, {"level"});

    for (const char* method : {"avg", "min", "max", "sum"}) {
        auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"status", method}})});
        const auto why = validationError(p, probe);
        EXPECT_NE(why.find("only 'latest' may downsample a non-numeric field"), std::string::npos)
            << "method=" << method << " why=" << why;
    }

    auto ok = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"status", "latest"}})});
    EXPECT_EQ(validationError(ok, probe), "");

    // Numeric fields keep the full method set.
    auto numeric = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"level", "min"}})});
    EXPECT_EQ(validationError(numeric, probe), "");
}

// A policy may legitimately be installed before the field's first write, so an
// unknown type must not block it. The compactor's fold-time gate (which knows
// the series' real TSMValueType) is what stops the wrong fold in that case.
TEST_F(FieldMethodValidationTest, UnknownFieldTypeIsAccepted) {
    auto p = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"not_yet_written", "sum"}})});
    EXPECT_EQ(validationError(p, probeWith({"status"}, {"level"})), "");
    // ...and with no probe at all (the compactor's own call shape).
    EXPECT_EQ(validationError(p), "");
}

// nonNumericFoldFields() is what the sweep and the compactor use to decide
// "may a Boolean/String series fold at all?". It must name ONLY explicit
// `latest` overrides — a measurement-wide `"method": "latest"` must not opt
// non-numeric fields in, or a policy written before Phase 4 would start
// destroying status history on upgrade.
TEST_F(FieldMethodValidationTest, MeasurementWideLatestDoesNotOptNonNumericFieldsIn) {
    auto wide = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "latest")});
    EXPECT_TRUE(timestar::retention::nonNumericFoldFields(wide).empty());

    auto explicitOptIn = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"status", "latest"}})});
    ASSERT_EQ(timestar::retention::nonNumericFoldFields(explicitOptIn).size(), 1u);
    EXPECT_EQ(timestar::retention::nonNumericFoldFields(explicitOptIn)[0], "status");

    // A non-`latest` override is not an opt-in either.
    auto maxed = policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", {{"flow_total", "max"}})});
    EXPECT_TRUE(timestar::retention::nonNumericFoldFields(maxed).empty());
}

// ===========================================================================
// Protobuf
// ===========================================================================

class FieldMethodProtoTest : public ::testing::Test {};

TEST_F(FieldMethodProtoTest, OverridesSurviveThePutRequest) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("scada");
    auto* t = req.add_downsample_tiers();
    t->set_after("7d");
    t->set_interval("1m");
    t->set_method("avg");
    (*t->mutable_field_methods())["flow_total"] = "max";
    (*t->mutable_field_methods())["status"] = "latest";

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));

    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());
    ASSERT_EQ(parsed.downsampleTiers.size(), 1u);
    EXPECT_EQ(parsed.downsampleTiers[0].fieldMethods.at("flow_total"), "max");
    EXPECT_EQ(parsed.downsampleTiers[0].fieldMethods.at("status"), "latest");
}

// proto3 maps are absent-as-empty, so a client that predates the field is
// indistinguishable from one that sets no overrides.
TEST_F(FieldMethodProtoTest, PreFieldMethodsClientParsesAsNoOverrides) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("cpu");
    auto* ds = req.mutable_downsample();
    ds->set_after("7d");
    ds->set_interval("1h");
    ds->set_method("max");

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));
    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());
    ASSERT_TRUE(parsed.downsample.has_value());
    EXPECT_TRUE(parsed.downsample->fieldMethods.empty());
}

TEST_F(FieldMethodProtoTest, GetResponseCarriesOverridesInBothRepresentations) {
    timestar::proto::RetentionPolicyData data;
    data.measurement = "scada";
    timestar::proto::ParsedRetentionPutRequest::DownsampleData t;
    t.interval = "1m";
    t.intervalNanos = ONE_MINUTE_NS;
    t.method = "avg";
    t.fieldMethods = {{"flow_total", "max"}};
    data.downsampleTiers.push_back(t);
    data.downsample = t;

    auto bytes = timestar::proto::formatRetentionGetResponse(data);
    ::timestar_pb::RetentionGetResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    ASSERT_EQ(resp.policy().downsample_tiers_size(), 1);
    EXPECT_EQ(resp.policy().downsample_tiers(0).field_methods().at("flow_total"), "max");
    ASSERT_TRUE(resp.policy().has_downsample());
    EXPECT_EQ(resp.policy().downsample().field_methods().at("flow_total"), "max");
}

// Validation is transport-independent: the same policy must be rejected for the
// same reason whether it arrived as JSON or protobuf.
TEST_F(FieldMethodProtoTest, ValidationRejectsTheSamePolicyOverProto) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("scada");
    auto* t = req.add_downsample_tiers();
    t->set_after(std::to_string(7 * ONE_DAY_NS) + "ns");
    t->set_after_nanos(7 * ONE_DAY_NS);
    t->set_interval(std::to_string(ONE_MINUTE_NS) + "ns");
    t->set_interval_nanos(ONE_MINUTE_NS);
    t->set_method("avg");
    (*t->mutable_field_methods())["status"] = "avg";      // wrong for a boolean
    (*t->mutable_field_methods())["flow_total"] = "p99";  // not a method at all

    std::string bytes;
    ASSERT_TRUE(req.SerializeToString(&bytes));
    auto parsed = timestar::proto::parseRetentionPutRequest(bytes.data(), bytes.size());

    RetentionPolicy p;
    p.measurement = parsed.measurement;
    for (const auto& src : parsed.downsampleTiers) {
        DownsamplePolicy ds;
        ds.after = src.after;
        ds.afterNanos = src.afterNanos;
        ds.interval = src.interval;
        ds.intervalNanos = src.intervalNanos;
        ds.method = src.method;
        ds.fieldMethods = src.fieldMethods;
        p.downsampleTiers.push_back(std::move(ds));
    }
    timestar::retention::normalizeRetentionTiers(p);

    // Unknown method first (it is a per-tier check, run before the cross-tier
    // and type rules).
    EXPECT_NE(validationError(p).find("fieldMethods['flow_total']"), std::string::npos) << validationError(p);

    // With the unusable method removed, the boolean rule still fires.
    p.downsampleTiers[0].fieldMethods->erase("flow_total");
    timestar::retention::normalizeRetentionTiers(p);
    EXPECT_NE(validationError(p, probeWith({"status"}, {})).find("only 'latest' may downsample a non-numeric field"),
              std::string::npos);
}

// ===========================================================================
// Downgrade
//
// `fieldMethods` is the SECOND field to land in the 0x0B record. Phase 2's
// review found the first one broke rollback: glaze rejects unknown keys by
// default, so a reader one version behind failed the WHOLE document and dropped
// the policy, TTL included. The fix was to read retention records leniently
// (kRetentionReadOpts, native_index.cpp). This is that fix, re-proven for the
// next field — against a struct that GENUINELY lacks it, since a reader that
// knows the field proves nothing.
// ===========================================================================

namespace {

// What a binary compiled before per-field methods sees. Note the nested tier
// struct also lacks the field — using the current DownsamplePolicy here would
// silently make the test tautological.
struct PreFieldMethodsTier {
    std::string after;
    uint64_t afterNanos = 0;
    std::string interval;
    uint64_t intervalNanos = 0;
    std::string method;
};

struct PreFieldMethodsPolicy {
    std::string measurement;
    std::string ttl;
    uint64_t ttlNanos = 0;
    std::optional<PreFieldMethodsTier> downsample;
    std::vector<PreFieldMethodsTier> downsampleTiers;
};

}  // namespace

template <>
struct glz::meta<PreFieldMethodsTier> {
    using T = PreFieldMethodsTier;
    static constexpr auto value = object("after", &T::after, "afterNanos", &T::afterNanos, "interval", &T::interval,
                                         "intervalNanos", &T::intervalNanos, "method", &T::method);
};

template <>
struct glz::meta<PreFieldMethodsPolicy> {
    using T = PreFieldMethodsPolicy;
    static constexpr auto value = object("measurement", &T::measurement, "ttl", &T::ttl, "ttlNanos", &T::ttlNanos,
                                         "downsample", &T::downsample, "downsampleTiers", &T::downsampleTiers);
};

class FieldMethodDowngradeTest : public ::testing::Test {};

TEST_F(FieldMethodDowngradeTest, RecordWithFieldMethodsLoadsInAFieldIgnorantReader) {
    const std::map<std::string, std::string> fm{{"flow_total", "max"}, {"status", "latest"}};
    auto p =
        policyOf({tier(7 * ONE_DAY_NS, ONE_MINUTE_NS, "avg", fm), tier(90 * ONE_DAY_NS, FIFTEEN_MINUTES_NS, "avg", fm)},
                 730 * ONE_DAY_NS);
    auto json = glz::write_json(p);
    ASSERT_TRUE(json.has_value());
    ASSERT_NE(json->find("fieldMethods"), std::string::npos) << "fixture must actually contain the new key";

    // Glaze's DEFAULT read fails the whole document. This is exactly the Phase 2
    // defect, reproduced for the new field: a strict reader loses the TTL too.
    PreFieldMethodsPolicy strict;
    EXPECT_TRUE(static_cast<bool>(glz::read_json(strict, *json)))
        << "if this starts succeeding, glaze's unknown-key default changed";

    // The options NativeIndex actually reads retention records with. Keep in
    // step with kRetentionReadOpts in native_index.cpp.
    static constexpr glz::opts kRetentionReadOpts{.error_on_unknown_keys = false};
    PreFieldMethodsPolicy rolledBack;
    ASSERT_FALSE(static_cast<bool>(glz::read<kRetentionReadOpts>(rolledBack, *json)))
        << "a record carrying fieldMethods must still load in a reader that has never heard of it";

    EXPECT_EQ(rolledBack.ttlNanos, 730 * ONE_DAY_NS) << "the TTL must survive the downgrade";
    ASSERT_EQ(rolledBack.downsampleTiers.size(), 2u) << "the cascade must survive the downgrade";
    EXPECT_EQ(rolledBack.downsampleTiers[0].intervalNanos, ONE_MINUTE_NS);
    EXPECT_EQ(rolledBack.downsampleTiers[1].intervalNanos, FIFTEEN_MINUTES_NS);
    EXPECT_EQ(rolledBack.downsampleTiers[0].method, "avg");
    // The legacy mirror survives too, so an even older reader gets tier 0.
    ASSERT_TRUE(rolledBack.downsample.has_value());
    EXPECT_EQ(rolledBack.downsample->intervalNanos, ONE_MINUTE_NS);

    // The DEGRADATION is the honest part: the old binary folds every field with
    // the tier method, so a totalizer it would have kept as `max` is averaged.
    // Finer-grained routing is lost; no data is deleted that would not have been
    // folded anyway. Stated here so a future reader does not mistake "loads" for
    // "behaves identically".
    EXPECT_EQ(rolledBack.downsampleTiers[0].method, "avg");
}

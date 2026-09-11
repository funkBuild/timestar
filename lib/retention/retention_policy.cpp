#include "retention_policy.hpp"

#include <algorithm>
#include <array>
#include <set>
#include <string>

namespace timestar::retention {

namespace {

constexpr std::array<std::string_view, 5> kMethods{"avg", "min", "max", "sum", "latest"};

// "downsample" for a single tier (so legacy single-tier bodies keep the exact
// error text they have always produced), "downsample[k]" once it is a list.
std::string tierLabel(size_t index, size_t total) {
    if (total <= 1) {
        return "downsample";
    }
    return "downsample[" + std::to_string(index) + "]";
}

}  // namespace

bool isValidDownsampleMethod(std::string_view method) {
    for (auto m : kMethods) {
        if (m == method) {
            return true;
        }
    }
    return false;
}

const std::map<std::string, std::string>& fieldMethodsOf(const DownsamplePolicy& tier) {
    static const std::map<std::string, std::string> kNone;
    return tier.fieldMethods.has_value() ? *tier.fieldMethods : kNone;
}

const std::string& tierMethodForField(const DownsamplePolicy& tier, const std::string& field) {
    const auto& overrides = fieldMethodsOf(tier);
    auto it = overrides.find(field);
    return it != overrides.end() ? it->second : tier.method;
}

bool policyHasFieldMethods(const RetentionPolicy& policy) {
    for (const auto& tier : effectiveTiers(policy)) {
        if (!fieldMethodsOf(tier).empty()) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> nonNumericFoldFields(const RetentionPolicy& policy) {
    std::vector<std::string> fields;
    for (const auto& tier : effectiveTiers(policy)) {
        for (const auto& [field, method] : fieldMethodsOf(tier)) {
            if (method != "latest") {
                continue;
            }
            if (std::find(fields.begin(), fields.end(), field) == fields.end()) {
                fields.push_back(field);
            }
        }
    }
    return fields;
}

void normalizeRetentionTiers(RetentionPolicy& policy) {
    if (policy.downsampleTiers.empty()) {
        if (policy.downsample.has_value()) {
            policy.downsampleTiers.push_back(*policy.downsample);
        }
        return;
    }
    policy.downsample = policy.downsampleTiers.front();
}

std::vector<DownsamplePolicy> effectiveTiers(const RetentionPolicy& policy) {
    if (!policy.downsampleTiers.empty()) {
        return policy.downsampleTiers;
    }
    if (policy.downsample.has_value()) {
        return {*policy.downsample};
    }
    return {};
}

bool policyIsActionable(const RetentionPolicy& policy) {
    if (policy.ttlNanos > 0) {
        return true;
    }
    if (!policy.downsampleTiers.empty()) {
        for (const auto& tier : policy.downsampleTiers) {
            if (tier.afterNanos > 0 && tier.intervalNanos > 0) {
                return true;
            }
        }
        return false;
    }
    return policy.downsample.has_value() && policy.downsample->afterNanos > 0 && policy.downsample->intervalNanos > 0;
}

std::optional<std::string> validateRetentionPolicy(const RetentionPolicy& policy,
                                                   const FieldTypeProbe& nonNumericField) {
    const auto tiers = effectiveTiers(policy);
    if (tiers.empty()) {
        // TTL-only is a legitimate policy; nothing further to check.
        return std::nullopt;
    }

    if (tiers.size() > kMaxDownsampleTiers) {
        return "downsample accepts at most " + std::to_string(kMaxDownsampleTiers) + " tiers, got " +
               std::to_string(tiers.size());
    }

    for (size_t k = 0; k < tiers.size(); ++k) {
        const auto& tier = tiers[k];
        const std::string label = tierLabel(k, tiers.size());

        if (tier.method.empty()) {
            return label + ".method is required";
        }
        if (!isValidDownsampleMethod(tier.method)) {
            return "Invalid " + label + ".method: must be one of avg, min, max, sum, latest";
        }
        // One method for every tier in v1. Mixing (e.g. avg then max) has no
        // agreed composition semantics — max-of-averages is neither the max nor
        // the average of the raw points — so it is rejected rather than
        // silently given one. Relaxing this later is backward compatible.
        if (tier.method != tiers.front().method) {
            return "All downsample tiers must use the same method (got '" + tiers.front().method + "' and '" +
                   tier.method + "')";
        }
        for (const auto& [field, fieldMethod] : fieldMethodsOf(tier)) {
            if (field.empty()) {
                return label + ".fieldMethods contains an empty field name";
            }
            if (!isValidDownsampleMethod(fieldMethod)) {
                return "Invalid " + label + ".fieldMethods['" + field + "']: must be one of avg, min, max, sum, latest";
            }
        }
        if (tier.afterNanos == 0) {
            return label + ".after must be greater than zero";
        }
        if (tier.intervalNanos == 0) {
            return label + ".interval must be greater than zero";
        }
        // A stage whose threshold is shorter than its own bucket can never hold
        // a complete bucket, so it would fold partial buckets forever.
        if (tier.afterNanos < tier.intervalNanos) {
            return label + ".after (" + std::to_string(tier.afterNanos) + "ns) must be at least " + label +
                   ".interval (" + std::to_string(tier.intervalNanos) + "ns)";
        }

        if (k == 0) {
            continue;
        }
        const auto& prev = tiers[k - 1];
        if (tier.afterNanos <= prev.afterNanos) {
            return "downsample tier " + std::to_string(k) + " after (" + std::to_string(tier.afterNanos) +
                   "ns) must be strictly greater than tier " + std::to_string(k - 1) + " after (" +
                   std::to_string(prev.afterNanos) + "ns)";
        }
        if (tier.intervalNanos <= prev.intervalNanos) {
            return "downsample tier " + std::to_string(k) + " interval (" + std::to_string(tier.intervalNanos) +
                   "ns) must be strictly greater than tier " + std::to_string(k - 1) + " interval (" +
                   std::to_string(prev.intervalNanos) + "ns)";
        }
        // DIVISIBILITY. Buckets are epoch-aligned, so interval[k] | interval[k+1]
        // is exactly the condition under which every stage-k bucket lies wholly
        // inside one stage-k+1 bucket. Without it a stage-k bucket straddles a
        // stage-k+1 boundary and its aggregate is attributed to whichever side
        // its start timestamp fell on — data silently moved across buckets.
        if (tier.intervalNanos % prev.intervalNanos != 0) {
            return "downsample tier " + std::to_string(k) + " interval (" + std::to_string(tier.intervalNanos) +
                   "ns) must be an exact multiple of tier " + std::to_string(k - 1) + " interval (" +
                   std::to_string(prev.intervalNanos) + "ns)";
        }
    }

    // ONE METHOD PER FIELD, ACROSS ALL TIERS (the v1 rule, and the per-field
    // analogue of the one-method-per-policy rule above).
    //
    // Checked on the RESOLVED method, not on the override entry: a field named
    // in tier 0 but omitted from tier 1 silently falls back to tier 1's own
    // method there, so `avg` would compose over what the operator asked to keep
    // as `max`. Requiring the resolved methods to agree catches both that and a
    // straight disagreement between two explicit overrides.
    std::set<std::string> namedFields;
    for (const auto& tier : tiers) {
        for (const auto& [field, fieldMethod] : fieldMethodsOf(tier)) {
            (void)fieldMethod;
            namedFields.insert(field);
        }
    }
    // PRESENCE, not merely resolution. Comparing only the RESOLVED methods let a
    // field through whenever the tier's own method happened to equal the
    // override — `"method": "latest"` on every tier plus {"status": "latest"}
    // named in the SECOND tier only resolves to "latest" everywhere and used to
    // validate.
    //
    // That policy is not harmless. `nonNumericFold` — the one route by which a
    // Boolean/String series folds at all — is derived from TIER 0's fieldMethods
    // in the compactor (tsm_compactor.cpp, `resolvedByKey`) and from the union
    // over EVERY tier in Engine::sweepDownsampleRewrites()
    // (nonNumericFoldFields). Under such a policy the sweep nominated the file
    // for a downsample rewrite, the compactor declined to fold the series, the
    // rewrite produced an equivalent file, and the next sweep nominated it
    // again — an endless rewrite loop. Requiring presence in every tier makes
    // the two derivations identical by construction, and matches the rule
    // docs/api-retention.md already states.
    for (const auto& field : namedFields) {
        const std::string& first = tierMethodForField(tiers.front(), field);
        for (size_t k = 0; k < tiers.size(); ++k) {
            const auto& overrides = fieldMethodsOf(tiers[k]);
            auto it = overrides.find(field);
            if (it == overrides.end()) {
                return "All downsample tiers must use the same method for field '" + field + "' (tier " +
                       std::to_string(k) + " does not name it, so it would fall back to that tier's own method '" +
                       tiers[k].method + "'); name the field in every tier or in none";
            }
            if (it->second != first) {
                return "All downsample tiers must use the same method for field '" + field + "' (tier 0 resolves to '" +
                       first + "', tier " + std::to_string(k) + " to '" + it->second +
                       "'); name the field in every tier or in none";
            }
        }
    }

    // NON-NUMERIC FIELDS. Boolean and String pass through UNFOLDED by default:
    // storage-side folding is destructive and, for a SCADA status word, the
    // transition history is usually the point of retaining it. `latest` is the
    // one opt-in, and it matches the canonical query-time LATEST-per-bucket
    // rule (CLAUDE.md) so a folded status word reads the same way it always did.
    //
    // Anything else is REFUSED rather than coerced, mirroring
    // DerivedQueryExecutor's treatment of a non-numeric operand — the
    // alternative is silently averaging a boolean as 1.0/0.0, which this engine
    // forbids everywhere else.
    if (nonNumericField) {
        for (const auto& field : namedFields) {
            const std::string& resolved = tierMethodForField(tiers.front(), field);
            if (resolved == "latest") {
                continue;
            }
            auto isNonNumeric = nonNumericField(field);
            if (isNonNumeric.has_value() && *isNonNumeric) {
                return "downsample.fieldMethods['" + field + "'] is '" + resolved + "', but '" + field +
                       "' is a boolean/string field: only 'latest' may downsample a non-numeric field (values are "
                       "never coerced to 1.0/0.0)";
            }
        }
    }

    if (policy.ttlNanos > 0 && policy.ttlNanos <= tiers.back().afterNanos) {
        if (tiers.size() == 1) {
            return "ttl must be greater than downsample.after";
        }
        return "ttl must be greater than the last downsample tier's after (" + std::to_string(tiers.back().afterNanos) +
               "ns)";
    }

    return std::nullopt;
}

std::optional<std::string> parseDownsampleRequestField(const glz::raw_json& raw, std::vector<DownsamplePolicy>& out) {
    out.clear();

    const std::string& text = raw.str;
    size_t i = 0;
    while (i < text.size() && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r')) {
        ++i;
    }
    if (i >= text.size()) {
        return "downsample must be an object or an array of objects";
    }

    if (text[i] == 'n') {  // JSON null — treated as "no downsampling"
        return std::nullopt;
    }

    if (text[i] == '[') {
        std::vector<DownsamplePolicy> tiers;
        auto err = glz::read_json(tiers, text);
        if (err) {
            return "Invalid downsample array: " + std::string(glz::format_error(err));
        }
        if (tiers.empty()) {
            return "downsample array must contain at least one tier";
        }
        out = std::move(tiers);
        return std::nullopt;
    }

    if (text[i] == '{') {
        DownsamplePolicy tier;
        auto err = glz::read_json(tier, text);
        if (err) {
            return "Invalid downsample object: " + std::string(glz::format_error(err));
        }
        out.push_back(std::move(tier));
        return std::nullopt;
    }

    return "downsample must be an object or an array of objects";
}

uint8_t buildDownsampleStages(const RetentionPolicy& policy, uint64_t now,
                              std::array<DownsampleStage, kMaxDownsampleTiers>& out, std::string* invalidReason) {
    out = {};

    const auto tiers = effectiveTiers(policy);
    if (tiers.empty()) {
        return 0;
    }

    // A policy that would not survive validation must NOT fold. The original
    // code mapped any unrecognised method string to AVG, so a typo silently
    // averaged data the operator asked to keep as max/latest — destructive, and
    // invisible. Refusing to fold is the only non-destructive failure mode; TTL
    // still applies.
    if (auto why = validateRetentionPolicy(policy); why.has_value()) {
        if (invalidReason) {
            *invalidReason = *why;
        }
        return 0;
    }

    uint8_t stageCount = 0;
    for (const auto& tier : tiers) {
        if (stageCount >= kMaxDownsampleTiers) {
            break;
        }
        // `after` increases across tiers, so once one tier is not yet reachable
        // neither is any later one.
        if (now <= tier.afterNanos) {
            break;
        }
        // ALIGN THE THRESHOLD DOWN TO ITS OWN BUCKET GRID. Only COMPLETE
        // buckets may fold.
        //
        // With a raw `now - after` threshold, the bucket that contains it is
        // split: its [bucketStart, threshold) prefix folds and is emitted at
        // bucketStart, while the rest of the same bucket passes through raw.
        // The next compaction — threshold now past that bucket's end —
        // re-folds the partial aggregate TOGETHER with the raw remainder, i.e.
        // avg(avg(prefix), rest...): an unweighted fold-of-fold with wrong
        // weights, compounding once per compaction for as long as the moving
        // threshold sits inside the bucket.
        //
        // Aligned, a bucket only ever folds once it can no longer receive
        // points from the raw side, so re-folding a completed bucket's single
        // bucket-start point is genuinely idempotent for every offered method —
        // at every stage.
        const uint64_t interval = tier.intervalNanos;
        const uint64_t threshold = ((now - tier.afterNanos) / interval) * interval;
        if (threshold == 0) {
            break;  // nothing can be older than this stage
        }
        if (stageCount == 0) {
            out[stageCount++] = {threshold, interval};
            continue;
        }
        const uint64_t prevThreshold = out[stageCount - 1].threshold;
        if (threshold > prevThreshold) {
            // Cannot happen for a validated policy (divisibility forces
            // thresholds to be non-increasing). Only reachable from a direct
            // caller that built a policy by hand; drop the stage rather than
            // break the fold's ordering assumption — leaving data FINER than
            // asked is the safe direction.
            break;
        }
        if (threshold == prevThreshold) {
            // The finer stage's band is empty (its threshold coincides with
            // this coarser one), so everything below the boundary belongs to
            // the coarser stage. Replace rather than append: an equal-threshold
            // pair would make the finer stage unreachable anyway.
            out[stageCount - 1] = {threshold, interval};
            continue;
        }
        out[stageCount++] = {threshold, interval};
    }

    return stageCount;
}

}  // namespace timestar::retention

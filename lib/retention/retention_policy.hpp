#pragma once

#include <glaze/json.hpp>

#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct DownsamplePolicy {
    std::string after;  // Duration string, e.g. "30d"
    uint64_t afterNanos = 0;
    std::string interval;  // Duration string, e.g. "5m"
    uint64_t intervalNanos = 0;
    std::string method;  // "avg", "min", "max", "sum", "latest"

    // PER-FIELD METHOD OVERRIDES (Phase 4). field name -> method.
    //
    // One method per measurement destroys SCADA data: a flow totalizer averaged
    // over a 15-minute bucket is meaningless and a status word needs its last
    // value, not its mean. This is also what makes the `avg` cascade
    // approximation defensible — sum/min/max/latest compose EXACTLY across
    // stages (docs/api-retention.md), so routing counters to them confines the
    // inexactness to analog averages.
    //
    // OPTIONAL, not an empty map, on purpose: glaze's default
    // skip_null_members omits a disengaged optional entirely, so a policy that
    // names no overrides serialises — to the JSON API and to the persisted 0x0B
    // record — byte-for-byte as it did before this field existed.
    //
    // On a Boolean/String field this is the ONLY way to enable a fold, and only
    // with "latest": see validateRetentionPolicy().
    std::optional<std::map<std::string, std::string>> fieldMethods;
};

template <>
struct glz::meta<DownsamplePolicy> {
    using T = DownsamplePolicy;
    static constexpr auto value =
        object("after", &T::after, "afterNanos", &T::afterNanos, "interval", &T::interval, "intervalNanos",
               &T::intervalNanos, "method", &T::method, "fieldMethods", &T::fieldMethods);
};

// A measurement's retention policy: an optional TTL plus an ORDERED CASCADE of
// downsample tiers (1 Hz raw -> 1m after 7d -> 15m after 90d).
//
// TWO representations of the same thing, kept in sync by normalizeRetentionTiers():
//
//  - `downsampleTiers` is CANONICAL. Empty means no downsampling. Ordered
//    finest-first: `afterNanos` and `intervalNanos` both strictly increase, so
//    tier 0 is the first stage a point reaches as it ages.
//  - `downsample` is a LEGACY MIRROR of `downsampleTiers[0]`, written and
//    returned so a reader that knows only the legacy field applies the FINEST
//    tier only. That degradation is deliberate: finer than intended is never
//    coarser and never destroys data the newer binary would have kept.
//
// Records written before the cascade carry only `downsample`; loading promotes
// it to a one-element tier list (see normalizeRetentionTiers).
//
// LIMIT OF THE MIRROR, on the JSON persistence path specifically: it only helps
// a reader that TOLERATES the unknown `downsampleTiers` key. Glaze rejects
// unknown keys by default, so a binary built before this struct grew the field
// fails the whole document and drops the policy (TTL included) rather than
// falling back to the mirror. NativeIndex reads retention records leniently for
// exactly this reason (kRetentionReadOpts, native_index.cpp), which fixes the
// direction that is still open — rollback to any binary from this version
// forward — but cannot fix rollback to one that already shipped. The protobuf
// surface has no such caveat: proto3 ignores unknown fields by construction.
struct RetentionPolicy {
    std::string measurement;
    std::string ttl;  // Duration string, e.g. "90d"
    uint64_t ttlNanos = 0;
    std::optional<DownsamplePolicy> downsample;     // legacy mirror of downsampleTiers[0]
    std::vector<DownsamplePolicy> downsampleTiers;  // canonical; empty = no downsampling
};

template <>
struct glz::meta<RetentionPolicy> {
    using T = RetentionPolicy;
    static constexpr auto value = object("measurement", &T::measurement, "ttl", &T::ttl, "ttlNanos", &T::ttlNanos,
                                         "downsample", &T::downsample, "downsampleTiers", &T::downsampleTiers);
};

// Request structure for PUT /retention (ttl and downsample are optional).
//
// `downsample` is deliberately RAW JSON: the field accepts either a single
// object (legacy, one tier) or an array of objects (the cascade), and a single
// typed member cannot express both. parseDownsampleRequestField() below does
// the shape dispatch.
struct RetentionPolicyRequest {
    std::string measurement;
    std::optional<std::string> ttl;
    std::optional<glz::raw_json> downsample;
};

template <>
struct glz::meta<RetentionPolicyRequest> {
    using T = RetentionPolicyRequest;
    static constexpr auto value = object("measurement", &T::measurement, "ttl", &T::ttl, "downsample", &T::downsample);
};

namespace timestar::retention {

// Hard cap on cascade depth. Four stages is already 1 Hz -> 1m -> 15m -> 1h;
// beyond that the per-merge context build and the fold's live-state array grow
// for no realistic gain, and every extra stage compounds the avg approximation
// documented in docs/api-retention.md.
inline constexpr size_t kMaxDownsampleTiers = 4;

// The methods a downsample tier may name.
//
// COUNT is deliberately absent: count-of-counts is not count, so a cascade
// would silently report the number of stage-1 buckets rather than the number of
// raw samples. Of the five offered, min/max/sum/latest compose EXACTLY over
// stages; avg does not (see docs/api-retention.md).
bool isValidDownsampleMethod(std::string_view method);

// A tier's per-field overrides, or a shared empty map when it names none.
// Callers must go through this rather than dereference the optional, so
// "absent" and "present but empty" cannot behave differently anywhere.
const std::map<std::string, std::string>& fieldMethodsOf(const DownsamplePolicy& tier);

// The method a tier applies to `field`: its override if one is named, else the
// tier's own method. THE single definition of that fallback — the compactor's
// context build and every validation rule resolve through it.
const std::string& tierMethodForField(const DownsamplePolicy& tier, const std::string& field);

// Does any tier name a per-field override at all? A policy that names none must
// cost the compaction path nothing extra: no field resolution, no per-field
// context, no behavioural difference of any kind.
bool policyHasFieldMethods(const RetentionPolicy& policy);

// Fields whose override is "latest" AND therefore eligible to fold a
// Boolean/String series. Non-numeric folding is OPT-IN and reachable no other
// way — a measurement-wide `"method": "latest"` does NOT enable it, because a
// policy that predates this field must keep behaving exactly as it did.
std::vector<std::string> nonNumericFoldFields(const RetentionPolicy& policy);

// Make `downsample` and `downsampleTiers` agree, in both directions:
//   - tiers empty + legacy object present -> promote it to a one-element list
//     (this is how a pre-cascade persisted record loads);
//   - tiers non-empty -> overwrite the legacy field with tier 0.
// Idempotent. Every load, every store, and every cache update runs it, so no
// code below this layer has to know which field a record was written with.
void normalizeRetentionTiers(RetentionPolicy& policy);

// The canonical tier list for a policy that may not have been normalized —
// used by direct compactor callers (tests, tools) that build a policy by hand.
std::vector<DownsamplePolicy> effectiveTiers(const RetentionPolicy& policy);

// Could this policy change a single stored point? A policy with no TTL and no
// usable tier is INERT and must not cost the compaction path any index work.
bool policyIsActionable(const RetentionPolicy& policy);

// Validate a policy whose duration strings have already been converted to
// nanoseconds. Returns nullopt when valid, else a human-readable reason.
//
// SHARED so no caller can skip it. Before this existed the compactor mapped an
// unrecognised method string to AVG silently, which would have let a typo
// destructively average a totalizer; the compactor now refuses to fold a policy
// this function would reject.
//
// Rules (see docs/api-retention.md):
//   - 1..kMaxDownsampleTiers tiers;
//   - every tier: afterNanos > 0, intervalNanos > 0, method in the allowed set;
//   - one method across all tiers (v1);
//   - afterNanos strictly increasing; intervalNanos strictly increasing;
//   - interval[k+1] % interval[k] == 0 — with epoch-aligned buckets this is
//     what guarantees no stage-k bucket straddles a stage-k+1 boundary, so a
//     stage-k+1 bucket is exactly a whole number of stage-k buckets;
//   - after[k] >= interval[k] (a threshold shorter than its own bucket is
//     degenerate: the stage could never hold a complete bucket);
//   - ttl, if set, > after[last];
//   - every fieldMethods value in the allowed set;
//   - one method PER FIELD across all tiers (v1) — a field named in any tier
//     must resolve to the same method in every tier, which (since the tier
//     methods are already required to agree) means it must be named in all of
//     them or none;
//   - `latest` only, on a field the probe reports as non-numeric.
//
// The last rule needs a fact this function cannot know on its own: whether a
// field is stored as Boolean/String. `nonNumericField` supplies it — nullopt
// meaning "unknown", which ACCEPTS, because a field the index has never seen
// has no type to violate. The API boundary passes a probe backed by the
// field-type index (0x09); the compactor calls this without one, since it holds
// no index and enforces the same rule at fold time from the series' real
// TSMValueType, which is stronger than any name lookup.
using FieldTypeProbe = std::function<std::optional<bool>(const std::string& field)>;

std::optional<std::string> validateRetentionPolicy(const RetentionPolicy& policy,
                                                   const FieldTypeProbe& nonNumericField = {});

// Parse the polymorphic `downsample` field of a PUT body. Accepts an object
// (one tier), an array of objects (the cascade), or JSON null (no tiers).
// Returns nullopt on success, else a human-readable reason.
std::optional<std::string> parseDownsampleRequestField(const glz::raw_json& raw, std::vector<DownsamplePolicy>& out);

// ONE STAGE of a downsample cascade, resolved against a wall clock.
//
// A stage is a (threshold, interval) pair: points older than `threshold` fold
// into `interval`-wide, epoch-aligned buckets. `threshold` is always ALIGNED
// DOWN to its own `interval`, so only COMPLETE buckets ever fold — that is what
// makes re-folding already-folded data byte-identical.
struct DownsampleStage {
    uint64_t threshold = 0;  // points with ts < threshold fold at this stage
    uint64_t interval = 0;   // bucket width in nanoseconds
};

// Resolve a policy's tier list into the ordered stage array the fold consumes,
// as of `now`. Writes stages FINEST FIRST (largest threshold, smallest
// interval) and returns how many were written.
//
// SHARED between the compactor (which performs the fold) and
// Engine::sweepDownsampleRewrites() (which decides whether a fold is worth
// scheduling). They MUST agree: a sweep that computed a stage the compactor
// then declines to apply would rewrite the same file every sweep forever, and a
// sweep that missed a stage the compactor would apply leaves quiet series
// unfolded — the exact defect the age-driven trigger exists to fix.
//
// Returns 0 (and sets `invalidReason`, if given) for a policy that fails
// validateRetentionPolicy(): a policy that cannot fold must never produce work.
uint8_t buildDownsampleStages(const RetentionPolicy& policy, uint64_t now,
                              std::array<DownsampleStage, kMaxDownsampleTiers>& out,
                              std::string* invalidReason = nullptr);

}  // namespace timestar::retention

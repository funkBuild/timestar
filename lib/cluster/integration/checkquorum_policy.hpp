#pragma once

#include <algorithm>
#include <cctype>
#include <string>

// THE CHECKQUORUM ENABLE POLICY, EXTRACTED PURE (debt D-30).
//
// ADR 0005 wanted two mechanisms: (b) a new message-type byte, so an old peer DROPS a
// transfer vote rather than misparsing it -- shipped; and (c) cluster-wide gated
// activation, so the guard cannot be on anywhere until every voter can read that byte --
// NOT built, and deliberately deferred (see the D-30 register row for the trigger that
// would build it).
//
// What replaces (c) for now is RELEASE ORDERING: the decoder ships with the guard OFF, so
// by the time any release turns the guard on, every node it can be rolling-upgraded from
// already reads tag 8 and there is no mixed-enable window to protect. That argument rests
// on ONE property, and it is the whole of the deferral's safety:
//
//     ENABLING IS A BUILD DECISION. NO RUNTIME INPUT CAN TURN CHECKQUORUM ON.
//
// A per-node enable knob is exactly the mixed-version hazard ADR 0005 exists to prevent --
// one node running the disruption guard while an older peer drops its transfer votes --
// and it is the failure the ADR was written after observing. The property was previously
// enforced by a function in an anonymous namespace and pinned by nothing: a future edit
// that "completed" the override into a symmetric boolean would leave every test green and
// silently revoke the deferral this file documents.
//
// So the parse and the decision are pure functions here, in the shape `planReadRouting`
// (D-13) and `LeadershipBalancePass` (D-22) took, and `cluster_data_plane.cpp` keeps only
// what genuinely needs the process: reading the environment and logging the outcome.
namespace timestar::cluster {

// What the operator's override SAID. Note `EnableRefused`: an enabling value is a
// distinct, recognised outcome rather than a parse failure, because an operator who
// writes `TIMESTAR_CLUSTER_CHECKQUORUM=1` deserves a log line saying the knob is
// disable-only -- not one saying their value was not a boolean.
enum class CheckQuorumOverride {
    None,           // unset, empty, or whitespace only
    Disable,        // 0 / false / no / off
    EnableRefused,  // 1 / true / yes / on -- understood, and deliberately not honoured
    Invalid,        // anything else
};

// TRIM AND LOWERCASE BEFORE COMPARING. This is an operator's only lever over the guard and
// it is reached at 3am: `FALSE`, `Off` and a value with a stray space or trailing newline
// (trivially produced by a shell here-doc, a docker-compose YAML scalar, or an env file)
// must all mean what they obviously mean. A case-SENSITIVE exact match silently fell
// through to the Invalid arm and left the guard ON -- i.e. the one outcome the operator
// was trying to avoid.
inline CheckQuorumOverride parseCheckQuorumOverride(const char* env) {
    if (env == nullptr || *env == '\0')
        return CheckQuorumOverride::None;
    std::string v(env);
    v.erase(0, v.find_first_not_of(" \t\r\n"));
    if (const auto last = v.find_last_not_of(" \t\r\n"); last != std::string::npos)
        v.erase(last + 1);
    else
        v.clear();
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (v.empty())
        return CheckQuorumOverride::None;  // whitespace only == unset
    if (v == "0" || v == "false" || v == "no" || v == "off")
        return CheckQuorumOverride::Disable;
    if (v == "1" || v == "true" || v == "yes" || v == "on")
        return CheckQuorumOverride::EnableRefused;
    return CheckQuorumOverride::Invalid;
}

// THE DECISION. `buildDefault` is `kCheckQuorumDefault`, the one line a future release
// flips (D-9's measurement is why it is false today).
//
// Two rules, and the first is the one that must never acquire an exception:
//
//   1. `buildDefault == false` => OFF, for every override, always. The override is then
//      INERT rather than merely unnecessary -- there is nothing to disable and nothing may
//      enable -- and that is what makes the release ordering above hold.
//   2. `buildDefault == true` => ON unless the override disables it. An UNPARSEABLE value
//      leaves it ON: this is a guard, and a typo must not silently remove one. The
//      operator gets an error log naming the accepted spellings.
constexpr bool resolveCheckQuorum(bool buildDefault, CheckQuorumOverride ov) {
    if (!buildDefault)
        return false;
    return ov != CheckQuorumOverride::Disable;
}

}  // namespace timestar::cluster

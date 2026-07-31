// THE CHECKQUORUM ENABLE POLICY (debt D-30).
//
// ADR 0005's mechanism (c) -- cluster-wide gated activation -- is deliberately deferred,
// and what stands in for it is release ORDERING: the transfer-vote decoder ships with the
// guard OFF, so any later release that turns the guard on can only be rolling-upgraded
// from binaries that already read tag 8.
//
// That argument is only sound while ENABLING IS A BUILD DECISION. If any runtime input
// could turn the guard on, an operator could enable it on one node of a mixed-version
// cluster and reproduce precisely the failure ADR 0005 was written after: the enabled
// node's voters drop its transfer votes, and every leadership transfer costs an election
// timeout of leaderlessness instead of one tick round.
//
// So these tests are not tests of a string parser. `OverrideCanNeverEnableTheGuard` is the
// deferral's safety argument, executable.
#include "../../../lib/cluster/integration/checkquorum_policy.hpp"

#include <gtest/gtest.h>

using timestar::cluster::CheckQuorumOverride;
using timestar::cluster::parseCheckQuorumOverride;
using timestar::cluster::resolveCheckQuorum;

namespace {

// Everything a `TIMESTAR_CLUSTER_CHECKQUORUM=...` could plausibly say, including the
// spellings that MEAN enable. The point of listing the enabling ones is that they are
// understood and still refused -- not unparsed.
const char* const kAllOverrides[] = {
    "1",  "true", "TRUE", "True", "yes",    "YES", "on", "ON",   " on\n", "0", "false", "FALSE",
    "no", "off",  "OFF",  " 0 ",  "false ", "",    " ",  "\t\n", "maybe", "2", "-1",    "enable",
};

}  // namespace

// ---------------------------------------------------------------------------
// THE PROPERTY. With the build default off, NOTHING enables the guard.
TEST(CheckQuorumPolicyTest, OverrideCanNeverEnableTheGuard) {
    for (const char* s : kAllOverrides) {
        EXPECT_FALSE(resolveCheckQuorum(/*buildDefault=*/false, parseCheckQuorumOverride(s)))
            << "TIMESTAR_CLUSTER_CHECKQUORUM='" << s << "' enabled CheckQuorum with the build default OFF -- "
            << "that is ADR 0005's mixed-version hazard and it revokes debt D-30's deferral";
    }
    // Including the absence of the variable entirely.
    EXPECT_FALSE(resolveCheckQuorum(false, parseCheckQuorumOverride(nullptr)));
}

// The override is INERT, not merely unnecessary, when the build default is off: the same
// value produces the same answer whatever it says.
TEST(CheckQuorumPolicyTest, TheOverrideIsInertWhileTheBuildDefaultIsOff) {
    const bool unset = resolveCheckQuorum(false, parseCheckQuorumOverride(nullptr));
    for (const char* s : kAllOverrides)
        EXPECT_EQ(resolveCheckQuorum(false, parseCheckQuorumOverride(s)), unset) << "value: '" << s << "'";
}

// ---------------------------------------------------------------------------
// THE OTHER HALF, which is the shape of the NEXT release rather than this one: with the
// build default on, the knob disables and only disables.
TEST(CheckQuorumPolicyTest, WithTheBuildDefaultOnTheOverrideOnlyDisables) {
    for (const char* s : {"0", "false", "FALSE", "no", "NO", "off", "OFF", " 0 ", "False\n"})
        EXPECT_FALSE(resolveCheckQuorum(/*buildDefault=*/true, parseCheckQuorumOverride(s))) << "value: '" << s << "'";
    for (const char* s : {"1", "true", "yes", "on", "ON", " on\t"})
        EXPECT_TRUE(resolveCheckQuorum(true, parseCheckQuorumOverride(s))) << "value: '" << s << "'";
    EXPECT_TRUE(resolveCheckQuorum(true, parseCheckQuorumOverride(nullptr)));
    EXPECT_TRUE(resolveCheckQuorum(true, parseCheckQuorumOverride("")));
}

// A TYPO MUST NOT SILENTLY REMOVE A GUARD. `disabled`, `flase`, `2` are not disable
// spellings, and the safe reading of an unparseable value is the guard staying ON (the
// operator gets an error log naming the accepted spellings).
TEST(CheckQuorumPolicyTest, AnUnparseableValueLeavesTheGuardOn) {
    for (const char* s : {"maybe", "flase", "disabled", "2", "-1", "0x0", "null"}) {
        EXPECT_EQ(parseCheckQuorumOverride(s), CheckQuorumOverride::Invalid) << "value: '" << s << "'";
        EXPECT_TRUE(resolveCheckQuorum(/*buildDefault=*/true, parseCheckQuorumOverride(s))) << "value: '" << s << "'";
    }
}

// ---------------------------------------------------------------------------
// The parse itself. Case- and whitespace-insensitivity is not cosmetic: a case-SENSITIVE
// match once sent `FALSE` down the Invalid arm, which left the guard ON -- the one outcome
// the operator was trying to avoid.
TEST(CheckQuorumPolicyTest, DisableSpellingsAreCaseAndWhitespaceInsensitive) {
    for (const char* s : {"0", "false", "FALSE", "False", "no", "No", "NO", "off", "Off", "OFF", "  false  ", "false\n",
                          "\toff\r\n", " 0"})
        EXPECT_EQ(parseCheckQuorumOverride(s), CheckQuorumOverride::Disable) << "value: '" << s << "'";
}

// An enabling value is RECOGNISED, so the operator can be told the knob is disable-only
// rather than told their value was not a boolean.
TEST(CheckQuorumPolicyTest, EnablingValuesAreUnderstoodAndRefusedRatherThanUnparsed) {
    for (const char* s : {"1", "true", "TRUE", "yes", "Yes", "on", "ON", " on\n", "\ttrue "})
        EXPECT_EQ(parseCheckQuorumOverride(s), CheckQuorumOverride::EnableRefused) << "value: '" << s << "'";
}

// Unset, empty and whitespace-only are the same thing -- an env var set to nothing by a
// compose file must not read as a typo.
TEST(CheckQuorumPolicyTest, UnsetEmptyAndWhitespaceAreAllNone) {
    EXPECT_EQ(parseCheckQuorumOverride(nullptr), CheckQuorumOverride::None);
    EXPECT_EQ(parseCheckQuorumOverride(""), CheckQuorumOverride::None);
    EXPECT_EQ(parseCheckQuorumOverride(" "), CheckQuorumOverride::None);
    EXPECT_EQ(parseCheckQuorumOverride("\t\r\n "), CheckQuorumOverride::None);
}

// The decision is `constexpr`, which is a small structural guarantee that it depends on
// nothing but its arguments -- no getenv, no config, no state a later edit could reach for.
static_assert(!resolveCheckQuorum(false, CheckQuorumOverride::EnableRefused),
              "the build default OFF must not be enableable at runtime (ADR 0005 mechanism (c) / debt D-30)");
static_assert(!resolveCheckQuorum(false, CheckQuorumOverride::None));
static_assert(resolveCheckQuorum(true, CheckQuorumOverride::None));
static_assert(!resolveCheckQuorum(true, CheckQuorumOverride::Disable));

# ADR 0005 — CheckQuorum and leadership transfer

**Status:** accepted; transfer bypass implemented; production default remains
off.

## Context

Raft's CheckQuorum disruption guard ignores ordinary vote requests while a
follower still hears a leader. A leadership transferee campaigns immediately
after `TimeoutNow`, while the other voters can still be inside that lease. If
the transfer campaign is indistinguishable from an unsolicited campaign, the
other voters ignore it until the lease expires and a planned transfer becomes a
multi-second leaderless window.

CheckQuorum is not required for Raft safety. A leader without a quorum cannot
commit. TimeStar also bounds proposal waiters, so a quorum-less write fails
within its deadline even while CheckQuorum is disabled. The guard primarily
improves promptness: an isolated leader stops presenting itself as leader
sooner.

Measurements on the current topology showed worse one-node-failure recovery
with CheckQuorum enabled, so the production data plane explicitly sets it to
false. Direct Raft tests may enable it to preserve the behavior for future
tuning.

## Decision

`RequestVote` carries `campaignTransfer`. A campaign started by an accepted
`TimeoutNow` sets the flag, and the disruption guard permits that vote request
through the lease. All ordinary vote checks remain in force: term, log
up-to-date, and one vote per term.

`TimeoutNow` is accepted only from the node believed to be leader before the
message is stepped. This is required: without it, an arbitrary peer could cause
a transfer-marked campaign and bypass the lease.

Hibernated groups credit every skipped tick pass. Otherwise an idle follower
would stretch its lease by the hibernation factor and delay failover even when
CheckQuorum is disabled elsewhere.

## V1 protocol rule

The Raft envelope is the single current v1 format and starts with `TSR1`.
`RequestVote` always includes both `preVote` and `campaignTransfer`; setting both
is invalid. Unknown magic, tags, flags, truncated payloads, or trailing bytes
are rejected.

There is no historical-tag decoder or mixed-version activation policy. During
the greenfield phase, every node in a cluster must run the same v1 schema. The
v1 layout is updated in place and development journals must be recreated after
an incompatible change.

## Consequences

- Leadership transfer works when a direct Raft configuration enables
  CheckQuorum.
- A forged `TimeoutNow` from a non-leader is ignored without changing term or
  leader state.
- The production data plane continues to use bounded proposal deadlines with
  CheckQuorum disabled.
- Enabling CheckQuorum in production is a performance/availability decision,
  not a protocol-version activation. It requires fresh one-node-failure and
  partition measurements on the release candidate.

## Required tests

- transfer under CheckQuorum completes without waiting for a full election
  timeout;
- unsolicited campaigns still obey the lease;
- transfer votes still obey term, log, and one-vote-per-term rules;
- same-term and higher-term forged `TimeoutNow` messages are ignored;
- hibernation does not stretch the lease;
- the v1 codec round-trips transfer votes and rejects malformed envelopes.

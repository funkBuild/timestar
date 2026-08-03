#!/bin/bash
# GATE (debt D-19, second half): TCP RESETS **and** a LEADERSHIP REBALANCE **and** a raised
# connection count, all at once.
#
# WHY IT IS NOT THE SUM OF TWO GATES. `fault_injection_gate.sh` destroys a peer's
# connections while every group sits still; `rolling_rebalance_gate.sh` moves leadership
# while the transport is healthy. Each fault alone is absorbed by a mechanism that assumes
# the other is not happening: a slice refused by a group mid-transfer is RE-DISPATCHED, and
# re-dispatch is exactly what needs a working connection -- over a transport that is
# simultaneously being reset, behind a reconnect backoff, inside ONE 1.5 s deadline shared
# by both retry reasons. Raising the connection count multiplies the sockets the resetter
# destroys per round, so the reconnect the re-dispatch waits on is contended too.
#
# HOW IT IS BUILT. This is `fault_injection_gate.sh` in combined mode, not a copy of it:
# the proxy topology, the balance-first step (node 3 must lead a fair share or nothing
# crosses the fault), the per-storm probe and the read-back are the same code. Only the
# knobs differ, and they live here so the two gates' budgets cannot be confused:
#
#   GATE_REBALANCE_STORM=1   a rebalance loop runs through every reset storm
#   GATE_CONNECTIONS=16      4x the reset gate's connection count
#   GATE_STORM_ROUNDS=2      two storms, not three -- see the disk note below
#   GATE_BENCH_BATCHES=1200  keeps each fast arm above the per-storm reset floor
#
# THE BUDGET IS ITS OWN, AND IT IS NOT THE RESET GATE'S. Two faults produce more client
# errors than one, and pretending otherwise would either fail this gate on correct
# behaviour or silently loosen the reset gate. Measured, three runs (the first before the
# pre-balance was made to stop early -- see the finding below):
#
#     run                1            2            3
#     errors/storm       [0 5] = 5    [19 17] = 36 [3 0] = 3
#     reset rounds/conns 80/234 79/236 81/229 81/235 79/221 75/217
#     retained           83 % 82 %    81 % 79 %    83 % 87 %
#     rebalance          129 / 420    13 / 420     0 / 408   (transfers / calls)
#     probe read-back    200/200 on every node, every storm, all three runs
#
# Those are historical 10,000-timestamp results. Two current 1,200-request durable draws
# measured totals of 26 and 54 errors, 49-66 reset rounds and 196-266 destroyed
# connections per storm, 38-53% retained throughput, 1/15 transfers and 195/201 rebalance
# calls. Its 150-call floor is tied to the reset anti-vacuity floor: a minimally valid
# 70-round run lasts about 21 seconds and must therefore execute the 0.2-second three-node
# rebalance loop throughout.
#
# The error ceiling remains ~2x the historical worst draw (36). Note the reset gate's own
# budget is 60 across THREE storms at 4 connections: the extra headroom here is for the
# CONNECTION COUNT, which multiplies the sockets each reset round destroys, far more than
# they are the rebalance.
#
# THE FINDING, and it is why the anti-vacuity floor is on CALLS and not on transfers.
# `transfers_initiated` is ~ZERO under the fault -- 0, 0 and 13 across the runs above, with
# 400+ calls issued and the cluster deliberately left imbalanced (measured 1536/1536/1024
# against a fair share of 1365). The same endpoint on a healthy transport moves 2175-2901
# (`skewed_rebalance_gate.sh`). The cause is the fault itself: the only node below fair
# share is the one behind the reset proxy, its acks die every 0.3 s, and D-1's liveness
# filter therefore reads it as dead and refuses to hand it groups. That is D-1 working --
# shipping leadership to a peer whose connection is being destroyed is exactly what it
# exists to prevent -- and it is the first LIVE measurement of that filter acting, which
# the D-18 row records as reasoned and unit-pinned rather than measured.
#
# The consequence for this gate is honest rather than convenient: under a transport fault
# the rebalance arm mostly does not get to act, so what is combined here is a reset storm,
# a raised connection count, and a balancer CORRECTLY declining. A future change that made
# the filter permissive would show up as a transfer count that suddenly is not zero.
#
# DISK. Each storm is a full bench and nothing is deleted between them (see the reset
# gate's BENCH_BATCHES note). The durable profile is about 0.7 GiB at RF=3 for
# two storms plus the baseline; the historical ~20-GiB figure used the retired 10,000-
# timestamp request. Do not raise GATE_STORM_ROUNDS or either bench dimension without
# doing that arithmetic and raising the scratch preflight when needed.
#
# Usage: combined_fault_rebalance_gate.sh [SERVER_BINARY]
#   exit 0 = pass, 1 = property failed, 2 = setup refused, 3 = VOID (re-draw)
set -u
cd "$(dirname "$0")" || exit 2

export GATE_REBALANCE_STORM=1
export GATE_CONNECTIONS="${GATE_CONNECTIONS:-16}"
export GATE_STORM_ROUNDS="${GATE_STORM_ROUNDS:-2}"
export GATE_BENCH_BATCHES="${GATE_BENCH_BATCHES:-1200}"
export GATE_MAX_STORM_ERRORS="${GATE_MAX_STORM_ERRORS:-80}"
export GATE_MIN_COMBINED_CALLS="${GATE_MIN_COMBINED_CALLS:-150}"
# The rebalance storm competes with the bench for the same reactors, so the throughput dip
# is deeper than the reset gate's and the floor moves with it.
export GATE_MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-25}"

echo "=== COMBINED: reset storm + leadership rebalance + $GATE_CONNECTIONS connections ==="
exec ./fault_injection_gate.sh "$@"

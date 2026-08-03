# Protocol and persisted-format versioning

TimeStar is a greenfield project. All current inter-node protocols and persisted
formats are version 1. Version markers remain mandatory, but implementations
accept and emit v1 only and the v1 layout is updated in place until the first
production release.

This is an intentional compatibility reset. Data, journals, snapshots, or RPC
frames produced by earlier development layouts are unsupported and must be
discarded or rebuilt. There are no rolling-upgrade, downgrade, activation, or
historical-reader guarantees during the greenfield phase.

## Current v1 markers

| Surface | v1 marker |
|---|---|
| Data-plane connection handshake | exact version `1`; any other value is rejected |
| Write batch | `TSW1` |
| Replicated data command | `TSC1` |
| VShard snapshot payload | `TSP1` |
| Cluster backup manifest | `TSBK` plus little-endian `uint32(1)`; exactly 4,096 canonical `TSP1` units |
| Control command | `TCC1` |
| Group-0 retention policy value | `TSRP1` |
| Frozen delete-plan RPC frame | version byte `1` |
| Control join RPC frames | version byte `1` |
| Movement destination/actuation RPC frames | version byte `1`, carried only after the exact-v1 connection handshake |
| Group-0 snapshot | `TSG0SNP1` |
| Persisted VShard movement job | `TSMJ1`: step, VShard, target map epoch, full-width destination/victim IDs, exact source voters |
| Raft envelope | `TSR1`; `AppendEntriesReply` carries both replicated match index and state-machine applied index |
| Raft journal snapshot record | `TSRSNAP1` |
| WAL segment | `TSWL` plus little-endian `uint32(1)` |
| TSM file | `TASM` plus byte `1` |
| TSM string value block | `STR1` (raw) or `STD1` (dictionary), both v1 variants |
| TSM filename | `<tier>_<sequence>[_d<data-sequence>].tsm` canonical decimal fields |
| TSM tombstone | `TSMT` plus `uint32(1)` |
| NativeIndex manifest | `TSMF` plus `uint32(1)` |
| NativeIndex SSTable footer | `TSIX` plus `uint32(1)` |
| Raft journal segment | `JRNL` plus `uint32(1)` |
| VShard manifest | `VSMF` plus `uint32(1)` |
| VShard snapshot manifest | `VSNP` plus `uint32(1)` |
| VShard extent map | `VXMP` plus `uint32(1)` |
| Snapshot pin set | `SPNS` plus `uint32(1)` |
| Durable control map | `TSCMAP1` and version `1` |

Nested request/reply payloads carried over the negotiated data-plane connection
use the one current v1 schema. Fields are mandatory according to that schema;
decoders do not accept truncated historical layouts or optional compatibility
tails.

The exact-v1 Raft append reply reports application progress separately from log
replication progress. Group-0 lifecycle code uses that distinction to retain a
departing learner until it has applied the final serving map and its own
`Removed` record. This is an in-place `TSR1` schema update for the greenfield
format, not a second protocol version.

NativeIndex v1 uses local-ID forward mappings, roaring postings, and per-value
tag markers directly. It has no pre-bitmap migration or tag-value blob reader;
series metadata without the atomically persisted local-ID counter fails closed.
The retired `shard_N` migration planner and rewriter are not shipped. Startup
recognizes those development roots only to reject them before mutation.

## Rules while greenfield

1. Keep an explicit v1 marker at every durable or network boundary.
2. Update the v1 encoder and decoder together; do not add a v2 constant or a
   fallback reader.
3. Reject an unknown marker, version, tag, flag, trailing byte, checksum, or
   unsupported peer before applying state.
4. Tests cover current v1 round trips and malformed/unknown-version rejection.
   They do not synthesize retired layouts.
5. Recreate development clusters after a v1 layout change. Never interpret a
   decoder fallback as a migration strategy.
6. Introduce v2 only after production compatibility becomes a real requirement,
   with an explicit deployment, migration, and rollback design.

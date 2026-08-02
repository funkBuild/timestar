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
| Control command | `TCC1` |
| Frozen delete-plan RPC frame | version byte `1` |
| Group-0 snapshot | `TSG0SNP1` |
| Persisted VShard movement job | `TSMJ1`: step, VShard, target map epoch, full-width destination/victim IDs, exact source voters |
| Raft envelope | `TSR1` |
| Raft journal snapshot record | `TSRSNAP1` |
| WAL segment | `TSWL` plus little-endian `uint32(1)` |
| TSM file | `TASM` plus byte `1` |
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

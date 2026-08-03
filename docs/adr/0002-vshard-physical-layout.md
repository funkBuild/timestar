# ADR 0002 — VShard-aware physical storage and snapshots

**Status:** Accepted and implemented for exact v1

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)

## Context

TimeStar has 4,096 stable VShard identities but must not create 4,096 storage
engines or indexes per node. The shipped implementation therefore separates the
logical Raft/snapshot boundary (one group per VShard) from the physical Engine
boundary (one Engine, WAL stream, TSM directory, and NativeIndex per reactor).

The original proposal reserved a live `vshards/NNNN/` tree containing
`raft.meta`, a per-VShard `MANIFEST`, catalogs, tombstone objects, and TSM extent
references. That tree was never the implementation. Keeping it in the accepted
ADR made non-existent files look like production authority, so exact v1 removes
the unused path API and documents the storage that is actually opened.

## Decision

### Live data-root layout

```text
<data_dir>/
  node.json                         # stable node and cluster identity
  control_map.cache                 # latest durably applied Group-0 serving map
  cluster_raft/
    group0/
      seg_*.jnl
      snapshot_sidecars/
    vshard_<id>/                    # default private data-group journal, id 0..4095
      seg_*.jnl
      snapshot_sidecars/
    retired/                        # exact-v1 epoch-named journal quarantines
  shard_<reactor>/
    <sequence>.wal                  # reactor-local Engine WAL generations
    tsm/
      <tier>_<sequence>.tsm
      <tier>_<sequence>_d<data-sequence>.tsm
      <matching-basename>.tombstone
    native_index/
      MANIFEST
      wal/idx_*.wal
      idx_*.sst
```

Snapshot staging directories below `tsm/`, restore markers, and backup-export
state are temporary or workflow-specific namespaces, not independent storage
authorities. `vshards/NNNN.tsp1` is an archive namespace inside an exact-v1
cluster backup, not a live data-root layout.

The optional shared journal replaces the `vshard_<id>` directories with one
`cluster_raft/shard_<reactor>` stream. It is not the release or restore layout;
see [ADR 0001](0001-journal-segmentation-and-retention.md).

### VShard identity and reactor ownership

A VShard is the fixed 12-bit value `0..4095` derived from the canonical
`SeriesId128`. The reserved upper four bits of its on-wire/on-journal `u16`
must be zero. Exact-v1 replicated startup accepts only a reactor count for which
`assignCore(vshard, core_count)` keeps every series in a VShard on one reactor.

The VShard identity selects its data Raft group, routing entry, snapshot, backup
unit, and retirement operation. It does not select a separate Engine instance.
WAL and freshly flushed TSM generations may contain many VShards owned by the
reactor.

### TSM placement and last-write ordering

Clustered startup enables VShard-partitioned background compaction before the
compaction loop starts. Ordinary mixed inputs converge into VShard-pure outputs;
a retention merge may temporarily produce another mixed physical generation.
Correctness never depends on physical purity: every series ID deterministically
names its VShard, reads can filter by that identity, and snapshot creation always
materializes a VShard-pure resolved object.

TSM filenames carry immutable file identity and write-generation rank. A flush
reserves its sequence at memory-store rollover, before concurrent conversions
can reorder completion. Compaction inherits the maximum input data sequence.
That makes file rank follow store write order and preserves last-write-wins when
the same timestamp appears in multiple generations.

Exact-v1 TSM framing, block metadata, revision ranges, and tombstone sidecars are
defined in [tsm_format.md](../tsm_format.md). There is no object UUID, live
per-VShard manifest, whole-file BLAKE3 field, or old-layout reader in the local
TSM format. The local v1 format instead authenticates each compressed block and
series entry with CRC32, authenticates the complete index and footer at open,
and rejects structurally inconsistent authenticated metadata before registration.

### NativeIndex layout

Each reactor owns one NativeIndex under `shard_<reactor>/native_index`. Durable
series-metadata and value-type keys encode the VShard after their type byte, so
the implementation can address the per-VShard catalog state without creating
one index service per group. Measurement/tag discovery structures remain shared
reactor-local indexes and are updated or removed through the same Engine
transaction as VShard snapshot install and retirement.

The earlier generic `<vshard><type><payload>` key wrapper was never called by
NativeIndex. Exact v1 removes that duplicate helper and its standalone test;
`key_encoding.cpp` is the one durable key grammar.

### Snapshot boundary

Snapshot production pins the current immutable TSM generation, resolves only
the selected VShard with tombstone effects applied, and writes one VShard-pure
TSM object plus its deterministic catalog. The manifest records:

- VShard and snapshot-revision fence;
- ordered object identifiers and revision ranges;
- logical verification hash and catalog hash;
- delete-receipt and retention state needed after log compaction.

The production encoder streams that state into one checksummed `TSP1` sidecar.
Install validates exact-v1 framing, complete-file hash/size, manifest and catalog
hashes, canonical object names, the Raft-to-manifest revision boundary, and the
resolved logical view before publishing receiver-allocated local TSM names.
Snapshot install or replica retirement replaces only the named VShard's logical
state; unrelated series in mixed Engine objects remain live.

## Consequences

- Operators back up and restore through the exact-v1 cluster workflow, never by
  copying a proposed `vshards/NNNN` live directory.
- Reactor-count changes are allowed only when the VShard cohesion rule holds;
  VShard count changes require a future format/migration design.
- Raft state is isolated by private VShard journals, while Engine files remain
  bounded by reactor count and are isolated logically.
- Snapshot/backup integrity is self-contained in `TSP1`; local TSM block,
  entry, index, and footer integrity is enforced independently by exact v1.

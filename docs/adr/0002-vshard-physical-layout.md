# ADR 0002 — VShard-partitioned physical TSM and NativeIndex layout

**Status:** Accepted (Phase 1 / Task 4 prerequisite; resolves open decision 3
and the Task-4-critical part of open decision 5)

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)
(sections "Stable identity and layout", "Multiplexed object model", "TSM and
catalog requirements")

**Prerequisite for:** VShard-workers Task 4a (catalog), 4c (TSM/index/compaction
partitioning), 4d (snapshot/restore).

## Context

The plan simultaneously mandates multiplexing ("no 4,096 independent services")
and per-VShard guarantees (snapshot, pin, extraction, anti-entropy). These
reconcile only if the physical layout is fixed precisely enough that Task 4 code
can cite it rather than each guarantee re-deriving it. Decision 5 additionally
requires resolving the VShard identity before Task 4 bakes IDs into every file.

## Decision

### 1. Directory layout

```
<data_dir>/
  node.json                       # node identity (Phase 3; proposal)
  journals/core_<n>/seg_*.jnl     # per-core multiplexed journals (ADR 0001)
  vshards/<id>/                   # <id> = zero-padded 0000..4095 (StorageLayout::vshardDir)
    raft.meta                     # per-VShard Raft hard state snapshot pointer
    MANIFEST                      # per-VShard object set (CRC-framed, like today's index MANIFEST)
    catalog/                      # durable SeriesId128 -> definition (Task 4a)
    tsm/                          # VShard-pure TSM objects (tier >= 1) + tier-0 extent refs
    tombstones/                   # first-class tombstone objects
    snapshots/<S>.pin             # persistent pin files listing object UUIDs
  tsm-shared/core_<n>/seg_*.t0    # tier-0 multiplexed segments (see 3)
  index/core_<n>/                 # one NativeIndex per core, VShard-prefixed keys (see 4)
```

The `vshards/0000`..`vshards/4095` boundary is frozen (StorageLayout owns it).
`journals/`, `tsm-shared/`, and `index/` are per-core and multiplexed; only the
`vshards/<id>/` subtree is per-VShard, and even there tier-0 data lives by
reference into the shared segments.

### 2. Immutable object identity

Every immutable object (a tier ≥ 1 TSM file, a tier-0 shared segment, a catalog
snapshot, a tombstone object, an index SSTable) carries in its header:

```
object_uuid (16 bytes)     # stable, globally unique, assigned at creation, never reused
format_version (u32)
virtual_shard_count (u16)  # the VIRTUAL_SHARD_COUNT the object was written under (see 6)
byte_length (u64)
whole_file_hash (32 bytes) # BLAKE3/SHA-256 over the file's bytes
```

TSM data blocks additionally gain a per-block `crc32c` so corruption is
attributable to a block and repairable, not fatal to the whole file.

The MANIFEST is the authority for which objects are live; an object present on
disk but absent from the MANIFEST (and from every pin file) is GC-eligible.

### 3. Tier-0 multiplexing, converging to VShard-pure files

- **Tier 0 may be multiplexed.** A flush writes a shared tier-0 segment
  (`tsm-shared/core_<n>/seg_*.t0`) containing blocks for many VShards; the
  per-VShard MANIFEST records the byte **extents** of that VShard's blocks
  within the segment. This keeps flush to one append stream per core.
- **Compaction converges to VShard-pure files at tier ≥ 1.** A tier-0→tier-1
  merge reads one VShard's extents and writes a VShard-pure object under
  `vshards/<id>/tsm/`. From tier 1 up, a TSM object belongs to exactly one
  VShard.
- **Snapshotting a VShard extracts only its tier-0 extents** from the shared
  segments plus its VShard-pure tier ≥ 1 objects — never the whole segment.

### 4. NativeIndex: one per core, VShard-prefixed keys

- One NativeIndex instance per core (`index/core_<n>/`), not per VShard.
- Every key is prefixed with the 2-byte VShard id **before** the existing type
  prefix (0x01..0x1x). So the current key `<type><measurement>...` becomes
  `<vshard:2><type><measurement>...`. Day bitmaps (0x0D) and HLL sketches (0x14)
  are likewise VShard-prefixed.
- A VShard's index extract is a prefix range-scan `[<vshard>, <vshard>+1)`
  serialised into the snapshot, so **install is a load, not a rebuild**.
  Rebuild-from-journal is the repair path only, with the stated cost bound
  (replay of the VShard's retained log).

### 5. Revisions replace `_d<N>` inside `vshards/`

Duplicate-timestamp resolution uses replicated point revisions (ADR 0003), not
local tier/sequence ranking. Inside `vshards/` the `_d<N>` dataSeq filename
ranking is dead: flush and compaction always materialise the LWW winner;
steady-state blocks store only a per-block `[minRev, maxRev]` in the index entry;
a per-point revision column exists only in tier-0 blocks that may hold intra-file
duplicates. Legacy `_d<N>` names remain migration input only.

### 6. VShard identity and decision 5 (split epoch) — accept the risk, make it safe

The on-disk VShard identity is the canonical **12-bit value 0..4095**
(`VShardId`), frozen at cluster creation. We **do not** bake speculative
split-epoch bits into the hot identity or the directory grammar: doing so would
complicate every ID, journal record, and `vshards/NNNN` name for a feature that
may never ship, and 4,096 VShards already give ample headroom (4096 / node-count
VShards per node).

Instead we **record the accepted risk that splitting requires a full versioned
data migration**, and make that migration *safe and detectable* rather than
ambiguous: every immutable object and journal segment stamps the
`virtual_shard_count` and `format_version` it was created under (field in 2).
A future scheme that changes the VShard count or splits a VShard is therefore a
detectable format-version change with a migration, never a silent
reinterpretation of existing IDs. The unused top 4 bits of the 16-bit on-disk
VShard field are **reserved and must be zero** (validated on read), leaving a
clean escape hatch without committing to a split encoding now.

### 7. Per-VShard gates are logical, not byte-for-byte

Wherever shared tier-0 segments or the multiplexed index exist, "unrelated
VShards untouched" means their **logical** time-block hashes and MANIFEST entries
are unchanged; shared-segment and multiplexed-index bytes are exempt from
byte-identity. Anti-entropy compares logical hashes (ADR-adjacent; see the
plan's anti-entropy section), so replicas that compacted independently still
agree.

## Rationale

- Multiplex where cost scales with VShard count (flush streams, index instances,
  compaction fibers); partition where the guarantee is per-VShard (snapshot,
  pin, extract, catalog). The MANIFEST-extent indirection is what lets tier-0 be
  shared yet per-VShard-extractable.
- Object UUID + whole-file hash + block CRC give attributable corruption and the
  anti-entropy verification hash the Task 4 gate consumes.
- Accepting the split risk (6) keeps v1 IDs minimal; the format-version stamp is
  the cheap insurance that turns a hypothetical future split into a bounded,
  detectable migration.

## Alternatives considered

- **Pure physical partitioning (a real directory + files per VShard for
  everything).** Rejected: 4,096 tiny tier-0 files per flush cycle, 4,096 index
  instances — the "no 4,096 services" violation.
- **Reserve split-epoch bits in the live VShard id now.** Rejected as premature:
  it taxes every record/path for a maybe-never feature; the format-version stamp
  achieves detectability without the tax.
- **Keep `_d<N>` dataSeq ranking inside `vshards/`.** Rejected: local sequence
  numbering is not comparable across replicas that compact independently;
  revisions are the replicated, comparable ordering (ADR 0003).

## Consequences

- Task 4a writes the catalog under `vshards/<id>/catalog/` with object identity
  from (2).
- Task 4c implements tier-0 shared segments + extents, tier ≥ 1 VShard-pure
  convergence, VShard-prefixed index keys, and `[minRev,maxRev]` block ranges.
- Task 4d implements snapshot extraction (tier-0 extents + pure objects + index
  prefix extract + tombstones) and pin files with controller leases.
- The `virtual_shard_count`/`format_version` stamp is validated on every object
  open; a mismatch fails closed (a store from a different VShard count is not
  silently opened).

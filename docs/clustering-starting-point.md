# Cluster Starting Point: Storage Layout Foundation

**Status:** In implementation on `cluster-design`

**Parent design:** [Cluster Architecture and Implementation Plan](clustering.md)

**Scope:** The first independently reviewable epic on the path to clustering

## Baseline after merging main

On 2026-07-18, `cluster-design` merged `main` at `bcd7f82`. Main now honours
`server.data_dir` for the legacy shard paths and includes end-to-end coverage
from `90c7ce3`. That behaviour is the baseline: this branch must preserve it.
The remaining layout work replaces global path lookup with an injected,
immutable path authority; it does not reimplement or temporarily disable the
working configured root.

## Decision

The first cluster implementation work should be a single-node storage-layout
foundation, preceded by a fail-closed guard around the current CPU-core-count
rebalance.

This epic will:

1. Stop startup from automatically running a migration that can make series
   metadata undiscoverable.
2. Introduce one immutable `StorageLayout` value that owns every persistent
   path.
3. Preserve the now-working `server.data_dir` behaviour while replacing global
   path lookup with an immutable, injected `StorageLayout`.
4. Preserve the current `shard_N` on-disk layout under that root so this change
   does not also become a storage-format migration.
5. Establish the seam needed for the later move from CPU-core shards to stable
   virtual shards.

Do not begin with Raft, etcd, node discovery, or cross-machine RPC. Those
components would have no safe, movable replicated unit while files are still
addressed by the number of CPU cores in one process.

## Why this is the starting point

The current repository has three concrete blockers below the proposed cluster
protocol:

- `server.data_dir` is now wired through global helpers, but path construction
  and configuration lookup remain implicit dependencies. A later VShard
  migration would otherwise need to change many components at once.
- Persistent ownership is encoded as `shard_N`, where N is a Seastar CPU core.
  CPU-core ownership is not a stable unit that can move between machines.
- The startup core-count rebalancer creates an empty destination NativeIndex.
  Existing TSM blocks contain only `SeriesId128`, so the lost discovery
  metadata cannot be reconstructed from those blocks alone.

This work is useful before clustering, has a small correctness surface, and can
be fully tested on one machine. It deliberately separates path correctness
from the much larger storage-format and consensus changes.

## Alternatives considered

### Start with Multi-Raft

Rejected as the first step. The replicated log would not yet have a stable
storage owner or a complete durable state machine to apply into. A working
network demo at this stage would give false confidence while leaving recovery
and movement undefined.

### Start with etcd and placement maps

Rejected as the first step. Desired placement is only control-plane intent. It
cannot safely move a `shard_N` directory whose contents mix unrelated future
VShards, and it must not become a substitute for the data consensus protocol.

### Start with the complete VShard file-format rewrite

Rejected as too broad. It combines path plumbing, series catalog durability,
WAL semantics, TSM partitioning, compaction, migration, and routing. The layout
foundation gives that work a stable API and lets reviewers verify one class of
change at a time.

### Continue using the process working directory

Rejected. Working-directory state is difficult to isolate in tests and unsafe
operationally. It also makes node-local storage configuration misleading.

## Non-goals

This epic does not:

- change the TSM or WAL formats;
- introduce VShard directories or move data between shards;
- add Raft, cluster membership, network protocols, or etcd;
- claim replicated durability or high availability;
- change series routing or query consistency;
- make NativeIndex rebuildable from existing TSM data;
- repair the current core-count rebalancer; or
- promise that a single hot series scales across nodes.

The rebalancer is disabled on unsafe core-count changes until a metadata-safe
offline or VShard-aware migration is implemented.

This is a temporary legacy-format guard, not the final scaling behaviour. In
the VShard format, CPU shards become persisted OSD-like storage workers. Adding
workers after a process restart will redistribute a proportional subset of
stable VShard ownership, while replicas continue to use distinct machines as
their failure domains. The parent design's
[CPU storage-worker contract](clustering.md#cpu-shards-as-osd-like-storage-workers)
defines the replacement path.

## Design rules

### One root, one path authority

Production code must obtain persistent paths from `StorageLayout`. Storage
components must not independently concatenate `shard_`, `tsm`, WAL filenames,
or control filenames.

The initial API should be a small immutable value, for example:

```cpp
namespace timestar {

class StorageLayout {
public:
    explicit StorageLayout(std::filesystem::path root);

    const std::filesystem::path& root() const noexcept;
    std::filesystem::path shardDir(unsigned shard) const;
    std::filesystem::path tsmDir(unsigned shard) const;
    std::filesystem::path tsmFile(unsigned shard,
                                  unsigned tier,
                                  uint64_t sequence) const;
    std::filesystem::path walFile(unsigned shard,
                                  uint64_t sequence) const;
    std::filesystem::path nativeIndexDir(unsigned shard) const;

    std::filesystem::path placementFile() const;
    std::filesystem::path shardCountMetadataFile() const;
    std::filesystem::path rebalanceStateFile() const;
    std::filesystem::path rebalanceStagingDir() const;

private:
    std::filesystem::path root_;
};

} // namespace timestar
```

The exact method names may change during implementation, but there must be one
owner for these decisions.

### No hidden filesystem I/O

`StorageLayout` performs lexical path operations only. It must not call
`canonical()`, inspect the disk, create directories, or block a Seastar reactor
thread. Root validation and directory creation remain explicit startup work.

Use `lexically_normal()` where normalization is needed. Decide whether a root
is absolute during startup, before it is injected into sharded services. Do not
silently resolve symlinks differently on different calls.

### Immutable dependency, not a global

Create the layout once from the loaded server configuration and inject it into
Engine and storage services. A global path singleton would make tests interfere
with each other and would preserve the current hidden dependency on process
state.

### Backward-compatible default

With the default root `.` the resulting directory and filename structure must
remain compatible with existing `shard_N` data. This epic changes the path
authority, not the layout version.

All artifacts must switch to the configured root in the same activation
change. A process must not write WAL under one root and index or placement
metadata under another.

### Fail closed on ambiguous data

Startup must refuse to proceed if it detects a CPU-core-count change for an
existing store. The error must report:

- configured data root;
- previous and requested shard counts;
- why automatic migration is unsafe; and
- the supported recovery or rollback action.

It must fail before renaming, moving, deleting, or creating replacement shard
contents.

## Implementation sequence

Each step is intended to be a separate, reviewable pull request or commit. Do
not combine later VShard or consensus code with these changes.

### Step 0: Update the isolated branch from main — completed

The isolated worktree first rebased its documentation commits, then merged
current `main` at `bcd7f82` as commit `a9506d4`. The only Task 1 conflict was in
server startup; resolution retained main's `data_dir` wiring and replaced the
unsafe normal-startup rebalancer path with the safety session.

This operation must not check out, reset, clean, or modify the main worktree.

Exit criteria:

- the isolated worktree is based on the intended current main commit;
- the documentation commits remain present; and
- the worktree is clean before implementation begins.

### Step 1: Replace automatic rebalance with a startup safety gate

Split core-count inspection from mutation. Startup needs a read-only decision
with states equivalent to:

```text
FreshStore
MatchingShardCount
UnsafeShardCountChange(previous, requested)
InterruptedLegacyRebalance
InvalidMetadata(reason)
```

Required behaviour:

- A fresh store records the current count as part of successful initialization.
- A matching count starts normally.
- An existing store with a different count fails before storage services open.
- An interrupted legacy rebalance fails with explicit operator guidance; it is
  not automatically resumed by normal server startup.
- Missing or malformed metadata is treated conservatively when shard data is
  present.
- A committed shard must have a fully decoded NativeIndex manifest with valid
  framing and CRCs, and every referenced SSTable must exist as a regular file
  of the committed size.
- The configured root remains bound to the locked directory inode throughout
  inspection and commit; root replacement fails closed.
- The mutating legacy rebalancer may remain for tests or a future explicit
  offline tool, but normal startup must not call `execute()` or
  `recoverIfNeeded()`.

Likely files:

- `bin/timestar_http_server.cpp`
- `lib/storage/shard_rebalancer.hpp`
- `lib/storage/shard_rebalancer.cpp`
- `test/unit/storage/shard_rebalancer_test.cpp`

Tests:

- fresh empty root;
- matching shard count;
- mismatch with WAL, TSM, or index data present;
- malformed shard-count metadata;
- interrupted legacy rebalance marker;
- assertion that the failure path did not rename or create shard contents.
- empty, random, truncated, unsupported-version, and bad-CRC manifests;
- missing, wrong-type, and wrong-size manifest-referenced SSTables;
- root replacement and control-file insertion between inspect and commit; and
- behavioural enforcement of inspect → authorize mutation → initialize →
  commit ordering.

The lifetime directory lock is advisory. Operators must not run an older
TimeStar binary, which does not participate in this locking protocol, against
the same data root concurrently. The new startup path also uses descriptor-
relative inspection and verifies the configured path still names the locked
inode; the advisory-lock limitation is not presented as protection from a
non-cooperating process.

Exit criteria:

- changing `--smp` on an existing store produces a clear startup error;
- no automatic code path can replace a populated NativeIndex with an empty
  one; and
- unchanged-count restart behaviour remains intact.

### Step 2: Add and unit-test `StorageLayout`

Add `lib/storage/storage_layout.hpp` and, if needed, a small implementation
file. Keep it independent of Seastar so path rules can be tested cheaply.

The tests must cover:

- default root `.`;
- relative and absolute roots;
- roots containing spaces;
- multiple shard, tier, and sequence values;
- control files and rebalance staging paths;
- lexical normalization without filesystem access; and
- compatibility with the existing default filenames.

Use typed values for shard IDs and sequence numbers where the existing APIs
make that practical, but do not introduce the full VShard type in this step.

Likely files:

- `lib/storage/storage_layout.hpp`
- `lib/storage/storage_layout.cpp`
- `test/unit/storage/storage_layout_test.cpp`

Exit criteria:

- every persistent path required by current production startup has a layout
  method;
- path tests require no reactor or real data files; and
- the default-root expectations exactly describe the current on-disk names.

### Step 3: Inject the layout without changing the default layout

Thread the immutable layout through the storage owners, then remove their local
path builders. Components known to require conversion include:

| Component | Current path responsibility | Target |
| --- | --- | --- |
| `Engine` | creates and returns `shard_N` paths | owns/injects layout |
| `TSMFileManager` | derives shard from reactor and builds TSM paths | asks layout using explicit shard |
| `TSMCompactor` | generates compacted TSM filenames | asks layout |
| WAL classes | generate `shard_N` WAL filenames | ask layout |
| `NativeIndex` | opens `shard_N/native_index` | asks layout |
| `ShardRebalancer` | builds root and staging paths | asks layout |
| server startup | placement and shard-count control files | asks layout |
| benchmark reporting | scans `shard_N` below the working directory | scans paths supplied by layout |

Where a component already receives its owning shard explicitly, retain that
value. Where it implicitly calls `seastar::this_shard_id()`, pass the identity
from its owner when feasible so path selection is deterministic in tests.

Likely files include:

- `lib/core/engine.hpp` and `lib/core/engine.cpp`
- `lib/storage/tsm_file_manager.hpp` and `.cpp`
- `lib/storage/tsm_compactor.hpp` and `.cpp`
- `lib/storage/wal.hpp` and `.cpp`
- `lib/storage/memory_store.hpp` and `.cpp`, if it constructs WAL objects
- `lib/index/native/native_index.hpp` and `.cpp`
- `lib/storage/shard_rebalancer.hpp` and `.cpp`
- `bin/timestar_http_server.cpp`
- `bin/timestar_benchmark.cpp`
- affected unit fixtures

Constraints:

- avoid a compatibility constructor that silently falls back to `.` in
  production code;
- test-only conveniences must be visibly test-only;
- convert `std::filesystem::path` to strings only at APIs that require them;
- do not perform directory or metadata I/O inside the path object; and
- keep all data structures and file formats unchanged.

Exit criteria:

- the default configuration passes existing storage, index, compaction, WAL,
  restart, and rebalancer safety tests;
- production path literals are centralized;
- no leaf storage class reads the global configuration to discover its root;
  and
- source inspection finds no independent `shard_` path construction outside
  `StorageLayout`, legacy-layout parsing/migration code, and tests.

### Step 4: Preserve and centralize `server.data_dir`

Main already activated the configured root. Once all consumers are injected,
remove their global path lookup while preserving that root atomically for:

- shard directories;
- WAL files;
- TSM files and compaction output;
- NativeIndex;
- `placement.json`;
- `shard_count.meta`;
- rebalance state and staging; and
- any temporary files that must be crash-atomically renamed with those files.

Startup must create or validate the root explicitly before sharded storage
services start. It must report the selected root in operational logs without
printing unrelated configuration secrets.

Integration tests must use a unique temporary root and verify:

1. start with a non-default `data_dir`;
2. ingest enough data to exercise WAL and persisted TSM/index state;
3. stop cleanly;
4. restart from the same root;
5. query and discover the original series; and
6. confirm no TimeStar data artifacts appeared in the process working
   directory.

Also test the default `.` root against an existing-layout fixture. If a root
contains an unsupported mixture of old and new partial state, startup must fail
with a diagnostic rather than choose one silently.

Exit criteria:

- a configured data root contains all persistent server state;
- restart from that root preserves data and discovery metadata;
- the working directory remains clean when a different root is selected; and
- default-root backward compatibility is covered by a test.

### Step 5: Close the epic and define the next storage contract

Do not add an unused generalized ownership hierarchy merely to anticipate
VShards. Instead, record the requirements learned from layout injection and
write the next implementation plan around an explicit stable VShard identity.

The next epic begins only after this plan's acceptance gates pass. Its order is:

1. define the local durable acknowledgement boundary and group-commit tests;
2. make the series catalog durable and rebuildable;
3. partition WAL, TSM, index visibility, deletes, and compaction by VShard;
4. prove one VShard can snapshot, restore, and move locally; then
5. put that complete state machine behind a Multi-Raft prototype.

## Verification plan

### Static checks

Use repository searches as a review aid, not as the sole test:

```sh
rg -n 'shard_|placement\.json|shard_count\.meta|rebalance\.state' \
  bin lib --glob '*.{cpp,hpp}'
rg -n 'data_dir' bin lib test
```

Every remaining production match must be either the central layout,
legacy-layout recognition, a log message, or a non-path use that a reviewer can
explain.

### Targeted tests

At minimum, run the focused suites for:

- `StorageLayout`;
- shard-rebalance startup safety;
- WAL filename generation and recovery;
- TSM creation and compaction;
- NativeIndex open/reopen;
- Engine restart; and
- configuration parsing of `server.data_dir`.

Test filters should use the names present after implementation rather than
hard-coding an assumed command here.

### Full validation

Run the repository's configured format/lint checks, warning-as-error build, and
complete unit/integration test suite. The full restart test is required even if
all pure path tests pass: path consistency is a system property across several
components.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Constructor changes spread across Seastar sharded services | Land the pure layout first, then inject one ownership chain at a time while retaining default-path tests |
| Tests accidentally depend on the process working directory | Give each integration fixture its own temporary root and assert both expected and forbidden files |
| Only some artifacts honour `data_dir` | Activate the configured root only after every consumer is injected; verify a restart and inspect the working directory |
| Root normalization changes symlink behaviour | Use lexical normalization only and document whether startup stores a relative or absolute path |
| Legacy data is mutated before mismatch detection | Run the read-only safety gate before opening storage services or creating shard directories |
| Static WAL filename helpers encourage hidden defaults | Make root/layout an explicit argument or convert the helper into a layout method |
| A broad abstraction anticipates the wrong VShard design | Keep `StorageLayout` concrete for today's physical shards; extend it when the VShard contract is specified |

## Epic definition of done

The starting epic is complete only when all of the following are true:

- normal startup never runs the unsafe automatic core-count rebalancer;
- all persistent paths are derived from one injected immutable layout;
- `server.data_dir` contains all storage and node-local control artifacts;
- the default root opens the existing `shard_N` layout without migration;
- a non-default-root ingest/restart/query test passes;
- existing storage, index, WAL, and compaction tests pass;
- a core-count mismatch fails before mutating stored data;
- warning-as-error and full test validation pass; and
- all work remains on the isolated cluster branch/worktree.

Passing this gate does not make TimeStar clustered. It creates the safe local
storage boundary on which the VShard and replicated-state-machine work can be
built without immediately rewriting every storage component again. The
fail-closed rule remains for legacy `shard_N` data; the subsequent VShard
storage epic replaces it with automatic storage-worker rebalancing for the new
format.

# Stable VShards and Local Storage Workers

**Status:** Tasks 1, 2a, 2b, and 3a complete on `cluster-design`; Task 3b next

**Parent design:** [Cluster Architecture and Implementation Plan](clustering.md)

**Prerequisite:** [Storage Layout Foundation](clustering-starting-point.md)

## Outcome

This epic replaces CPU-core storage identity with stable VShard identity and
persisted, OSD-like node-local storage workers. Adding one CPU worker after a
restart should rebalance only its proportional share of VShards. Removing CPU
capacity must keep every old worker addressable until its VShards have been
fenced, handed off, and drained.

This does not make reactor cores replica failure domains. Replica placement is
host-first; local worker selection is a separate second stage.

## Non-negotiable state separation

Three maps have different authority and must not be collapsed:

1. **Desired local placement** says which active worker should eventually
   execute each VShard. It is deterministic control-plane intent.
2. **Effective local ownership** records the fenced ownership generation that
   may currently serve each VShard. Requests route by this map during handoff.
3. **Runtime worker mapping** assigns every persisted active or draining worker
   to a reactor core for this process lifetime. It is execution state, not
   storage identity or replica placement.

A desired-placement delta is not an executable move plan. Later handoff code
must quiesce the old owner, durably advance a generation, open the new owner,
and only then change effective routing. No task in this epic may infer that a
desired owner is already safe to serve.

## Placement algorithm version 1

The cluster uses 4,096 fixed VShards. Local desired placement uses deterministic
integer-ticket highest-random-weight rendezvous:

- A worker weight is a persisted **absolute number of capacity tickets**.
- CPU workers initially receive one ticket each.
- Tickets are numbered from zero through `weight - 1`.
- For a VShard, every active worker ticket receives a frozen 64-bit score; the
  worker holding the highest ticket owns the desired slot.
- Draining workers remain registered and runtime-mapped but receive no desired
  VShards.

With independent uniform scores, a worker's expected share is its ticket count
divided by total tickets. Adding one ticket can only retain the old winner or
move a VShard to the worker receiving that ticket. Removing a worker can only
move VShards that worker won.

Weights are deliberately not ratio-normalized: `1:2` and `10:20` have the same
expected proportions but different candidate sets and therefore different
placements. The weight quantum, algorithm version, 64-bit mixing constants,
tuple encoding, ticket numbering, limits, and golden vectors are storage
compatibility decisions. Changing any of them requires a versioned placement
migration.

Version 1 bounds the model to 256 workers, 1,024 tickets per worker, and 8,192
aggregate tickets. At 256 equal workers the expected minimum share is 16
VShards. Placement also fails closed if any active worker actually receives
zero VShards, because accepting a worker that cannot own load would make the
scale-by-one claim false. Metadata outside these worker/ticket limits fails
closed instead of causing unbounded startup work.

## Implementation tasks

### Task 1: Pure desired placement and runtime mapping

- Introduce stable `StorageWorkerId`, active/draining records, and versioned
  integer-ticket rendezvous.
- Produce a `DesiredPlacementDelta` by comparing desired placement with an
  explicit effective-owner table.
- Fail if an effective owner is absent from the worker registry; hard removal
  is illegal until effective ownership is zero.
- Map every persisted active or draining worker deterministically onto the
  available reactor cores using capacity-ticket-balanced assignment, including
  co-scheduling after `--smp` shrink.
- Keep the model disconnected from `PlacementTable`, `placement.json`, Engine,
  server startup, and legacy `shard_N` routing.

Gate: golden vectors are stable; input order is irrelevant; one-worker growth
moves approximately `1/(N+1)` VShards and only to the new worker; draining
moves only the draining worker's VShards; invalid or hard-removed ownership
fails closed.

### Task 2: Durable worker registry

- Task 2a defines the pure registry state machine and canonical `workers.json`
  codec. Format version 1 records the placement-algorithm version, generation,
  next monotonic worker ID, sorted worker records, and checksum. The checksum is
  CRC-32/ISO-HDLC over fixed-width little-endian fields and excludes the JSON
  checksum field. Decoding accepts only the exact compact encoder output and is
  bounded to 64 KiB before parsing, closing duplicate-key and alternate-JSON
  ambiguities.
- Task 2b places `workers.json` under `StorageLayout` and adds the locked,
  descriptor-relative durable I/O protocol. Updates use an exclusive temporary
  file, complete write, file fsync, close, atomic rename, and root-directory
  fsync. Recovery re-fsyncs a complete valid next-generation temporary file
  before rolling it forward, and re-fsyncs the directory before accepting a
  final-only generation after an ambiguous crash. With a valid final file it
  durably cleans partial or stale scratch state. Without a final file, invalid
  temporary evidence is preserved and startup fails closed. Corrupt
  authoritative state is never masked. Reserved names are opened nonblocking
  and never followed, so a FIFO or symlink cannot hang or redirect startup.
- The store reuses the server-lifetime root lock, checks the configured root
  inode before and after namespace mutations, and rejects a mismatched or
  replaced root. Rename and install-failure cleanup additionally require the
  reserved pathname to still name the exact device/inode that this install
  created or recovery validated. Rename also revalidates the exact canonical
  bytes, not only the inode, for both the prepared source and the authoritative
  destination immediately before replacement. After the last crash-test
  observer the store repeats an unobservable file-and-directory fsync pass,
  then rechecks root binding, identity, and contents before acknowledging
  success. An `O_EXCL` collision or pathname substitution preserves the
  pre-existing artifact and fails closed.
- Process-crash injection covers every create/write/fsync/close/rename boundary
  in installation and every fsync/remove/rename boundary in recovery. These
  tests model process exit: each child independently acquires the root lock and
  the recovering parent reacquires it after child exit. The ordered file and
  directory fsync protocol supplies the power-loss durability contract. The
  codec/store remain disconnected from live Engine routing.
- Create worker IDs monotonically when CPU capacity grows; never recycle IDs.
- On shrink, retain excess workers, co-schedule them, and mark them draining.
- Install updates through fsync-plus-atomic-rename under the existing root lock.
- Reject corrupt, duplicate, zero-ID, unsupported-version, or invalid-transition
  state. A CRC detects corruption, not a valid historical rollback; freshness
  is enforced by comparing candidates to the last accepted generation.

No local file format can detect rollback of an entire data-root snapshot when
every generation witness is rolled back together. This phase detects rollback
between the final and temporary candidates it can observe. A future replicated
control plane must provide the external epoch witness needed to reject a
coordinated whole-root rollback.

Gate: crash injection at every write/rename/fsync boundary recovers one complete
registry generation when one exists, and otherwise fails closed while
preserving ambiguous evidence. A missing reactor never makes an old worker's
data unreachable.

### Task 3: VShard layout and effective ownership generations

- Add VShard-addressed paths without reusing `shard_N` as identity.
- Persist a checksummed effective owner and monotonically increasing ownership
  generation for every VShard.
- Fence opens and requests by `(VShardId, generation)`.
- Refuse simultaneous service by old and new workers in one generation.

Gate: fault injection cannot produce two serving owners or an ownerless
acknowledged generation.

Task 3a implements the pure ownership state machine and codec, still
disconnected from filesystem storage and live Engine routing:

- An immutable `EffectiveVShardTarget` owns the exact accepted worker-registry
  snapshot, its generation, and a 128-bit XXH3 fingerprint of its canonical
  encoding. It calculates the 4,096-entry desired placement once, rather than
  rebuilding rendezvous scores for every handoff.
- The effective layout stores a monotonic layout revision, target registry
  generation and fingerprint, and one `(owner, ownership generation)` entry per
  VShard. The target names desired intent being reconciled; it does not claim
  convergence. A registry target may jump forward across skipped intent while
  old effective owners remain recorded.
- One changed revision is either witness-only, without changing any ownership
  generation, or changes exactly one mismatched owner to the current desired
  Active worker and increments only that VShard generation. Impossible history
  is rejected when an entry generation exceeds the layout revision or the sum
  of ownership changes exceeds the available revision history.
- Task 3a deliberately exposes no runtime authorizer or publication API. It
  freezes the `(VShard, generation, owner)` fence data shape, but a raw decoded,
  cached, or speculative layout cannot mint an accepted fence or authorize
  work. Non-copyability alone would not prove uniqueness because callers could
  construct two independent authorities from old and new snapshots.
- Format v1 is a fixed 49,212-byte binary with frozen magic, explicit
  little-endian header and entry fields, exactly 4,096 implicit/index-ordered
  entries, exact-length/canonical re-encoding, and a trailing CRC-32/ISO-HDLC.
  The CRC detects corruption; the registry fingerprint binds cross-file state;
  neither supplies authenticity or detects coordinated whole-root rollback.

The pure transition is only a proposal. It does not prove that an operation
admitted under the old fence has finished, nor that the destination is ready.
Task 5 must prepare the destination without serving requests; quiesce and drain
reads, writes, deletes, compaction, retention, and all other background work at
the prior fence; durably commit the ownership revision; publish it through the
single current authority; activate the destination; and only then acknowledge
the cutover. Task 3b/5 must make that authority root-bound and shared by every
handle, with private construction requiring opaque accepted durable-state and
quiesced-prior-fence capabilities. Task 3b must also allow generation-one
creation only on a provably fresh root (or explicit migration), fail closed if
authoritative ownership state unexpectedly disappears, serialize
registry/layout updates, and reject same-revision divergence during crash
recovery. Those rules prevent local fence ABA; a coordinated whole-root
rollback still needs a future external epoch witness.

### Task 4: Complete VShard storage boundary

- Make the series catalog durable and rebuildable.
- Partition journal/WAL, index visibility, deletes, retention, compaction, and
  TSM manifests by VShard.
- Define and test the durable acknowledgement boundary before replication.
- Prove one VShard snapshot includes every query-visible state component.

Gate: snapshot/restore preserves logical hashes and discovery metadata while
unrelated VShards remain byte-for-byte untouched.

### Task 5: Fenced local handoff and automatic rebalance

- Reconcile desired deltas one VShard at a time with bounded concurrency.
- Quiesce writes, drain reads, persist the new generation, and transfer runtime
  ownership without rewriting shared-volume TSM data.
- Persist resumable jobs and enforce foreground load/disk-watermark backpressure.
- Retire a draining worker only after effective ownership reaches zero.

Gate: continuous ingest/query/delete tests survive crashes at every transition;
adding one worker increases usable throughput and moves only its proportional
share; shrinking preserves reachability until drain completes.

## Boundary with multi-machine replication

This epic is node-local. Multi-machine placement later chooses distinct hosts
for each VShard replica, then chooses one eligible storage worker inside each
host. Multiple workers on one host may improve CPU and I/O distribution, but
they never satisfy multiple replica slots and never raise the advertised HA
failure-domain count.

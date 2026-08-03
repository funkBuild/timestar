# Exact-v1 cluster operations

This is the operator contract for the greenfield RF=3 implementation. Every
wire and durable boundary is marked version 1 and updated in place; there is no
mixed-version rollout, old-format reader, downgrade, or migration promise.
Unknown versions fail before mutation. Recreate development data after an
incompatible v1 change.

Production approval is still blocked until the distinct-host fault matrix and
approved deployment SLOs in
[cluster-production-readiness.md](cluster-production-readiness.md) are recorded
against one exact candidate. The procedures below describe the shipped
behavior; they do not waive that evidence requirement.

## Deployment contract

Use at least three nodes, with each RF=3 voter in a distinct host/AZ/rack
failure domain. Give every node its own durable data volume and stable DNS or IP
address. A three-node initial configuration uses the same ordered peer list,
cluster UUID, RF, and control seed on every node; only `node_id`,
`failure_domain`, certificate, key, and data directory differ.

```toml
[server]
data_dir = "/var/lib/timestar"
auth_enabled = true
auth_token = "<injected secret>"
shutdown_timeout_seconds = 120

[cluster]
enabled = true
partitioned = true
replication_factor = 3
node_id = 1
peers = ["ts-a.internal:8086", "ts-b.internal:8086", "ts-c.internal:8086"]
cluster_uuid = "<32 lowercase hex characters>"
control_enabled = true
control_seed_node_id = 1
failure_domain = "az-a"
tls_cert_file = "/run/secrets/timestar-node.crt"
tls_key_file = "/run/secrets/timestar-node.key"
tls_ca_file = "/run/secrets/timestar-cluster-ca.crt"
backup_auth_key_file = "/run/secrets/timestar-backup-auth.key"

[seastar]
smp = 4
memory = "2G"
```

The 4-reactor/2-GiB shape is the bounded local qualification profile, not a
universal capacity recommendation. Replace it only with the exact deployment
shape whose SLO report is approved. Four reactors at 1 GiB is invalid: it begins
at the 256-MiB-per-reactor ingest free-memory floor and sheds all writes.

Before release qualification, copy
`test/cluster_gates/production_slo_policy.example.v1.json` into protected
release evidence and replace every placeholder with the approved policy name,
authority, change/release reference, and UTC validity window of at most 366
days. The deployment shape must be the one that will ship; thresholds may be
stricter but cannot be weaker than the repository's bounded safety envelope.
Validate and use it as follows:

```sh
python3 test/cluster_gates/production_slo_policy.py \
  --policy /protected/timestar-production-slo.v1.json
GATE_SLO_POLICY_FILE=/protected/timestar-production-slo.v1.json \
  test/cluster_gates/production_slo_report.sh
```

Without `GATE_SLO_POLICY_FILE`, the collector remains useful as a local
regression but marks its report `provisional`. The distinct-host preflight
rejects provisional, expired, hash-mismatched, or policy-divergent reports.

Replicated transport listens on HTTP port + 1000 for data RPC and HTTP port +
2000 for Raft RPC. Permit those ports only between cluster nodes. The
certificate SAN must match that node's configured address; a certificate valid
for another peer is rejected. Leave
`cluster.development_allow_insecure_transport` and every `TIMESTAR_UNSAFE_TEST_*`
variable unset.

Client HTTP remains plaintext at the process. Put it behind a TLS operator/data
ingress, apply network policy and rate limits there, and keep server bearer
authentication enabled. Protect the bearer token, TLS keys, and backup HMAC key
as different secrets.

## Enforced resource bounds

| Resource | Exact-v1 bound/default |
|---|---|
| Open files | replicated startup obtains at least 8,192 or exits before Engine/Raft opens |
| HTTP write/query bodies | 64 MiB / 1 MiB by default |
| Originated write memory | 32 MiB per reactor; excess concurrent work returns retryable 503 |
| Peer-ingress write memory | separate 32 MiB per reactor budget |
| Data RPC admission | 128 MiB estimated resident memory per reactor; outbound frame at most 10.67 MiB |
| Raft RPC admission | 64 MiB per reactor; payload/send ceilings 14/18 MiB |
| Uncommitted Raft proposals | 64 MiB aggregate and 14 MiB per group per reactor |
| Snapshot transfer | 4 MiB chunks, four concurrent outbound transfers per reactor |
| Snapshot file | at most 1 TiB per VShard; in-memory control snapshots at most 128 MiB |
| Snapshot production | eligible at 8,192 entries or 64 MiB, minimum 60 s per group; sequential per reactor |
| Journal retention | 1 MiB private segments; snapshot/GC once a minute bounds live suffix and reclaims sealed prefixes |
| Background storage | six WAL conversions per reactor; two compactions/process and 256 MiB compaction memory by default |

The write admission byte overrides accept positive decimal bytes only. Snapshot
threshold overrides are qualification seams and must appear in the recorded
deployment environment. Do not enable the optional shared journal for the
release or restore workflow.

Capacity planning must also reserve encrypted staging space for one complete
backup plus export/import working state, and enough local headroom for snapshots,
compaction, and the 24-hour retired-replica quarantine. Alert before the data
filesystem or inode pool approaches exhaustion.

## Bootstrap and health

1. Prepare all three empty data roots and node-specific mTLS credentials.
2. Start the configured control seed once with `--cluster-init`; start the other
   configured nodes normally. Ordinary startup never initializes Group 0.
3. Require HTTP 200 from `/health` on every node.
4. Inspect `/cluster/status` on every node. Require the same 32-hex
   `cluster_uuid`, distinct `node_id` and `failure_domain`, `protocol_version:1`,
   RF=3, exactly 4,096 leaders and 12,288 hosted replicas in aggregate, one
   positive serving-map epoch, healthy Group 0, and zero apply, tick, or
   durability failures.

`/health` is traffic readiness. `/cluster/status.control_locally_ready` proves
locally applied current-term control state, not a fresh quorum round; mutating
operator calls remain the authoritative control-quorum check.

## Join, move, drain, and remove

All mutation routes require the bearer token and should be reached only through
the protected operator ingress. Join a fresh configured observer by minting a
one-use token on the current control leader and sending `{"token":"..."}` to
`POST /cluster/join` on that node. Retry `joining` responses until `active`.
Never place a token in a log or command history.

Move one VShard with an explicit job identity and the currently observed map
epoch:

```http
POST /cluster/vshards/move
Authorization: Bearer <operator token>
Content-Type: application/json

{"job_id":"move-0042","map_epoch":17,"vshard":42,"destination":4,"victim":3}
```

Omit `victim` for growth. HTTP 409 is a stale epoch, redirect, or policy
conflict; read status and retry only after determining which. HTTP 503 means the
control plane is unavailable. Do not invent a new job ID to hide an ambiguous
response.

Drain with `POST /cluster/nodes/drain {"node":3}`. The Group-0 controller moves
one referenced VShard at a time and resumes after leader loss. Wait for
`control_drain_references:0`, no pending topology job, stable RF=3 totals, and
healthy readback before calling `POST /cluster/nodes/remove`. Removal is refused
while any serving-map, data-voter, or meta-voter reference remains. Stop and
decommission the host only after its Removed record is applied and Group-0
membership cleanup is visible.

## Release qualification on distinct hosts

First generate the local exact-candidate report with
`test/cluster_gates/production_slo_report.sh`. It authenticates both the server
and insert driver and records their hashes. Then run the read-only preflight
against every deployed node:

```sh
python3 test/cluster_gates/multi_host_candidate_preflight.py \
  --candidate-report build/tmp/tsgate_slo_report/report.v1.json \
  --node https://ts-a.example.internal \
  --node https://ts-b.example.internal \
  --node https://ts-c.example.internal \
  --ca-file /protected/operator-ca.crt \
  --output build/tmp/multi-host-before.v1.json
```

Use `--client-cert` and `--client-key` together when the ingress requires mTLS,
and `--bearer-token-file` when it protects even public diagnostic routes. Plain
HTTP, redirects, embedded credentials, and bearer tokens containing HTTP
control characters are rejected. The preflight also rejects loopback,
link-local, or reserved addresses; endpoint or inter-node peer DNS/IP overlap;
peer-map disagreement; duplicate failure domains; mixed
components/revisions/cluster UUIDs; disabled snapshotting; the optional shared
journal; unstable maps or lifecycle work; incomplete RF=3 totals; and any
apply/durability fault. Every endpoint must cover the exact all-voter Group-0
and inter-node peer topology. The deployed reactor count and its aggregate
64-MiB-per-reactor uncommitted-proposal budget must match the high-volume server
profile authenticated by the candidate SLO report. Retain separate
infrastructure evidence that the report's per-process memory setting is also
the deployed limit. Run the preflight before and after every fault arm and
retain server, load-client, infrastructure, and preflight transcripts.

After each recovered post-fault preflight, bind it to the pre-fault report and
to distinct transcripts for the infrastructure action, workload, and every
qualified server:

```sh
python3 test/cluster_gates/multi_host_evidence_bundle.py \
  --before build/tmp/multi-host-before.v1.json \
  --after build/tmp/multi-host-after.v1.json \
  --fault-arm bidirectional-partition \
  --infrastructure-transcript build/tmp/partition-infrastructure.log \
  --workload-transcript build/tmp/partition-workload.log \
  --server-transcript 1=build/tmp/partition-node1.log \
  --server-transcript 2=build/tmp/partition-node2.log \
  --server-transcript 3=build/tmp/partition-node3.log \
  --output build/tmp/partition-evidence.v1.json
```

The binder permits leadership, term, and Group-0 log progress, but rejects a
different candidate, deployment profile, cluster UUID, node/peer/failure-domain
identity, or stable map epoch. It requires one distinct regular server log per
qualified node and distinct infrastructure/workload logs, hashes each file in
bounded streaming chunks, and fails if a file changes during the read. Retain
the bundle and its referenced immutable evidence together.

On disposable production-equivalent hosts, exercise these arms serially:

1. Authenticated leadership rebalance while reads and idempotent writes run;
   prove unauthenticated mutation returns 401.
2. Stop one voter, measure the write-error band and all-group recovery on the
   surviving quorum, restart it, and require exact readback.
3. Partition one node from the other two in both inter-node directions. The
   minority must stop accepting writes, the majority must continue, and healing
   must restore exact RF=3 state without acknowledged-data loss.
4. On one node's dedicated test volume, inject ENOSPC/directory-sync failure.
   The replica must report a durability fence, stop protocol traffic, and let
   the healthy quorum replace its leadership. Repair or replace the volume,
   restart, and require snapshot catch-up plus exact readback.
5. With one other voter down, replace a node certificate with an otherwise
   trusted certificate for the wrong peer identity. Quorum-dependent writes
   must fail retryably. Restore a certificate for that node's SAN and prove
   recovery without restarting the remaining voter.
6. Repeat backup coordinator loss/resume and the isolated all-voter restore drill
   from [backup-restore.md](backup-restore.md).

Never fill a shared host filesystem or alter firewall policy outside the
disposable qualification environment. Use a dedicated quota, loop device, or
storage fault facility with a separately verified recovery path.

## Monitoring and recovery

Page on `/health` failure, any `raft_durability_failures`, apply failure, control
durability failure, persistent leaderless group, snapshot refusal, a TSM
checksum/structural-integrity error, or growing apply lag. Investigate steadily
increasing pinned journal segments, abandoned snapshot transfers,
uncommitted-proposal refusals, and compaction degradation before capacity is
exhausted. A TSM integrity failure is not repairable in place: keep the affected
generation offline and recover it through replica snapshot catch-up or the
authenticated cluster restore workflow.

After a crash, restart the same node identity only with its original data root,
cluster UUID, peer position, and failure domain. A foreign/corrupt/non-v1 root
must remain offline; do not delete a fence or copy another node's `node.json`,
Group-0 state, map cache, or Raft journal into it. Replace the node through the
join/drain workflow or restore the whole cluster through the authenticated
offline procedure. Cluster backup/export, key retention, and disaster recovery
are defined in [backup-restore.md](backup-restore.md).

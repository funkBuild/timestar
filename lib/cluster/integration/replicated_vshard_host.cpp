#include "replicated_vshard_host.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"
#include "../../utils/logger.hpp"  // timestar::http_log
#include "../control/control_map_cache.hpp"
#include "../data/replicated_write_router.hpp"
#include "../data/write_errors.hpp"
#include "../raft/raft_node.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <utility>

namespace timestar::cluster {

namespace fs = std::filesystem;

namespace {
static_assert(ReplicatedVShardHost::kProposalTimeout == data::ReplicatedBatchWriteRouter::kAttemptTimeout,
              "receiver proposal bound must track one forwarding attempt");
static_assert(ReplicatedVShardHost::kProposalTimeout == data::ReadIndexSink::kAttemptTimeout,
              "receiver ReadIndex bound must track one leader-reach attempt");

uint8_t hexNibble(char c) {
    if (c >= '0' && c <= '9')
        return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f')
        return static_cast<uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F')
        return static_cast<uint8_t>(c - 'A' + 10);
    throw std::invalid_argument("journal identity must contain only hexadecimal characters");
}

std::array<uint8_t, 16> parseUuidBytes(std::string_view value) {
    if (value.size() != 32)
        throw std::invalid_argument("journal identity UUID must contain exactly 32 hexadecimal characters");
    std::array<uint8_t, 16> out{};
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<uint8_t>((hexNibble(value[2 * i]) << 4) | hexNibble(value[2 * i + 1]));
    return out;
}

bool canonicalBackupId(std::string_view value) {
    if (value.size() != 32)
        return false;
    bool nonzero = false;
    for (char c : value) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
        nonzero = nonzero || c != '0';
    }
    return nonzero;
}

std::string readPinnedBackupChunk(raft::PinnedSnapshotFile pin, uint64_t offset, uint32_t maximumBytes) {
    const auto path = pin->path;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        throw std::system_error(errno, std::generic_category(), "open backup capture sidecar: " + path.string());
    auto closeFd = seastar::defer([fd] { ::close(fd); });
    struct stat st{};
    if (::fstat(fd, &st) < 0)
        throw std::system_error(errno, std::generic_category(), "stat backup capture sidecar: " + path.string());
    if (!S_ISREG(st.st_mode) || st.st_size < 0 || static_cast<uint64_t>(st.st_size) != pin->size)
        throw std::runtime_error("backup capture sidecar changed while pinned: " + path.string());
    if (offset > pin->size)
        throw std::out_of_range("backup capture chunk offset exceeds the sidecar size");
    const size_t wanted = static_cast<size_t>(std::min<uint64_t>(maximumBytes, pin->size - offset));
    std::string bytes(wanted, '\0');
    size_t done = 0;
    while (done < wanted) {
        const ssize_t count = ::pread(fd, bytes.data() + done, wanted - done, static_cast<off_t>(offset + done));
        if (count < 0) {
            if (errno == EINTR)
                continue;
            throw std::system_error(errno, std::generic_category(), "read backup capture sidecar: " + path.string());
        }
        if (count == 0)
            throw std::runtime_error("backup capture sidecar was truncated while pinned: " + path.string());
        done += static_cast<size_t>(count);
    }
    return bytes;
}

data::OptDeadline boundedProposalDeadline(data::OptDeadline requested) {
    const auto localLimit = seastar::lowres_clock::now() + ReplicatedVShardHost::kProposalTimeout;
    if (!requested || *requested > localLimit)
        return localLimit;
    return requested;
}

fs::path snapshotDirectoryFor(const fs::path& journalRoot, uint16_t vshard, bool sharedJournal) {
    const auto journalDirectory = sharedJournal ? journalRoot / ("shard_" + std::to_string(seastar::this_shard_id()))
                                                : journalRoot / ("vshard_" + std::to_string(vshard));
    const auto sidecars = journalDirectory / "snapshot_sidecars";
    return sharedJournal ? sidecars / ("vshard_" + std::to_string(vshard)) : sidecars;
}

constexpr std::string_view kRetiredDirectoryPrefix = "v1_vshard_";
constexpr std::string_view kRetiredMarkerPrefix = "retired_at_";
constexpr std::string_view kRetiredMarkerSuffix = ".v1";

bool consumeUnsigned(std::string_view& text, uint64_t& value, std::string_view delimiter) {
    const size_t end = delimiter.empty() ? text.size() : text.find(delimiter);
    if (end == std::string_view::npos || end == 0)
        return false;
    const std::string_view digits = text.substr(0, end);
    if (digits.size() > 1 && digits.front() == '0')
        return false;
    const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size())
        return false;
    text.remove_prefix(end + delimiter.size());
    return true;
}

struct RetiredDirectoryName {
    uint16_t vshard = 0;
    uint64_t epoch = 0;
};

std::optional<uint16_t> activeVShardDirectoryName(std::string_view name) {
    constexpr std::string_view prefix = "vshard_";
    if (!name.starts_with(prefix))
        return std::nullopt;
    name.remove_prefix(prefix.size());
    uint64_t vshard = 0;
    if (!consumeUnsigned(name, vshard, {}) || !name.empty() || vshard >= timestar::VIRTUAL_SHARD_COUNT)
        return std::nullopt;
    return static_cast<uint16_t>(vshard);
}

std::optional<RetiredDirectoryName> retiredDirectoryName(std::string_view name) {
    if (!name.starts_with(kRetiredDirectoryPrefix))
        return std::nullopt;
    name.remove_prefix(kRetiredDirectoryPrefix.size());
    uint64_t vshard = 0;
    uint64_t epoch = 0;
    if (!consumeUnsigned(name, vshard, "_epoch_") || vshard >= timestar::VIRTUAL_SHARD_COUNT ||
        !consumeUnsigned(name, epoch, {}) || epoch == 0 || !name.empty())
        return std::nullopt;
    return RetiredDirectoryName{static_cast<uint16_t>(vshard), epoch};
}

std::optional<uint64_t> retiredAtMillis(std::string_view name) {
    if (!name.starts_with(kRetiredMarkerPrefix) || !name.ends_with(kRetiredMarkerSuffix))
        return std::nullopt;
    name.remove_prefix(kRetiredMarkerPrefix.size());
    name.remove_suffix(kRetiredMarkerSuffix.size());
    uint64_t millis = 0;
    if (!consumeUnsigned(name, millis, {}) || !name.empty())
        return std::nullopt;
    return millis;
}
}  // namespace

JournalIdentity JournalIdentity::fromHex(std::string_view clusterUuid, std::string_view bootId) {
    return JournalIdentity{parseUuidBytes(clusterUuid), parseUuidBytes(bootId)};
}

JournalIdentity JournalIdentity::testing() {
    JournalIdentity out;
    out.clusterUuid.fill(0x11);
    out.bootId.fill(0x44);
    return out;
}

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, std::chrono::milliseconds tick)
    : ReplicatedVShardHost(store, transport, self, std::move(journalRoot), JournalIdentity::testing(), tick) {}

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, JournalIdentity identity,
                                           std::chrono::milliseconds tick)
    : store_(store),
      self_(self),
      journalRoot_(std::move(journalRoot)),
      journalIdentity_(identity),
      registry_(transport, tick) {
    backupCaptureTimer_.set_callback([this] { expireBackupCaptureSessions(); });
}

ReplicatedVShardHost::~ReplicatedVShardHost() {
    // stop() (async: drains ticks, closes journal writers) MUST be called before
    // destruction. Destruction alone tears down the registry (groups) before
    // vshards_ (per the declaration order), which is UAF-safe, but skips writer
    // close() -> fd leak. Warn loudly rather than fail silently.
    if (!stopped_ && !vshards_.empty())
        timestar::http_log.warn(
            "ReplicatedVShardHost destroyed without stop(): {} VShard journal(s) not closed (fd leak)",
            vshards_.size());
}

bool ReplicatedVShardHost::sharedJournalEnabled() {
    // DEFAULT OFF, and the default is the decision, not an oversight (debt D-10).
    // The ordering contract on SharedShardJournal is airtight as reasoned and as
    // unit-tested, but a shared journal changes the FENCE BLAST RADIUS (one bad
    // fdatasync stops every group on the reactor, not one), it makes segment
    // retention cluster-wide (D-34's bricks were written for exactly this layout
    // and still have no caller), and its win is DISK-only -- invisible on the tmpfs
    // box every gate in this tree runs on. Shipping it on by default would trade a
    // measured-at-zero benefit here for a real change to the durability path, which
    // is the wrong side of that bet. Opt in with TIMESTAR_CLUSTER_SHARED_JOURNAL=1.
    static const bool on = [] {
        const char* e = std::getenv("TIMESTAR_CLUSTER_SHARED_JOURNAL");
        return e && e[0] == '1';
    }();
    return on;
}

uint64_t ReplicatedVShardHost::journalFsyncs() const {
    if (sharedWriter_)
        return sharedWriter_->fsyncs();
    uint64_t n = 0;
    for (const auto& [vs, state] : vshards_)
        if (state.writer)
            n += state.writer->fsyncs();
    return n;
}

uint64_t ReplicatedVShardHost::journalSyncRequests() const {
    uint64_t n = 0;
    if (sharedSink_)
        return sharedSink_->syncRequests();
    for (const auto& [vs, state] : vshards_)
        if (state.persistence)
            n += state.persistence->syncRequests();
    return n;
}

seastar::future<> ReplicatedVShardHost::addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts,
                                                  RecoveredConfigValidator recoveredConfigValidator) {
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    // AN IDENTITY-ASSUMING SITE, deliberately left so (debt D-11, ADR 0004). Three
    // things below are keyed by the VShard id AS a group id:
    //   * `registry_.addGroup(vshard, ...)` -- the group id the transport peeks and
    //     the registry maps;
    //   * the journal directory `vshard_<id>/`;
    //   * `EngineDataStateMachine(store_, VShardId{vshard})` -- one state machine per
    //     VShard, where a consolidated group needs one fanning out over its K VShards.
    // Converting these is not indirection, it is the consolidation itself (a group
    // gains a member list, its snapshot becomes a manifest of K VShard snapshots, and
    // its compaction boundary becomes the MIN over them). D-11 converts the ROUTING so
    // that work is local to this file and the snapshot path; see the ADR's "What
    // remains before consolidation is possible".
    //
    // A VShard is hosted at most once: re-adding would open a second JournalWriter on
    // the same dir (two recoverers over one journal) and leak the old writer's fd.
    if (registry_.group(vshard))
        throw std::runtime_error("ReplicatedVShardHost::addVShard: VShard already hosted");
    // A private journal re-add starts a fresh v1 sequence namespace. A terminal
    // watermark from its previous retired generation must never be inherited by
    // the new files.
    if (!sharedJournalEnabled())
        retention_.clearReleased(VShardId{vshard});
    // EVERY group on this shard shares ONE snapshot transfer budget (debt D-37), stamped
    // in here rather than at the ClusterDataPlane call site that builds `opts`: the budget
    // is a per-SHARD object and that site builds one options struct for the whole node.
    // The host outlives the registry (declaration order), so the pointer outlives every
    // RaftNode that holds it.
    opts.snapshotBudget = &snapshotBudget_;
    // The same ownership rule applies to the aggregate uncommitted-log budget
    // (CR-FIX-080). Recovery is accounted when RaftGroup is constructed below,
    // before the group can accept a proposal or tick.
    opts.uncommittedProposalBudget = &uncommittedProposalBudget_;
    VShardState vs;
    vs.seenInServingMap = !static_cast<bool>(recoveredConfigValidator);
    JournalSegmentHeader hdr;
    hdr.clusterUuid = journalIdentity_.clusterUuid;
    hdr.coreNumber = static_cast<uint16_t>(seastar::this_shard_id());
    hdr.bootId = journalIdentity_.bootId;

    // ONE journal per shard (debt D-10, opt-in) or one per VShard (default).
    //
    // Recovery is the SAME code either way: recoverRaftState(records, vshard)
    // filters a record set by VShard and replays it in vshard_seq order, so an
    // interleaved shared stream reconstructs each group's log exactly as a
    // dedicated one does. Physical order never mattered -- ADR 0001 made
    // vshard_seq the ordering key precisely because segment-GC copy-forward can
    // relocate a laggard's records behind its own newer ones.
    if (sharedJournalEnabled()) {
        if (!sharedWriter_) {
            fs::path dir = journalRoot_ / ("shard_" + std::to_string(seastar::this_shard_id()));
            fs::create_directories(dir);
            // A larger rotation target than the per-VShard 1 MiB: this file carries
            // every group on the reactor (~1365 at 4096/3 shards), so the per-VShard
            // size would rotate -- and seal, and fsync, and sync_directory --
            // constantly.
            sharedWriter_ = std::make_unique<JournalWriter>(dir, hdr, sharedJournalSegmentBytes_);
            sharedRecovered_ = co_await sharedWriter_->open();
            sharedSink_ = std::make_unique<SharedShardJournal>(*sharedWriter_);
            timestar::http_log.info(
                "cluster: shard {} opened a SHARED Raft journal at {} ({} record(s) recovered); every group on this "
                "reactor now group-commits through one fdatasync (debt D-10)",
                seastar::this_shard_id(), dir.string(), sharedRecovered_.size());
        }
    }
    std::vector<JournalRecord> ownRecovered;
    if (!sharedSink_) {
        fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vshard));
        fs::create_directories(dir);
        vs.writer = std::make_unique<JournalWriter>(dir, hdr, journalSegmentBytes_);
        ownRecovered = co_await vs.writer->open();
    }
    // Shared mode reads the ONE recovered set (never copied -- it is ~1365 groups'
    // records and every addVShard on this shard needs it); per-VShard mode reads
    // this VShard's own, which is dropped at the end of this frame.
    const std::vector<JournalRecord>& recovered = sharedSink_ ? sharedRecovered_ : ownRecovered;
    const fs::path snapshotDirectory = snapshotDirectoryFor(journalRoot_, vshard, sharedSink_ != nullptr);
    raft::RecoveredRaftState st = raft::recoverRaftState(recovered, VShardId{vshard}, snapshotDirectory);
    if (st.snapshot && st.snapshot->file)
        co_await raft::validateSnapshotFile(*st.snapshot->file);
    // This directory is private to the VShard, so recovery can safely retire
    // partial receives, interrupted producers, old extracted TSM objects and
    // superseded sidecars without racing another group.
    co_await raft::cleanupSnapshotDirectory(snapshotDirectory,
                                            st.snapshot && st.snapshot->file ? st.snapshot->file : nullptr);

    std::vector<NodeId> baseVoters = voters;
    std::vector<NodeId> baseLearners;
    if (st.snapshot && !st.snapshot->config.voters.empty()) {
        // The membership as of the boundary lives ONLY in the snapshot once its
        // ConfigChange entries are compacted away, so it -- not the configured voter list
        // -- is the config floor for a recovered group.
        baseVoters = st.snapshot->config.voters;
        baseLearners = st.snapshot->config.learners;
    }

    vs.persistence = sharedSink_
                         ? std::make_unique<raft::JournalRaftPersistence>(
                               static_cast<JournalSink&>(*sharedSink_), VShardId{vshard}, st.nextSeq, snapshotDirectory)
                         : std::make_unique<raft::JournalRaftPersistence>(*vs.writer, VShardId{vshard}, st.nextSeq,
                                                                          snapshotDirectory);
    // SEED THE RECLAIM FLOOR FROM WHAT WAS ACTUALLY RECOVERED (debt D-34). Mandatory,
    // not an optimisation: a fresh persistence object knows none of the on-disk records'
    // seqs, so its "oldest live entry" would be the first entry appended AFTER this
    // restart -- a much higher seq -- and the floor would jump straight over the
    // recovered log suffix and release the records it is made of.
    vs.persistence->seedRetention(std::move(st.retention));
    if (st.snapshot && st.snapshot->file)
        vs.persistence->seedSnapshotFile(st.snapshot->file);
    // The recovered floor is the starting point for reclamation too, so a node that
    // restarts over an already-compacted journal can collect on its FIRST pass instead
    // of waiting for the next compaction.
    if (const uint64_t floor = vs.persistence->releasedSeq(); floor > 0)
        retention_.setReleased(VShardId{vshard}, floor);
    vs.sm = std::make_unique<EngineDataStateMachine>(store_, VShardId{vshard});
    raft::RaftNode node(self_, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
    // Dynamic destination creation may reopen a journal left by an interrupted
    // earlier movement. Validate the fully recovered active configuration before
    // installing snapshot files or registering a ticking group. A stale journal
    // must fail closed; letting it campaign under an unrelated configuration is
    // worse than leaving the approved move pending.
    if (recoveredConfigValidator && !recoveredConfigValidator(node.config()))
        throw std::runtime_error(
            "ReplicatedVShardHost::addVShard: recovered configuration is not authorized by Group 0");
    if (st.snapshot && st.snapshotFromPeer) {
        // A RECEIVED SNAPSHOT MUST BE RE-INSTALLED (review F2), and this is the case that
        // turned a fail-closed refusal into silent loss before it was caught.
        //
        // `RaftGroup::drainReady` persists AND FSYNCS the incoming Snapshot record BEFORE
        // `sm_.applySnapshot` writes the TSM files -- correct for Raft (the Ready contract
        // requires durability before anything observable) and it means the payload is
        // durable in the JOURNAL and nowhere else until the install finishes. A kill -9 in
        // that window leaves a replica whose log has been truncated to the boundary and
        // whose Engine holds only whichever files landed. Skipping the re-install there --
        // as the produced-snapshot reasoning below would have us do -- makes that replica
        // report itself caught up, serve replica reads out of a hole, and, if elected,
        // build and serve a SNAPSHOT out of that hole.
        //
        // Re-installing is idempotent: `TSMFileManager::addTSMFile` is keyed by the file's
        // rank (tier+seq, parsed from the name), so a file `Engine::init` already
        // registered is not registered twice, and `restoreVShardSnapshot` verifies before
        // it installs. So this is safe whether the crash landed before, during or after
        // the original install.
        timestar::http_log.info(
            "cluster: VShard {} recovered a RECEIVED snapshot at boundary index {} (term {}); re-installing its "
            "payload, because a crash between persisting the record and installing the files would otherwise leave "
            "this replica reporting itself caught up over a hole",
            vshard, st.snapshot->index, st.snapshot->term);
        co_await vs.sm->applySnapshot(*st.snapshot);
        // MOVE on the LAST use (debt D-32): `seedRecoveredSnapshot` takes its Snapshot by
        // value, so an lvalue duplicated the whole payload once per recovered group -- on
        // the startup path, where a shard opens ~1365 of them.
        node.seedRecoveredSnapshot(std::move(*st.snapshot));
    } else if (st.snapshot) {
        // RECOVERING A LOCALLY-PRODUCED SNAPSHOT (debt D-6). This used to throw outright --
        // which was correct while nothing ever compacted, and would have turned the FIRST
        // restart after D-6 into a fail-closed refusal to start.
        //
        // THE PAYLOAD IS NOT RE-INSTALLED, and that is deliberate rather than lazy. A
        // VShard snapshot is a manifest plus the bytes of the TSM files it references, and
        // `snapshotVShard` builds it FROM THIS NODE'S OWN on-disk files at a boundary that
        // by construction covers only FLUSHED data. Those files are still on this disk
        // after a restart, and `Engine::init` has already registered them -- so the local
        // state machine is already at or above the boundary and there is nothing to
        // restore. Re-installing would be actively WRONG, not merely wasteful:
        // `installVShardSnapshotFiles` calls `TSMFileManager::addTSMFile` for every file it
        // writes, so a file already registered by init would be registered TWICE and its
        // points counted twice by every query.
        //
        // What the core does still need is the snapshot as something it can SERVE: without
        // it a restarted leader has no payload for a follower below its boundary. That is
        // what seedRecoveredSnapshot does, and it deliberately does NOT surface the
        // snapshot to this node's own state machine.
        //
        // (The log suffix ABOVE the boundary replays as usual, which is what covers the
        // unflushed writes the boundary was chosen to exclude.)
        // Read what the log line needs BEFORE the move (debt D-32) -- see the received
        // branch above for why the move is worth having.
        const uint64_t snapIndex = st.snapshot->index;
        const uint64_t snapTerm = st.snapshot->term;
        auto stateMachineState = st.snapshot->file
                                     ? co_await data::decodeSnapshotStateMachineStateFile(st.snapshot->file->path)
                                     : data::decodeSnapshotStateMachineState(st.snapshot->data);
        if (!stateMachineState)
            throw std::runtime_error(
                "ReplicatedVShardHost: locally produced snapshot has invalid state-machine framing");
        vs.sm->restoreDeleteReceiptState(std::move(stateMachineState->deleteReceipts), snapIndex);
        vs.sm->restoreRetentionState(std::move(stateMachineState->retentionCutoff), snapIndex);
        node.seedRecoveredSnapshot(std::move(*st.snapshot));
        timestar::http_log.info(
            "cluster: VShard {} recovered from a compacted journal at boundary index {} (term {}); the local Engine "
            "already holds the flushed data the snapshot describes, so only the log suffix above it is replayed",
            vshard, snapIndex, snapTerm);
    }
    registry_.addGroup(vshard, std::move(node), *vs.persistence, *vs.sm);
    vshards_[vshard] = std::move(vs);
    co_return;
}

std::optional<seastar::gate::holder> ReplicatedVShardHost::holdVShardOperation(uint16_t vshard) {
    if (stopped_)
        return std::nullopt;
    auto state = vshards_.find(vshard);
    if (state == vshards_.end() || state->second.retiring || !state->second.operationGate ||
        state->second.operationGate->is_closed())
        return std::nullopt;
    return state->second.operationGate->hold();
}

seastar::future<bool> ReplicatedVShardHost::retireVShard(uint16_t vshard, uint64_t mapEpoch) {
    co_return co_await retireVShard(vshard, mapEpoch, RetirementAuthority::AppliedRaftConfiguration);
}

seastar::future<bool> ReplicatedVShardHost::retireVShard(uint16_t vshard, uint64_t mapEpoch,
                                                         RetirementAuthority authority) {
    if (mapEpoch == 0)
        throw std::invalid_argument("ReplicatedVShardHost::retireVShard: map epoch must be non-zero");
    if (sharedJournalEnabled())
        throw std::runtime_error(
            "cluster: online VShard retirement requires private v1 journals; shared journals are not supported");
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    auto state = vshards_.find(vshard);
    if (state == vshards_.end())
        co_return false;

    auto groupLifetime = registry_.groupHandle(vshard);
    if (auto* group = groupLifetime.get()) {
        const auto& config = group->node().config();
        const auto containsSelf = [this](const std::vector<NodeId>& nodes) {
            return std::find(nodes.begin(), nodes.end(), self_) != nodes.end();
        };
        const bool locallyStillMember =
            containsSelf(config.voters) || containsSelf(config.votersOutgoing) || containsSelf(config.learners);
        if (locallyStillMember && authority != RetirementAuthority::CommittedServingMap)
            throw std::runtime_error("cluster: refusing to retire VShard " + std::to_string(vshard) +
                                     " before this node is absent from its applied Raft configuration");
        if (locallyStillMember)
            timestar::http_log.info(
                "cluster: retiring VShard {} from committed serving-map epoch {}; the removed member did not "
                "receive final Cnew, but Group 0 committed the exact completed movement proof",
                vshard, mapEpoch);
    } else if (!state->second.retiring || state->second.retirementEpoch == 0) {
        throw std::runtime_error("cluster: refusing to retire VShard " + std::to_string(vshard) +
                                 " without its applied Raft configuration");
    }

    state->second.retiring = true;
    if (state->second.retirementEpoch == 0)
        state->second.retirementEpoch = mapEpoch;
    const uint64_t retirementEpoch = state->second.retirementEpoch;
    // Retire Raft first: it fails every pending apply/read/config waiter and
    // drains group work. Those failures let host wrapper operations release the
    // VShard gate; closing that gate first could wait forever on an unbounded
    // read barrier whose cancellation was still queued behind it.
    (void)co_await registry_.removeGroup(vshard);
    if (state->second.operationGate && !state->second.operationGate->is_closed())
        co_await state->second.operationGate->close();
    groupLifetime.reset();

    // No apply can now reach the Engine. Replace this replica's storage with a
    // durable empty generation before quarantining its journal: if storage
    // cleanup fails, the still-active journal directory makes startup retry the
    // same idempotent cleanup before it can finish retirement.
    co_await store_.retireVShardData(VShardId{vshard});

    // With no live replica, every journal record in this generation is dead.
    // Publish the terminal floor before moving the files so observability and
    // the shared GC rule cannot mistake a retired generation for a laggard.
    retention_.setReleased(VShardId{vshard}, std::numeric_limits<uint64_t>::max());
    if (state->second.writer)
        co_await state->second.writer->close();

    const fs::path source = journalRoot_ / ("vshard_" + std::to_string(vshard));
    const fs::path retiredRoot = journalRoot_ / "retired";
    const fs::path destination =
        retiredRoot / ("v1_vshard_" + std::to_string(vshard) + "_epoch_" + std::to_string(retirementEpoch));
    co_await seastar::async([&] {
        fs::create_directories(retiredRoot);
        const bool sourceExists = fs::exists(source);
        const bool destinationExists = fs::exists(destination);
        if (sourceExists && destinationExists)
            throw std::runtime_error("cluster: both active and retired VShard journal generations exist");
        if (sourceExists)
            fs::rename(source, destination);
        else if (!destinationExists)
            throw std::runtime_error("cluster: VShard journal disappeared before it could be quarantined");
    });
    co_await seastar::sync_directory(journalRoot_.string());
    co_await seastar::sync_directory(retiredRoot.string());

    if (retirementCheckpointHook_)
        retirementCheckpointHook_(vshard, ReplicaRetirementCheckpoint::JournalQuarantined);

    co_await ensureRetiredJournalMarker(destination);

    vshards_.erase(state);
    ++replicasRetired_;
    timestar::http_log.info("cluster: retired VShard {} at serving-map epoch {}; its v1 journal is quarantined at {}",
                            vshard, retirementEpoch, destination.string());
    co_return true;
}

seastar::future<size_t> ReplicatedVShardHost::reconcileCommittedServingMap(const control::ControlMap& map) {
    if (!control::isCompleteControlMap(map))
        throw std::invalid_argument("cluster: replica retirement requires a complete committed serving map");
    std::vector<uint16_t> retired;
    retired.reserve(vshards_.size());
    for (auto& [vshard, state] : vshards_) {
        const auto placement = map.placement.find(vshard);
        const bool placedHere =
            placement != map.placement.end() &&
            std::find(placement->second.begin(), placement->second.end(), self_) != placement->second.end();
        if (placedHere) {
            state.seenInServingMap = true;
        } else if (state.seenInServingMap) {
            // A prior retirement attempt may have drained the replica and moved
            // its journal before failing to persist the marker. Replay must run
            // the idempotent tail again instead of stranding retiring state.
            retired.push_back(vshard);
        }
    }
    size_t count = 0;
    for (uint16_t vshard : retired)
        if (co_await retireVShard(vshard, map.epoch, RetirementAuthority::CommittedServingMap))
            ++count;
    co_return count;
}

seastar::future<> ReplicatedVShardHost::ensureRetiredJournalMarker(const fs::path& directory) {
    const auto marker = co_await seastar::async([directory] {
        std::optional<fs::path> found;
        for (const auto& entry : fs::directory_iterator(directory)) {
            const auto parsed = retiredAtMillis(entry.path().filename().string());
            if (!parsed)
                continue;
            if (entry.is_symlink() || !entry.is_regular_file() || found)
                throw std::runtime_error("cluster: retired VShard journal has an invalid or ambiguous v1 marker");
            found = entry.path();
        }
        return found;
    });
    fs::path path;
    seastar::open_flags flags = seastar::open_flags::rw;
    if (marker) {
        path = *marker;
    } else {
        const auto now = std::chrono::system_clock::now().time_since_epoch();
        const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        if (millis < 0)
            throw std::runtime_error("cluster: system clock predates the v1 retirement marker epoch");
        path = directory / ("retired_at_" + std::to_string(millis) + ".v1");
        flags |= seastar::open_flags::create | seastar::open_flags::exclusive;
    }
    // Re-flush an existing marker too. This makes an exact retry repair a prior
    // file/directory sync failure instead of trusting mere namespace presence.
    auto file = co_await seastar::open_file_dma(path.string(), flags);
    co_await file.flush();
    co_await file.close();
    co_await seastar::sync_directory(directory.string());
}

seastar::future<size_t> ReplicatedVShardHost::recoverReplicaRetirements(control::ControlMap map) {
    if (!control::isCompleteControlMap(map))
        throw std::invalid_argument("cluster: replica-retirement recovery requires a complete serving map");
    if (sharedJournalEnabled())
        throw std::runtime_error("cluster: replica-retirement recovery requires private v1 VShard journals");
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    const fs::path retiredRoot = journalRoot_ / "retired";
    auto discovered = co_await seastar::async([this, &map, retiredRoot] {
        std::vector<fs::path> retired;
        std::vector<std::tuple<fs::path, fs::path, uint16_t>> stale;
        fs::create_directories(retiredRoot);
        for (const auto& entry : fs::directory_iterator(journalRoot_)) {
            if (entry.is_symlink() || !entry.is_directory())
                continue;
            const auto vshard = activeVShardDirectoryName(entry.path().filename().string());
            if (!vshard)
                continue;
            const auto placement = map.placement.find(*vshard);
            const bool placedHere =
                placement != map.placement.end() &&
                std::find(placement->second.begin(), placement->second.end(), self_) != placement->second.end();
            if (!placedHere) {
                const fs::path destination =
                    retiredRoot / ("v1_vshard_" + std::to_string(*vshard) + "_epoch_" + std::to_string(map.epoch));
                if (fs::exists(destination))
                    throw std::runtime_error(
                        "cluster: active and retired VShard journal generations collide during recovery");
                stale.emplace_back(entry.path(), std::move(destination), *vshard);
            }
        }
        for (const auto& entry : fs::directory_iterator(retiredRoot)) {
            if (!entry.is_symlink() && entry.is_directory() && retiredDirectoryName(entry.path().filename().string()))
                retired.push_back(entry.path());
        }
        return std::pair{std::move(stale), std::move(retired)};
    });

    // The active directory is the durable retry token for a crash-interrupted
    // retirement. Do not rename it until the Engine's empty generation is
    // durable; a crash at any earlier storage checkpoint leaves this exact work
    // discoverable on the next startup.
    for (const auto& [source, destination, vshard] : discovered.first) {
        (void)source;
        (void)destination;
        co_await store_.retireVShardData(VShardId{vshard});
    }
    co_await seastar::async([&discovered] {
        for (const auto& [source, destination, vshard] : discovered.first) {
            (void)vshard;
            fs::rename(source, destination);
            discovered.second.push_back(destination);
        }
    });
    co_await seastar::sync_directory(journalRoot_.string());
    co_await seastar::sync_directory(retiredRoot.string());
    for (const auto& directory : discovered.second)
        co_await ensureRetiredJournalMarker(directory);
    replicasRetired_ += discovered.first.size();
    co_return discovered.first.size();
}

seastar::future<size_t> ReplicatedVShardHost::reclaimRetiredJournals(std::chrono::system_clock::time_point now) {
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    const fs::path retiredRoot = journalRoot_ / "retired";
    const uint64_t nowMillis =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
    const uint64_t graceMillis =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(kRetiredJournalGrace).count());
    auto expired = co_await seastar::async([retiredRoot, nowMillis, graceMillis] {
        std::vector<fs::path> paths;
        if (!fs::exists(retiredRoot))
            return paths;
        for (const auto& entry : fs::directory_iterator(retiredRoot)) {
            if (entry.is_symlink() || !entry.is_directory() || !retiredDirectoryName(entry.path().filename().string()))
                continue;
            std::optional<uint64_t> retiredAt;
            bool ambiguous = false;
            for (const auto& child : fs::directory_iterator(entry.path())) {
                const auto parsed = retiredAtMillis(child.path().filename().string());
                if (!parsed)
                    continue;
                if (child.is_symlink() || !child.is_regular_file() || retiredAt) {
                    ambiguous = true;
                    break;
                }
                retiredAt = parsed;
            }
            if (!ambiguous && retiredAt && nowMillis >= *retiredAt && nowMillis - *retiredAt >= graceMillis)
                paths.push_back(entry.path());
        }
        return paths;
    });

    size_t reclaimed = 0;
    for (const auto& path : expired) {
        co_await seastar::async([path] { fs::remove_all(path); });
        co_await seastar::sync_directory(retiredRoot.string());
        ++reclaimed;
    }
    retiredJournalsReclaimed_ += reclaimed;
    co_return reclaimed;
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, data::ReplicatedCommand cmd) {
    return propose(vshard, std::move(cmd), std::nullopt);
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, data::ReplicatedCommand cmd,
                                                    data::OptDeadline deadline) {
    auto operation = holdVShardOperation(vshard);
    if (!operation)
        throw std::runtime_error("ReplicatedVShardHost::propose: VShard not hosted here");
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::propose: VShard not hosted here");
    deadline = boundedProposalDeadline(deadline);
    if (const auto* writes = std::get_if<data::WriteBatch>(&cmd))
        co_await store_.checkWriteAdmission(*writes);
    if (const auto* batch = std::get_if<data::DeleteRangeBatch>(&cmd)) {
        auto state = vshards_.find(vshard);
        if (state == vshards_.end() || !state->second.sm || !state->second.deleteProposalLock)
            throw std::runtime_error("ReplicatedVShardHost::propose: VShard state is unavailable");
        auto units = co_await seastar::get_units(*state->second.deleteProposalLock, 1);
        state->second.sm->checkDeleteAdmission(*batch);
        const bool committed = co_await g->proposeAndAwaitApplied(data::encodeReplicatedCommand(cmd), deadline);
        if (!committed)
            co_return false;
        switch (state->second.sm->deleteReceiptStatus(*batch)) {
            case EngineDataStateMachine::DeleteReceiptStatus::Retained:
                co_return true;
            case EngineDataStateMachine::DeleteReceiptStatus::Expired:
                throw data::DeleteReceiptExpiredError(
                    "delete idempotency receipt expired before the committed command applied");
            case EngineDataStateMachine::DeleteReceiptStatus::Missing:
                throw std::runtime_error(
                    "ReplicatedVShardHost::propose: committed delete has no receipt or retired-floor outcome");
        }
        throw std::runtime_error("ReplicatedVShardHost::propose: invalid delete receipt status");
    }
    co_return co_await g->proposeAndAwaitApplied(data::encodeReplicatedCommand(cmd), deadline);
}

seastar::future<data::ProposeOutcome> ReplicatedVShardHost::proposeCommandHinted(uint16_t vshard,
                                                                                 data::ReplicatedCommand cmd,
                                                                                 data::OptDeadline deadline) {
    data::ProposeOutcome out;
    auto operation = holdVShardOperation(vshard);
    raft::RaftGroup* g = registry_.group(vshard);
    if (!operation || !g) {
        out.rejects.push_back(data::SliceReject{vshard, raft::kNoNode, data::WriteFailure::NotLeader});
        co_return out;
    }
    try {
        if (co_await propose(vshard, std::move(cmd), deadline)) {
            out.committed = true;
            out.committedVShards.push_back(vshard);
        } else {
            out.rejects.push_back(classifyRefusal(vshard));
        }
    } catch (...) {
        const auto kind = data::classifyLocalWriteFailure(std::current_exception());
        // Expiry is terminal to the caller but is still an expected protocol
        // outcome. Encode it so a remote coordinator can return the same 409 as
        // a local one; rethrow genuinely fatal terminal failures.
        if (!data::isRetryableWriteFailure(kind) && kind != data::WriteFailure::Expired)
            throw;
        NodeId hint = g->leader();
        if (hint == self_)
            hint = raft::kNoNode;
        out.rejects.push_back(data::SliceReject{vshard, hint, kind});
    }
    co_return out;
}

seastar::future<uint64_t> ReplicatedVShardHost::snapshotVShard(uint16_t vshard) {
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    co_return co_await snapshotVShardLocked(vshard);
}

seastar::future<uint64_t> ReplicatedVShardHost::snapshotVShardLocked(uint16_t vshard) {
    auto operation = holdVShardOperation(vshard);
    if (!operation)
        co_return 0;
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return 0;  // not hosted here
    // REFUSE WHILE ANY TARGET-BEARING ROLLED STORE IS STILL AWAITING
    // CONVERSION TO TSM, and capture the active store's first surviving
    // revision in the same reactor turn.
    //
    // This is the FIRST of two conditions that make the boundary below safe, and it is the
    // one the snapshot-durability gate caught: WAL->TSM conversions run 6 at a time and so
    // complete OUT OF ORDER, which means "the highest revision in TSM" can sit ABOVE an
    // unconverted store's revisions. Truncating there discards log entries whose data lives
    // only in RAM -- measured as 7 of 200 acknowledged points lost across a kill -9, with
    // the three nodes disagreeing about how many (they had converted different stores).
    // Full reasoning on EngineLocalStore::hasUnconvertedStores.
    //
    // Skipping is free: the sweep comes back every few seconds and conversions finish in
    // seconds. A larger log is a cost; a truncated log is data loss.
    const uint64_t appliedAtStorageObservation = g->appliedIndex();
    const auto flushState = co_await store_.vshardFlushState(VShardId{vshard});
    if (flushState.pendingConversion) {
        ++snapshotsSkippedPendingConversion_;
        co_return 0;
    }

    // The active store is the only possible unflushed suffix now. Its oldest
    // surviving revision is therefore an exact fence: everything below it is
    // either materialised in TSM or has been durably deleted. With no surviving
    // target point, storage represents the whole applied prefix observed above.
    uint64_t storageBoundary = appliedAtStorageObservation;
    if (flushState.oldestUnflushedRevision) {
        if (*flushState.oldestUnflushedRevision == 0)
            throw std::runtime_error("cluster: active VShard data has a zero replicated revision");
        storageBoundary = std::min<uint64_t>(storageBoundary, *flushState.oldestUnflushedRevision - 1);
    }

    // Capture the resolved TSM view. Concurrent later writes are harmless: a
    // newly converted extent can raise maxFlushedRevision, for which the
    // conservative one-entry slack below remains safe, while an active write
    // stays above the observed storage boundary and remains in the suffix.
    auto payload = co_await store_.buildVShardSnapshotFile(VShardId{vshard});
    const uint64_t maxFlushedRevision = payload.manifest.snapshotRevision;

    // The TSM-derived fallback remains one entry below its highest revision.
    // Found while wiring the trigger (D-6), and a real hole rather than
    // belt-and-braces.
    //
    // `snapshotRevision` is the MAXIMUM revision appearing in the flushed extents, and a
    // revision is one whole log ENTRY -- but an entry's points do not all flush together.
    // `EngineLocalStore::applyWrites` buckets a batch's series by core and issues several
    // `insertBatch` calls, any of which can trigger a memory-store rollover: so entry N can
    // legitimately have SOME of its points in TSM and the rest still in the memory store,
    // and the manifest then reports `snapshotRevision == N` for a partially-flushed entry.
    // Truncating AT N discards the log entry holding the unflushed remainder, and those
    // points are gone on the next restart -- a silent data loss on the one invariant this
    // whole path exists to protect.
    //
    // Backing off by one entry closes it for the cost of retaining one entry: entry N stays
    // in the log and replays, and re-applying a fully-flushed entry is idempotent under LWW
    // (identical revision re-stamp, ADR 0003).
    const uint64_t tsmBoundary = maxFlushedRevision == 0 ? 0 : maxFlushedRevision - 1;
    uint64_t upto = std::max(storageBoundary, tsmBoundary);
    // And never above what this replica has APPLIED. RaftNode::compact clamps to
    // lastApplied_ itself, but clamping here too means the value we log and RETURN is the
    // one that was really used -- a caller that trusts the return value to mean "the log
    // below this is gone" would otherwise be wrong on a lagging replica.
    upto = std::min<uint64_t>(upto, g->appliedIndex());

    // Receipt retirement destroys historical dedupe state. If its entry sits
    // above the safe storage boundary, keeping the log is necessary but waiting
    // forever is not: rotate the active store which holds the blocking point.
    // The normal bounded conversion path publishes it; a later sweep then sees
    // the next active suffix and advances. The conditional rollover re-checks
    // the target after acquiring the shard-wide rollover lock, avoiding empty
    // rotations when another writer already moved it.
    if (auto state = vshards_.find(vshard); state != vshards_.end() && state->second.sm &&
                                            (!state->second.sm->canSnapshotDeleteReceiptStateThrough(upto) ||
                                             !state->second.sm->canSnapshotRetentionStateThrough(upto))) {
        ++snapshotsSkippedDeleteState_;
        if (flushState.oldestUnflushedRevision)
            (void)co_await store_.forceSnapshotRollover(VShardId{vshard});
        co_return 0;
    }
    if (upto == 0) {
        ++snapshotsSkippedUnflushed_;
        co_return 0;
    }
    // NOTHING TO GAIN, AND SOMETHING TO LOSE, FROM RE-SNAPSHOTTING THE SAME BOUNDARY
    // (review F3). A group whose flush watermark has not moved since its last snapshot
    // computes the SAME `upto` on every sweep. Compacting again truncates nothing, but it
    // does REPLACE `snapshot_.data` in place -- which invalidates any in-flight
    // InstallSnapshot transfer, and at an unchanged (index, term) it does so without
    // tripping the reply path's "our snapshot moved on" guard. `RaftNode::compact` now
    // drops live transfers defensively, but the transfer should not be disturbed at all
    // for a boundary that is not advancing.
    if (upto <= static_cast<uint64_t>(g->node().log().snapshotIndex())) {
        ++snapshotsSkippedNoAdvance_;
        co_return 0;
    }
    // Delete idempotency is replicated state just like the data. Include only
    // receipts covered by this exact boundary; suffix receipts must be rebuilt
    // by suffix replay or their deletes could be skipped after restore.
    if (auto state = vshards_.find(vshard); state != vshards_.end() && state->second.sm) {
        auto deleteState = state->second.sm->deleteReceiptStateThrough(upto);
        payload.deleteReceiptsRetiredBeforeMs = deleteState.retiredBeforeMs;
        payload.deleteReceiptsRetiredAtIndex = deleteState.retiredAtIndex;
        payload.deleteReceipts = std::move(deleteState.receipts);
        payload.retentionCutoff = state->second.sm->retentionStateThrough(upto);
    }
    // Bind the payload to the exact Raft boundary. The storage builder emits the
    // minimal data fence (highest extent revision); the host may safely promote
    // it across entries whose effects are already represented by the resolved
    // storage state. The receiver requires this fence to equal snap.index + 1.
    if (upto == UINT64_MAX)
        throw std::overflow_error("cluster: Raft snapshot boundary exhausted");
    payload.manifest.snapshotRevision = upto + 1;
    // Encode exact-v1 TSP1 directly from the Engine's immutable objects into a
    // sidecar. Both directions copy through fixed-size buffers: the producer,
    // the Raft leader and the receiver no longer hold a whole VShard snapshot
    // in reactor memory. The file-backed bound protects disk from a corrupt or
    // hostile descriptor; unlike the former 128-MiB RAM ceiling, it does not
    // strand a healthy large VShard behind an ever-growing uncompacted log.
    const auto snapshotDirectory = snapshotDirectoryFor(journalRoot_, vshard, sharedSink_ != nullptr);
    const auto output =
        snapshotDirectory / ("snapshot_v1_produce_g" + std::to_string(vshard) + "_i" + std::to_string(upto) + ".bin");
    auto encoded = co_await data::encodeSnapshotPayloadFile(std::move(payload), output);
    if (encoded.size > raft::kMaxVShardSnapshotFileBytes) {
        timestar::http_log.error(
            "cluster: NOT compacting VShard {}: its snapshot is {} bytes, over the {} byte file-backed snapshot "
            "bound. The log is kept instead.",
            vshard, encoded.size, raft::kMaxVShardSnapshotFileBytes);
        ++snapshotsRefusedTooLarge_;
        co_return 0;
    }
    auto snapshotFile = std::make_shared<raft::SnapshotFile>();
    snapshotFile->path = encoded.path;
    snapshotFile->size = encoded.size;
    snapshotFile->hash = encoded.hash;
    snapshotFile->removeOnDestroy = true;
    encoded.release();
    co_await g->compact(upto, std::move(snapshotFile));
    ++snapshotsTaken_;
    if (auto it = vshards_.find(vshard); it != vshards_.end()) {
        it->second.lastSnapshot = seastar::lowres_clock::now();
        if (it->second.sm)
            it->second.sm->noteSnapshotTaken();  // the bytes below the new boundary are gone
    }
    co_return upto;
}

seastar::future<std::optional<ReplicatedVShardHost::BackupSnapshotCapture>> ReplicatedVShardHost::captureVShardBackup(
    uint16_t vshard, std::chrono::milliseconds budget) {
    if (budget <= std::chrono::milliseconds::zero())
        throw std::invalid_argument("cluster backup capture budget must be positive");
    const auto deadline = seastar::lowres_clock::now() + budget;
    auto maintenance = co_await seastar::get_units(
        maintenanceLock_, 1, std::chrono::duration_cast<std::chrono::steady_clock::duration>(budget));
    if (stopped_)
        throw std::runtime_error("cluster backup capture stopped with its VShard host");
    auto operation = holdVShardOperation(vshard);
    if (!operation)
        co_return std::nullopt;
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return std::nullopt;

    // readBarrier both proves current leadership to a quorum and applies the
    // returned index locally. Everything below `target` is therefore a valid
    // export prefix even if leadership changes after this point.
    const raft::LogIndex target = co_await g->readBarrier(deadline);
    bool forcedRollover = false;
    while (true) {
        if (stopped_)
            throw std::runtime_error("cluster backup capture stopped with its VShard host");
        const auto& current = g->node().servableSnapshot();
        if (current.index >= target) {
            if (!current.fileBacked() || !current.file || current.file->path.empty())
                throw std::runtime_error("cluster backup capture reached a non-file-backed VShard snapshot");
            BackupSnapshotCapture capture;
            capture.vshard = vshard;
            capture.readIndex = target;
            capture.snapshotIndex = current.index;
            capture.snapshotTerm = current.term;
            capture.file = raft::PinnedSnapshotFile(current.file);
            co_return std::optional<BackupSnapshotCapture>(std::move(capture));
        }
        if (seastar::lowres_clock::now() >= deadline)
            throw seastar::timed_out_error{};

        (void)co_await snapshotVShardLocked(vshard);
        if (seastar::lowres_clock::now() >= deadline)
            throw seastar::timed_out_error{};
        if (g->node().servableSnapshot().index >= target)
            continue;

        // One conditional rollover is sufficient: it moves every active point
        // at or below the sampled ReadIndex into the bounded conversion path.
        // Writes arriving later remain a suffix and do not need to be chased.
        if (!forcedRollover) {
            (void)co_await store_.forceSnapshotRollover(VShardId{vshard});
            forcedRollover = true;
        }
        const auto remaining = deadline - seastar::lowres_clock::now();
        if (remaining <= seastar::lowres_clock::duration::zero())
            throw seastar::timed_out_error{};
        const auto poll = std::chrono::duration_cast<seastar::lowres_clock::duration>(std::chrono::milliseconds(50));
        co_await seastar::sleep(std::min(remaining, poll));
    }
}

void ReplicatedVShardHost::expireBackupCaptureSessions() {
    backupCaptureTimer_.cancel();
    const auto now = seastar::lowres_clock::now();
    for (auto it = backupCaptureSessions_.begin(); it != backupCaptureSessions_.end();) {
        if (it->second.expires <= now)
            it = backupCaptureSessions_.erase(it);
        else
            ++it;
    }
    if (backupCaptureSessions_.empty() || stopped_)
        return;
    auto earliest = backupCaptureSessions_.begin()->second.expires;
    for (const auto& [key, session] : backupCaptureSessions_)
        earliest = std::min(earliest, session.expires);
    backupCaptureTimer_.arm(earliest);
}

seastar::future<data::BackupCaptureDescriptor> ReplicatedVShardHost::beginBackupCapture(std::string operationId,
                                                                                        std::string clusterUuid,
                                                                                        uint16_t vshard) {
    auto held = backupCaptureGate_.hold();
    if (stopped_)
        throw std::runtime_error("cluster backup capture stopped with its VShard host");
    if (!VShardId{vshard}.valid() || !canonicalBackupId(operationId) || !canonicalBackupId(clusterUuid) ||
        parseUuidBytes(clusterUuid) != journalIdentity_.clusterUuid)
        throw std::invalid_argument("cluster backup capture identity does not match this node");

    expireBackupCaptureSessions();
    BackupCaptureKey key{operationId, vshard};
    if (auto existing = backupCaptureSessions_.find(key); existing != backupCaptureSessions_.end()) {
        existing->second.expires = seastar::lowres_clock::now() + kBackupCaptureSessionTtl;
        const auto& capture = existing->second.capture;
        data::BackupCaptureDescriptor descriptor{data::BackupCaptureStatus::Captured,
                                                 self_,
                                                 capture.vshard,
                                                 capture.readIndex,
                                                 capture.snapshotIndex,
                                                 capture.snapshotTerm,
                                                 capture.file->size,
                                                 capture.file->hash};
        expireBackupCaptureSessions();
        co_return descriptor;
    }
    if (backupCaptureSessions_.size() >= kMaxBackupCaptureSessions)
        throw std::runtime_error("cluster backup capture session capacity exhausted");

    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return data::BackupCaptureDescriptor{data::BackupCaptureStatus::Unavailable, raft::kNoNode, vshard};
    if (!g->isLeader())
        co_return data::BackupCaptureDescriptor{data::BackupCaptureStatus::Redirect, g->leader(), vshard};

    auto captured = co_await captureVShardBackup(vshard);
    if (!captured)
        co_return data::BackupCaptureDescriptor{data::BackupCaptureStatus::Unavailable, raft::kNoNode, vshard};
    if (stopped_)
        throw std::runtime_error("cluster backup capture stopped with its VShard host");

    // A duplicate begin can wait behind the host's maintenance semaphore while the
    // first request creates the durable snapshot. Recheck after the await so retries
    // return the first immutable capture instead of replacing its session.
    expireBackupCaptureSessions();
    if (auto existing = backupCaptureSessions_.find(key); existing != backupCaptureSessions_.end()) {
        existing->second.expires = seastar::lowres_clock::now() + kBackupCaptureSessionTtl;
        const auto& first = existing->second.capture;
        data::BackupCaptureDescriptor descriptor{data::BackupCaptureStatus::Captured,
                                                 self_,
                                                 first.vshard,
                                                 first.readIndex,
                                                 first.snapshotIndex,
                                                 first.snapshotTerm,
                                                 first.file->size,
                                                 first.file->hash};
        expireBackupCaptureSessions();
        co_return descriptor;
    }
    if (backupCaptureSessions_.size() >= kMaxBackupCaptureSessions)
        throw std::runtime_error("cluster backup capture session capacity exhausted");

    data::BackupCaptureDescriptor descriptor{data::BackupCaptureStatus::Captured,
                                             self_,
                                             captured->vshard,
                                             captured->readIndex,
                                             captured->snapshotIndex,
                                             captured->snapshotTerm,
                                             captured->file->size,
                                             captured->file->hash};
    backupCaptureSessions_.emplace(
        std::move(key),
        BackupCaptureSession{std::move(*captured), seastar::lowres_clock::now() + kBackupCaptureSessionTtl});
    expireBackupCaptureSessions();
    co_return descriptor;
}

seastar::future<data::BackupCaptureChunk> ReplicatedVShardHost::readBackupCapture(std::string operationId,
                                                                                  std::string clusterUuid,
                                                                                  uint16_t vshard, uint64_t offset,
                                                                                  uint32_t maximumBytes) {
    auto held = backupCaptureGate_.hold();
    if (stopped_)
        throw std::runtime_error("cluster backup capture stopped with its VShard host");
    if (!VShardId{vshard}.valid() || !canonicalBackupId(operationId) || !canonicalBackupId(clusterUuid) ||
        parseUuidBytes(clusterUuid) != journalIdentity_.clusterUuid)
        throw std::invalid_argument("cluster backup capture identity does not match this node");
    if (maximumBytes == 0 || maximumBytes > kMaxBackupCaptureChunkBytes)
        throw std::invalid_argument("cluster backup capture chunk bound is invalid");

    expireBackupCaptureSessions();
    auto found = backupCaptureSessions_.find(BackupCaptureKey{operationId, vshard});
    if (found == backupCaptureSessions_.end())
        throw std::runtime_error("cluster backup capture session is absent or expired");
    if (offset > found->second.capture.file->size)
        throw std::out_of_range("cluster backup capture offset exceeds the snapshot size");

    const uint64_t totalSize = found->second.capture.file->size;
    const uint64_t totalHash = found->second.capture.file->hash;
    auto pin = found->second.capture.file.pinAgain();
    found->second.expires = seastar::lowres_clock::now() + kBackupCaptureSessionTtl;
    expireBackupCaptureSessions();
    auto bytes = co_await seastar::async([pin = std::move(pin), offset, maximumBytes]() mutable {
        return readPinnedBackupChunk(std::move(pin), offset, maximumBytes);
    });
    co_return data::BackupCaptureChunk{vshard, offset, totalSize, totalHash, std::move(bytes)};
}

seastar::future<> ReplicatedVShardHost::finishBackupCapture(std::string operationId, std::string clusterUuid,
                                                            uint16_t vshard) {
    auto held = backupCaptureGate_.hold();
    if (!VShardId{vshard}.valid() || !canonicalBackupId(operationId) || !canonicalBackupId(clusterUuid) ||
        parseUuidBytes(clusterUuid) != journalIdentity_.clusterUuid)
        throw std::invalid_argument("cluster backup capture identity does not match this node");
    backupCaptureSessions_.erase(BackupCaptureKey{std::move(operationId), vshard});
    expireBackupCaptureSessions();
    co_return;
}

// ---------------------------------------------------------------------------
// Journal segment reclamation (debt D-34)
// ---------------------------------------------------------------------------

size_t ReplicatedVShardHost::publishReclaimFloors() {
    size_t advanced = 0;
    for (const auto& [vs, state] : vshards_) {
        if (!state.persistence)
            continue;
        const uint64_t floor = state.persistence->releasedSeq();
        // JournalRetention::setReleased is monotonic on its own, but comparing here is
        // what tells the caller whether anything CHANGED -- which is what decides
        // whether a collect pass has any work to do at all.
        if (floor > retention_.released(VShardId{vs})) {
            retention_.setReleased(VShardId{vs}, floor);
            ++advanced;
        }
    }
    return advanced;
}

seastar::future<size_t> ReplicatedVShardHost::reclaimJournalSegments() {
    // One pass at a time: a pass suspends over file I/O, and two concurrent passes over
    // the same directory would both plan against the same segment and race the unlink.
    if (stopped_ || journalGcRunning_ || snapshotGate_.is_closed())
        co_return 0;
    auto maintenance = co_await seastar::get_units(maintenanceLock_, 1);
    // UNDER THE SAME GATE AS THE SWEEP. This is a public entry (tests, and an operator
    // action later), so it can be called from outside snapshotSweep(); without the hold, a
    // pass in flight when stop() runs would keep unlinking against writers that stop() is
    // closing. stop() closes this gate before it touches the writers.
    auto held = snapshotGate_.hold();
    journalGcRunning_ = true;
    size_t deleted = 0;
    size_t pinned = 0;  // census for THIS pass; see journalSegmentsPinnedLastPass()
    std::exception_ptr err;
    try {
        publishReclaimFloors();
        ++journalGcPasses_;
        if (sharedSink_ && sharedWriter_) {
            // SHARED LAYOUT (debt D-10). ONE directory holding every group on this
            // reactor, so a segment is reclaimable only once EVERY group's floor has
            // passed every record in it -- planSegment's rule verbatim -- and a single
            // laggard would otherwise pin the whole 64 MiB segment for ~1365 groups.
            // Copy-forward is therefore ON, and it runs inside runExclusive() because
            // it APPENDS to the same writer every group is group-committing through.
            const fs::path dir = journalRoot_ / ("shard_" + std::to_string(seastar::this_shard_id()));
            const uint64_t active = sharedWriter_->currentSegmentNumber();
            // The sink is handed IN rather than wrapped around the whole collect: reading
            // and scanning each sealed segment is the expensive part and needs no lock (a
            // sealed segment is immutable), so only the bounded copy-forward write runs
            // inside runExclusive. Wrapping the whole call would hold every group on the
            // reactor off its group-commit for the length of a directory walk.
            auto result =
                co_await JournalGc::collect(dir, active, *sharedWriter_, retention_,
                                            JournalGc::Options{.continuePastPinned = true}, sharedSink_.get());
            deleted += result.deletedSegments.size();
            pinned += result.pinnedSegments.size();
            journalRecordsCopiedForward_ += result.copiedRecords;
        } else {
            // PER-VSHARD LAYOUT (the default). Each directory holds exactly one group's
            // records, so its own floor decides everything and DELETE-ONLY is both
            // sufficient and the reason no exclusion is needed: nothing here touches the
            // writer, and a sealed segment strictly below the active one is not
            // something the writer will ever touch either.
            //
            // Visit only the groups whose floor MOVED. Re-scanning ~1365 directories a
            // minute to re-derive an unchanged answer is the difference between a
            // background task and a background problem.
            for (auto& [vs, state] : vshards_) {
                if (stopped_)
                    break;
                if (!state.writer || !state.persistence)
                    continue;
                const uint64_t floor = retention_.released(VShardId{vs});
                if (floor == 0 || floor <= state.lastGcFloor)
                    continue;
                const fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vs));
                auto result = co_await JournalGc::collect(dir, state.writer->currentSegmentNumber(), *state.writer,
                                                          retention_, JournalGc::Options{.copyForward = false});
                // Recorded AFTER the pass: a throw leaves it unchanged so the next pass
                // retries rather than skipping a directory it never finished.
                state.lastGcFloor = floor;
                deleted += result.deletedSegments.size();
                pinned += result.pinnedSegments.size();
            }
        }
    } catch (...) {
        err = std::current_exception();
    }
    journalGcRunning_ = false;
    journalSegmentsDeleted_ += deleted;
    journalSegmentsPinnedLastPass_ = pinned;  // a gauge: replaced, never accumulated
    if (err)
        std::rethrow_exception(err);
    co_return deleted;
}

// ---------------------------------------------------------------------------
// The snapshot producer trigger (debt D-6)
// ---------------------------------------------------------------------------

void ReplicatedVShardHost::startSnapshotTrigger() {
    if (snapshotTriggerEnabled_ || stopped_)
        return;
    // (a) THE COHESION GATE, checked ONCE here rather than 1365 times in the sweep.
    // `buildVShardSnapshot` throws on a non-cohesive core count -- a single-core snapshot
    // would omit the series that scatter across cores, i.e. ship a PARTIAL snapshot -- and
    // that is a property of the node, not of a group, so it can never become true later.
    // Disabling the whole trigger with ONE warning is the difference between a legible
    // startup line and a permanent error every five seconds.
    if (!timestar::vshardsCohesiveOnCores(seastar::smp::count)) {
        timestar::http_log.warn(
            "cluster: the Raft snapshot trigger is DISABLED on this node: {} cores is not VShard-cohesive, so a "
            "single-core snapshot would omit series that scatter across cores. Raft logs will grow without bound and "
            "restart replay will lengthen accordingly (debt D-6).",
            seastar::smp::count);
        return;
    }
    snapshotTriggerEnabled_ = true;
    // JITTER the first fire so shards do not sweep in lockstep (they share the disk).
    snapshotCursor_ = seastar::this_shard_id() * 397;
    snapshotTimer_.set_callback([this] {
        if (stopped_ || snapshotSweepRunning_ || snapshotGate_.is_closed())
            return;
        snapshotSweepRunning_ = true;
        // The sweep body is a NAMED MEMBER COROUTINE. A coroutine lambda here would have
        // its frame owned by the with_gate temporary and outlive what it captured -- the
        // standing rule in this tree, and the bug that segfaulted shard 0 in raft_group.cpp.
        (void)seastar::with_gate(snapshotGate_,
                                 [this] { return snapshotSweep().finally([this] { snapshotSweepRunning_ = false; }); });
    });
    const auto jitter = std::chrono::milliseconds((seastar::this_shard_id() * 731) % 5000);  // NOLINT
    snapshotTimer_.arm(seastar::lowres_clock::now() + kSnapshotSweepInterval + jitter, {kSnapshotSweepInterval});
}

seastar::future<> ReplicatedVShardHost::snapshotSweep() {
    try {
        co_await maybeSnapshotOnce();
        // RECLAIM ON THE SNAPSHOT SWEEP'S TIMER, at a slower cadence (debt D-34).
        //
        // THREE things move a floor, and compaction is only the dominant one. The floor is
        // `min(newest HardState, newest Snapshot, oldest live entry) - 1`, so it also
        // advances when (b) a group whose HARD STATE was the binding term re-persists it --
        // an election, or compaction's own re-persist -- and when (c) a CONFLICTING
        // RE-APPEND in persistEntries pops the superseded suffix off the back of
        // `entrySeqs_` and raises its front, with no snapshot and no hard state involved at
        // all. None of the three is frequent enough to want a 5 s cadence, and a collect
        // reads whole sealed segments, so it gets its own longer interval and piggy-backs
        // on this timer rather than owning one. Failures are caught here with the snapshot
        // failure: nothing was deleted (the unlink is the last step of each segment) and
        // the next pass retries.
        const auto now = seastar::lowres_clock::now();
        if (lastJournalGc_.time_since_epoch().count() == 0 || now - lastJournalGc_ >= kJournalGcInterval) {
            lastJournalGc_ = now;
            const size_t deleted = co_await reclaimJournalSegments();
            const size_t retired = co_await reclaimRetiredJournals();
            if (deleted > 0)
                timestar::http_log.info(
                    "cluster: shard {} reclaimed {} sealed Raft journal segment(s) below the snapshot boundary "
                    "({} total, {} record(s) copied forward)",
                    seastar::this_shard_id(), deleted, journalSegmentsDeleted_, journalRecordsCopiedForward_);
            if (retired > 0)
                timestar::http_log.info(
                    "cluster: shard {} reclaimed {} retired v1 VShard journal generation(s) after the {} hour grace "
                    "period ({} total)",
                    seastar::this_shard_id(), retired, kRetiredJournalGrace.count(), retiredJournalsReclaimed_);
        }
    } catch (const std::exception& e) {
        // (e) A FAILED SNAPSHOT MUST NEVER TAKE THE WRITE PATH WITH IT. Nothing was
        // compacted (compact() is the last step and it either ran or did not), so the group
        // is exactly as it was: a larger log and working replication. Log and move on.
        timestar::http_log.warn("cluster: Raft snapshot sweep failed: {} (the log is kept; will retry)", e.what());
    }
    co_return;
}

seastar::future<size_t> ReplicatedVShardHost::maybeSnapshotOnce() {
    size_t taken = 0;
    if (stopped_ || vshards_.empty())
        co_return 0;
    ++snapshotSweeps_;
    const auto now = seastar::lowres_clock::now();

    // Collect candidates first, THEN snapshot: `snapshotVShard` suspends, and mutating
    // `vshards_` under a suspended iterator over it is how this would become a UAF.
    std::vector<uint16_t> candidates;
    const size_t productionLimit = snapshotProductionLimit();
    // (c) STAGGER: start the scan at a rotating offset so the same low-numbered VShards do
    // not always win the shard's production budget. Two passes over a map is cheaper than
    // any ordering trick and this runs once every 5 s.
    const size_t n = vshards_.size();
    const size_t start = n == 0 ? 0 : snapshotCursor_ % n;
    size_t nextCursor = n == 0 ? 0 : (start + 1) % n;
    size_t idx = 0;
    for (int lap = 0; lap < 2 && candidates.size() < productionLimit; ++lap) {
        idx = 0;
        for (const auto& [vs, state] : vshards_) {
            const size_t currentIdx = idx++;
            const bool inRange = (lap == 0) ? (currentIdx >= start) : (currentIdx < start);
            if (!inRange)
                continue;
            if (candidates.size() >= productionLimit)
                break;
            raft::RaftGroup* g = registry_.group(vs);
            if (!g || !state.sm)
                continue;
            // Not again inside kMinSnapshotInterval, so a hot VShard cannot monopolize the
            // production budget while every other group's log grows.
            if (state.lastSnapshot.time_since_epoch().count() != 0 && now - state.lastSnapshot < snapshotMinInterval_)
                continue;
            const raft::LogIndex boundary = g->node().log().snapshotIndex();
            const uint64_t applied = g->appliedIndex();
            const uint64_t entriesSince = applied > boundary ? applied - boundary : 0;
            snapshotMaxEntriesSinceSeen_ = std::max(snapshotMaxEntriesSinceSeen_, entriesSince);
            // EITHER threshold suffices; a zero threshold disables that half.
            const bool byEntries = snapshotEntryThreshold_ != 0 && entriesSince >= snapshotEntryThreshold_;
            const bool byBytes =
                snapshotByteThreshold_ != 0 && state.sm->appliedBytesSinceSnapshot() >= snapshotByteThreshold_;
            const bool always = snapshotEntryThreshold_ == 0 && snapshotByteThreshold_ == 0;
            if (!byEntries && !byBytes && !always)
                continue;
            candidates.push_back(vs);
            // Resume immediately AFTER the last group selected, not by an arithmetic
            // cursor jump. A `limit + 1` jump creates permanent-looking stripes when its
            // stride shares a divisor with the hosted-group count; with a min interval
            // shorter than a full scan, already-snapshotted groups can become eligible
            // again before those stripes ever get a turn.
            nextCursor = (currentIdx + 1) % n;
        }
    }
    snapshotCursor_ = nextCursor;

    // Sequential, deliberately: shared mode raises the number attempted per pass so a
    // fair scan completes within its target cycle, but it must not mean "read and encode
    // N VShards' TSM files at once on one reactor".
    for (uint16_t vs : candidates) {
        if (stopped_)
            break;
        const uint64_t upto = co_await snapshotVShard(vs);
        if (upto > 0) {
            ++taken;
            timestar::http_log.debug("cluster: snapshotted VShard {} and compacted its Raft log up to index {}", vs,
                                     upto);
        }
    }
    co_return taken;
}

seastar::future<bool> ReplicatedVShardHost::proposeBatch(data::WriteBatch batch) {
    // Group the batch's series by VShard (a series routes to its VShard by hash, same
    // authority every replica uses -- see WriteSeries::vshard). schemaVersion is
    // carried per group.
    return proposeVShardBatches(data::splitByVShard(std::move(batch)));
}

// WHY A BARE `false` NEEDS DECODING (write-scaleout 5 review, F1).
//
// `RaftGroup::proposeAndAwaitApplied` returns false for TWO different situations:
// `role_ != Leader` (ask someone else) and `leadTransferee_ != kNoNode` (I am the leader,
// I am handing leadership away, ask me again shortly). This site used to label both
// `NotLeader` and attach `g->leader()` as the hint -- which, when WE are the refusing
// leader, is OURSELVES. The router accepts the hint, re-buckets the slice into its LOCAL
// view, and asks the same refusing group again; six attempts later the client gets a 503
// saying "not-leader" and NO RPC EVER LEFT THE NODE. That is what made the one-node-down
// symptom look like a routing bug rather than a refusal.
//
// So: report the two apart, and NEVER hand back a hint naming this node. With no hint the
// router re-resolves from its live leader view on the next attempt instead of looping.
data::SliceReject ReplicatedVShardHost::classifyRefusal(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    NodeId hint = g ? g->leader() : raft::kNoNode;
    data::WriteFailure kind = data::WriteFailure::NotLeader;
    if (g && hint == self_) {
        // We ARE the leader and we refused: a transfer is in flight (or leadership flapped
        // between the propose and here). There is nowhere else to send this slice.
        kind = data::WriteFailure::LeaderRefused;
        hint = raft::kNoNode;
        ++proposeRefusedWhileLeader_;
        const auto now = seastar::lowres_clock::now();
        if (now - lastRefusalLog_ >= std::chrono::seconds(5)) {
            lastRefusalLog_ = now;
            timestar::http_log.warn(
                "cluster: this node LEADS VShard {} and refused a proposal ({} such refusals so far); a leadership "
                "transfer is {}in flight. Writes to it fail retryably until the transfer completes or is abandoned.",
                vshard, proposeRefusedWhileLeader_, g->transferInFlight() ? "" : "no longer ");
        }
    }
    return data::SliceReject{vshard, hint, kind};
}

seastar::future<bool> ReplicatedVShardHost::proposeVShardBatches(data::VShardBatches byVShard) {
    // Membership check for the WHOLE batch BEFORE any replication, so a routing error
    // fails atomically (nothing proposed) rather than after some groups committed.
    std::vector<seastar::gate::holder> operations;
    operations.reserve(byVShard.size());
    for (const auto& [vs, b] : byVShard) {
        auto held = holdVShardOperation(vs);
        if (!held || !registry_.group(vs))
            throw std::runtime_error("ReplicatedVShardHost::proposeBatch: VShard not led here");
        operations.push_back(std::move(*held));
    }
    // One admission decision for the whole request, before ANY group can append.
    // Apply is unconditional after this point, including on followers and replay.
    co_await store_.checkWriteAdmission(data::viewOf(byVShard));
    // Replicate every VShard group CONCURRENTLY; every group must commit for the
    // batch to ack.
    //
    // These are independent Raft groups with no ordering between them (this was never
    // an atomic multi-group commit -- see the header contract), but they used to be
    // proposed ONE AT A TIME, so a batch spanning N VShards serialised N full quorum
    // round trips (durable append + replicate + commit + apply) end to end. That was
    // the dominant write latency: measured at ~187ms average per 10k-point batch,
    // matching ~20 VShards x ~9ms. Issuing them together overlaps the network and
    // fsync waits instead of stacking them.
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byVShard.size());
    const auto deadline = seastar::lowres_clock::now() + kProposalTimeout;
    for (auto& [vs, b] : byVShard) {
        auto* g = registry_.group(vs);
        pending.push_back(g->proposeAndAwaitApplied(data::encodeWriteCommand(b), deadline));
    }

    // Await EVERY proposal even after one fails -- abandoning an in-flight future
    // would let its group's commit land on a destroyed continuation.
    bool allOk = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool ok = co_await std::move(f);
            allOk = allOk && ok;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return allOk;
}

seastar::future<data::ProposeOutcome> ReplicatedVShardHost::proposeVShardBatchesHinted(data::VShardBatchView view,
                                                                                       data::OptDeadline deadline) {
    data::ProposeOutcome out;
    // Membership check for the WHOLE view BEFORE any replication (same contract as the
    // bool overload): a routing error rejects atomically rather than after some groups
    // committed. Reported as rejects rather than thrown so the coordinator can re-resolve
    // and retry -- a group that is briefly not hosted here IS a routing miss, not a bug.
    //
    // When it fires NOTHING is proposed, so EVERY group in the view is uncommitted and
    // every one of them is named. Naming only the not-hosted groups (as this first did)
    // made the reject list a strict subset of the truth, and a caller that derived
    // "uncommitted == rejects" then acked slices this node never replicated. The
    // committed-set contract in node_store.hpp is the caller-side half of the same fix;
    // both halves are needed, because either alone leaves the other's mistake fatal.
    std::vector<seastar::gate::holder> operations;
    operations.reserve(view.size());
    bool anyMissing = false;
    for (const auto* g : view) {
        auto held = holdVShardOperation(g->first);
        if (!held || !registry_.group(g->first)) {
            anyMissing = true;
            continue;
        }
        operations.push_back(std::move(*held));
    }
    if (anyMissing) {
        for (const auto* g : view)
            out.rejects.push_back(data::SliceReject{
                g->first, registry_.group(g->first) ? registry_.group(g->first)->leader() : raft::kNoNode,
                data::WriteFailure::NotLeader});
        co_return out;  // committedVShards stays empty: nothing was proposed
    }

    // Reject overloaded work before the first Raft append. The caller classifies
    // WriteOverloadedError as retryable and, critically, unambiguous.
    co_await store_.checkWriteAdmission(view);

    // The RPC deadline lives only in the forwarding client; expiry or disconnect
    // does not cancel this server coroutine. Apply an independent receiver-side
    // maximum even when a caller supplies a later deadline, and share one absolute
    // point across every VShard so fan-out cannot multiply the wait. Earlier caller
    // deadlines remain authoritative.
    deadline = boundedProposalDeadline(deadline);

    // Replicate every group CONCURRENTLY (see proposeVShardBatches for why serialising
    // them cost a full quorum round trip per VShard).
    std::vector<seastar::future<bool>> pending;
    pending.reserve(view.size());
    for (const auto* g : view) {
        raft::RaftGroup* grp = registry_.group(g->first);
        // encodeWriteCommand rather than propose(vshard, ReplicatedCommand{...}): filling
        // the variant would COPY the slice, which the pre-3b path avoided only by moving
        // it (and so losing it). The retry needs the groups kept, so the copy is removed
        // instead of paid.
        pending.push_back(grp->proposeAndAwaitApplied(data::encodeWriteCommand(g->second), deadline));
    }

    // Await EVERY proposal even after one fails -- abandoning an in-flight future would
    // let its group's commit land on a destroyed continuation.
    std::exception_ptr fatalErr;
    for (size_t i = 0; i < pending.size(); ++i) {
        const uint16_t vs = view[i]->first;
        try {
            if (co_await std::move(pending[i])) {
                out.committedVShards.push_back(vs);  // durable quorum commit -- the ONLY way in
            } else {
                out.rejects.push_back(classifyRefusal(vs));
            }
        } catch (...) {
            const auto kind = data::classifyLocalWriteFailure(std::current_exception());
            if (!data::isRetryableWriteFailure(kind)) {
                if (!fatalErr)
                    fatalErr = std::current_exception();
                continue;
            }
            raft::RaftGroup* g = registry_.group(vs);
            NodeId hint = g ? g->leader() : raft::kNoNode;
            if (hint == self_)
                hint = raft::kNoNode;  // never point a retry back at the node that just failed it
            out.rejects.push_back(data::SliceReject{vs, hint, kind});
        }
    }
    if (fatalErr)
        std::rethrow_exception(fatalErr);
    // Derived from what actually committed, never from what was rejected.
    out.committed = out.committedVShards.size() == view.size();
    co_return out;
}

raft::RaftGroup* ReplicatedVShardHost::group(uint16_t vshard) {
    return registry_.group(vshard);
}

seastar::future<bool> ReplicatedVShardHost::awaitApplyCatchUp(std::chrono::milliseconds budget) {
    // FAIL CLOSED DURING SHUTDOWN. A node tearing down cannot prove it has applied what it
    // committed, and "we are stopping" is not evidence that the answer would be complete --
    // it is the reason it might not be. The caller turns this into QUERY_INCOMPLETE, which
    // is what a client should see from a node on its way out.
    if (stopped_ || readFenceGate_.is_closed())
        co_return false;
    const auto holder = readFenceGate_.hold();

    // Enroll every hosted group ONCE, before any suspension. The policy (apply_fence.hpp)
    // owns the two reasons a group is pending and when each clears; this function owns
    // only the sampling, the sleep and the budget.
    ApplyFencePolicy fence;
    for (auto& [vs, state] : vshards_)
        fence.enroll(vs, fenceStateOf(vs));
    if (fence.clear())
        co_return true;  // caught up: the fast path, and it never suspends

    // POLL rather than register a waiter per group. `waitApplied` would be the obvious
    // tool, but it takes the group's lock to register -- and a shard hosting ~1000 groups
    // would take ~1000 locks on a path that is almost always a no-op, on a reactor whose
    // Ready drains want those same locks. Polling costs a handful of integer reads per
    // still-pending group per pass and, unlike a registered waiter, needs nothing unwound
    // when the budget runs out. It also re-reads `hasCurrentTermCommit`, which a waiter
    // registered against a stale commit index could not.
    const auto deadline = seastar::lowres_clock::now() + budget;
    while (seastar::lowres_clock::now() < deadline) {
        co_await seastar::sleep(std::chrono::milliseconds(2));
        if (stopped_)
            co_return false;  // same reasoning as the entry check
        if (fence.refresh([this](uint16_t vs) { return fenceStateOf(vs); }))
            co_return true;
    }
    // FAIL CLOSED. The caller turns this into a QUERY_INCOMPLETE, which is the honest
    // answer: this node holds acknowledged data it cannot yet read out.
    const auto stuck = fence.pendingGroups();
    timestar::http_log.warn(
        "cluster: shard {} still has {} group(s) that cannot prove they have applied what they committed after {} ms "
        "(first: VShard {}); failing the read closed rather than answering out of state that is behind its own log "
        "(debt D-36)",
        seastar::this_shard_id(), stuck.size(), budget.count(), stuck.empty() ? 0 : stuck.front());
    co_return false;
}

FenceGroupState ReplicatedVShardHost::fenceStateOf(uint16_t vshard) {
    FenceGroupState s;
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        return s;  // not hosted: the policy drops it
    s.hosted = true;
    s.hasCurrentTermCommit = g->hasCurrentTermCommit();  // false for a durability-quarantined replica
    s.commitIndex = g->commitIndex();
    s.appliedIndex = g->appliedIndex();
    return s;
}

NodeId ReplicatedVShardHost::leaderOf(uint16_t vshard) const {
    raft::RaftGroup* g = const_cast<raft::RaftGroupRegistry&>(registry_).group(vshard);
    return g ? g->leader() : raft::kNoNode;
}

std::optional<EngineDataStateMachine::DeleteReceiptCounts> ReplicatedVShardHost::deleteReceiptCounts(uint16_t vshard) {
    auto found = vshards_.find(vshard);
    auto* group = registry_.group(vshard);
    if (found == vshards_.end() || !found->second.sm || !group)
        return std::nullopt;
    auto counts = found->second.sm->deleteReceiptCounts();
    counts.hasUnappliedEntries = group->node().log().lastIndex() > group->appliedIndex();
    return counts;
}

ReplicatedVShardHost::DeleteReceiptStats ReplicatedVShardHost::deleteReceiptStats() {
    DeleteReceiptStats stats;
    for (const auto& [vshard, state] : vshards_) {
        if (!state.sm)
            continue;
        const auto counts = state.sm->deleteReceiptCounts();
        stats.retained += counts.total;
        stats.maxPerVShard = std::max(stats.maxPerVShard, counts.total);
        stats.retiredBeforeMaxMs = std::max(stats.retiredBeforeMaxMs, counts.retiredBeforeMs);
        stats.retiredAtMaxIndex = std::max(stats.retiredAtMaxIndex, counts.retiredAtIndex);
        if (counts.retiredAtIndex == 0)
            continue;
        ++stats.groupsWithRetiredFloor;
        auto* group = registry_.group(vshard);
        if (!group || group->node().log().snapshotIndex() < counts.retiredAtIndex)
            ++stats.retirementSnapshotPending;
    }
    return stats;
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderReadIndex(uint16_t vshard) {
    auto operation = holdVShardOperation(vshard);
    if (!operation)
        throw std::runtime_error("ReplicatedVShardHost::leaderReadIndex: VShard not hosted here");
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderReadIndex: VShard not hosted here");
    // readBarrier() runs a quorum-confirmed ReadIndex round and REJECTS (throws) if this
    // node is not the current-term leader -- exactly the partition/redirect signal the
    // reaching replica needs, so no forwarding of stale barriers.
    co_return co_await g->readBarrier(seastar::lowres_clock::now() + data::ReadIndexSink::kAttemptTimeout);
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderCommitIndex(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: VShard not hosted here");
    if (!g->isLeader())
        // Bounded-staleness freshness must come from the leader's commit index; a
        // follower's is itself possibly stale, so reject rather than mislead.
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: not the leader for this VShard");
    return seastar::make_ready_future<raft::LogIndex>(g->commitIndex());
}

seastar::future<> ReplicatedVShardHost::stop() {
    if (stopped_)
        co_return;
    stopped_ = true;
    // Stop the snapshot trigger FIRST and wait for any in-flight sweep: it reads Engine
    // files and calls RaftGroup::compact, both of which need the registry and the store
    // still standing. (e) honours stop() -- an unclosed gate here is a compaction landing
    // on a torn-down group.
    snapshotTimer_.cancel();
    backupCaptureTimer_.cancel();
    if (!backupCaptureGate_.is_closed())
        co_await backupCaptureGate_.close();
    backupCaptureSessions_.clear();
    // The read fence FIRST: a waiting fence resumes into `registry_`/`vshards_`, so it has
    // to be drained while both still stand (debt D-36). It waits at most its own budget.
    if (!readFenceGate_.is_closed())
        co_await readFenceGate_.close();
    if (!snapshotGate_.is_closed())
        co_await snapshotGate_.close();
    co_await registry_.stop();  // stops the tick loop + drains
    // Block new host entry, cancel group waiters, then drain the wrappers that
    // own VShardState references. This is the same ordering as live retirement.
    std::vector<uint16_t> hosted;
    std::vector<std::shared_ptr<raft::RaftGroup>> groupLifetimes;
    hosted.reserve(vshards_.size());
    groupLifetimes.reserve(vshards_.size());
    for (auto& [vshard, state] : vshards_) {
        state.retiring = true;
        hosted.push_back(vshard);
        groupLifetimes.push_back(registry_.groupHandle(vshard));
    }
    for (uint16_t vshard : hosted)
        (void)co_await registry_.removeGroup(vshard);
    for (auto& [vshard, state] : vshards_)
        if (state.operationGate && !state.operationGate->is_closed())
            co_await state.operationGate->close();
    groupLifetimes.clear();
    // The shared sink first: a round in flight holds a reference to the writer, and
    // a waiter it has not yet resolved must be failed rather than left hanging.
    if (sharedSink_)
        co_await sharedSink_->stop();
    for (auto& [vs, state] : vshards_)
        if (state.writer)
            co_await state.writer->close();
    vshards_.clear();
    if (sharedWriter_)
        co_await sharedWriter_->close();
    sharedSink_.reset();
    sharedWriter_.reset();
    sharedRecovered_.clear();
}

}  // namespace timestar::cluster

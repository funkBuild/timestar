#include "raft_journal_persistence.hpp"

#include "raft_config.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace timestar::raft {

namespace {

void putU64(std::string& s, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

uint64_t getU64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
    return v;
}

// Snapshot journal record v1:
// [magic 8][flags u8][index u64][term u64][configLen u64][config][data]
constexpr char kSnapshotRecordMagic[8] = {'T', 'S', 'R', 'S', 'N', 'A', 'P', '1'};
constexpr uint8_t kSnapshotFlagReceivedFromPeer = 0x01;
constexpr uint8_t kSnapshotFlagExternalData = 0x02;
constexpr uint64_t kFnvOffset = 1469598103934665603ull;
constexpr uint64_t kFnvPrime = 1099511628211ull;

uint64_t extendFnv(uint64_t hash, const char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= kFnvPrime;
    }
    return hash;
}

uint64_t hashFileBounded(const std::filesystem::path& path, uint64_t expectedSize) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
        throw std::runtime_error("cannot open snapshot sidecar " + path.string());
    // Seastar cooperative-thread stacks are intentionally small;
    // snapshot-sized scratch storage must never live on them.
    std::vector<char> buffer(size_t{1} << 20);
    uint64_t bytes = 0;
    uint64_t hash = kFnvOffset;
    while (in) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const size_t got = static_cast<size_t>(in.gcount());
        if (got == 0)
            break;
        if (bytes > expectedSize || got > expectedSize - bytes)
            throw std::runtime_error("snapshot sidecar is longer than its descriptor");
        hash = extendFnv(hash, buffer.data(), got);
        bytes += got;
        seastar::thread::yield();
    }
    if (in.bad() || bytes != expectedSize)
        throw std::runtime_error("snapshot sidecar is truncated or unreadable");
    return hash;
}

void fsyncFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        throw std::system_error(errno, std::generic_category(), "open snapshot sidecar for fsync");
    const int rc = ::fsync(fd);
    const int saved = errno;
    ::close(fd);
    if (rc != 0)
        throw std::system_error(saved, std::generic_category(), "fsync snapshot sidecar");
}

void fsyncDirectoryBlocking(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        throw std::system_error(errno, std::generic_category(), "open snapshot directory for fsync");
    const int rc = ::fsync(fd);
    const int saved = errno;
    ::close(fd);
    if (rc != 0)
        throw std::system_error(saved, std::generic_category(), "fsync snapshot directory");
}

void createDirectoriesDurable(const std::filesystem::path& directory) {
    std::vector<std::filesystem::path> missing;
    auto cursor = directory;
    std::error_code ec;
    while (!cursor.empty() && !std::filesystem::exists(cursor, ec)) {
        if (ec)
            throw std::system_error(ec, "inspect snapshot directory");
        missing.push_back(cursor);
        cursor = cursor.parent_path();
    }
    if (cursor.empty() || !std::filesystem::is_directory(cursor, ec) || ec)
        throw std::runtime_error("snapshot directory has no valid existing ancestor: " + directory.string());
    for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
        if (!std::filesystem::create_directory(*it, ec) && ec)
            throw std::system_error(ec, "create snapshot directory");
        fsyncDirectoryBlocking(it->parent_path());
    }
}

bool validSnapshotFilename(std::string_view name) {
    const std::filesystem::path path(name);
    if (name.empty() || path.has_parent_path() || path.filename() != path)
        return false;
    return name.starts_with("snapshot_v1_") && name.ends_with(".bin");
}

std::string encodeSnapshotPayload(const Snapshot& s, bool receivedFromPeer) {
    std::string out;
    out.append(kSnapshotRecordMagic, sizeof(kSnapshotRecordMagic));
    uint8_t flags = receivedFromPeer ? kSnapshotFlagReceivedFromPeer : 0;
    if (s.fileBacked())
        flags |= kSnapshotFlagExternalData;
    out.push_back(static_cast<char>(flags));
    putU64(out, s.index);
    putU64(out, s.term);
    const std::string cfg = encodeConfig(s.config);
    putU64(out, cfg.size());  // length prefix (u64 for simplicity)
    out += cfg;
    if (s.fileBacked()) {
        const std::string name = s.file->path.filename().string();
        if (!validSnapshotFilename(name))
            throw std::invalid_argument("snapshot sidecar has a non-canonical filename");
        putU64(out, s.file->size);
        putU64(out, s.file->hash);
        putU64(out, name.size());
        out += name;
    } else {
        out += s.data;
    }
    return out;
}

// Inverse of encodeSnapshotPayload. Returns nullopt on any malformed/truncated
// payload (fail closed). `index`/`term` are taken from the record header by the
// caller and cross-checked against the payload.
std::optional<Snapshot> decodeSnapshotPayload(const std::string& p, bool* receivedFromPeer,
                                              const std::filesystem::path& snapshotDirectory) {
    constexpr size_t off = sizeof(kSnapshotRecordMagic) + 1;
    if (receivedFromPeer)
        *receivedFromPeer = false;
    if (p.size() < off + 24 || std::memcmp(p.data(), kSnapshotRecordMagic, sizeof(kSnapshotRecordMagic)) != 0)
        return std::nullopt;
    const uint8_t flags = static_cast<uint8_t>(p[sizeof(kSnapshotRecordMagic)]);
    if ((flags & ~(kSnapshotFlagReceivedFromPeer | kSnapshotFlagExternalData)) != 0)
        return std::nullopt;
    if (receivedFromPeer)
        *receivedFromPeer = (flags & kSnapshotFlagReceivedFromPeer) != 0;
    Snapshot s;
    s.index = getU64(p.data() + off);
    s.term = getU64(p.data() + off + 8);
    const uint64_t cfgLen = getU64(p.data() + off + 16);
    if (cfgLen > p.size() - off - 24)
        return std::nullopt;
    s.config = decodeConfig(p.substr(off + 24, cfgLen));
    const size_t dataOff = off + 24 + static_cast<size_t>(cfgLen);
    if ((flags & kSnapshotFlagExternalData) != 0) {
        if (snapshotDirectory.empty() || p.size() < dataOff + 24)
            return std::nullopt;
        const uint64_t size = getU64(p.data() + dataOff);
        const uint64_t hash = getU64(p.data() + dataOff + 8);
        const uint64_t nameLen = getU64(p.data() + dataOff + 16);
        if (nameLen > p.size() - dataOff - 24 || dataOff + 24 + nameLen != p.size())
            return std::nullopt;
        const std::string name = p.substr(dataOff + 24, static_cast<size_t>(nameLen));
        if (!validSnapshotFilename(name))
            return std::nullopt;
        const auto path = snapshotDirectory / name;
        std::error_code ec;
        if (!std::filesystem::is_regular_file(path, ec) || ec || std::filesystem::file_size(path, ec) != size || ec)
            return std::nullopt;
        s.file = std::make_shared<SnapshotFile>();
        s.file->path = path;
        s.file->size = size;
        s.file->hash = hash;
    } else {
        s.data = p.substr(dataOff);
    }
    return s;
}

}  // namespace

JournalRaftPersistence::JournalRaftPersistence(JournalWriter& writer, VShardId vshard, uint64_t startSeq,
                                               std::filesystem::path snapshotDirectory)
    : owned_(std::in_place, writer),
      sink_(*owned_),
      vshard_(vshard),
      nextSeq_(startSeq == 0 ? 1 : startSeq),
      startSeq_(nextSeq_),
      snapshotDirectory_(std::move(snapshotDirectory)) {}

JournalRaftPersistence::JournalRaftPersistence(JournalSink& sink, VShardId vshard, uint64_t startSeq,
                                               std::filesystem::path snapshotDirectory)
    : sink_(sink),
      vshard_(vshard),
      nextSeq_(startSeq == 0 ? 1 : startSeq),
      startSeq_(nextSeq_),
      snapshotDirectory_(std::move(snapshotDirectory)) {}

seastar::future<> JournalRaftPersistence::appendFenced(const JournalRecord& r) {
    // A failed append means the bookkeeping above already counted a record that is not in
    // the buffer, so no later sync may promote a sample derived from it. Freeze the floor
    // (debt D-34); the writer fences itself for the durability half.
    try {
        co_await sink_.append(r);
    } catch (...) {
        fenced_ = true;
        throw;
    }
}

uint64_t JournalRaftPersistence::intendedFloor() const {
    // Nothing is releasable until a snapshot exists: without one the whole log is
    // live, and its very first record is the log's own beginning.
    if (lastSnapshotSeq_ == 0)
        return 0;
    uint64_t floor = lastSnapshotSeq_;
    if (lastHardStateSeq_ != 0)
        floor = std::min(floor, lastHardStateSeq_);
    if (!entrySeqs_.empty())
        floor = std::min(floor, entrySeqs_.front().second);
    return floor - 1;  // floor >= 1 here (seqs start at 1), so this cannot underflow
}

void JournalRaftPersistence::seedRetention(JournalRetentionSeed seed) {
    // See the header: the promotion below is sound ONLY for records that are already on
    // disk. Fail loudly rather than let a second caller (or a caller placed after the
    // first append) launder buffered state into the durable watermark.
    if (seeded_)
        throw std::logic_error("JournalRaftPersistence::seedRetention called twice");
    if (nextSeq_ != startSeq_)
        throw std::logic_error(
            "JournalRaftPersistence::seedRetention called after a record was appended; the seeded floor may only be "
            "promoted from RECOVERED (already durable) records");
    seeded_ = true;
    lastHardStateSeq_ = seed.latestHardStateSeq;
    lastSnapshotSeq_ = seed.latestSnapshotSeq;
    entrySeqs_.assign(seed.entrySeqs.begin(), seed.entrySeqs.end());
    // Recovered records ARE durable -- they were read back off the disk -- so the seeded
    // floor is immediately reportable, without waiting for this incarnation to sync.
    durableFloor_ = std::max(durableFloor_, intendedFloor());
}

void JournalRaftPersistence::seedSnapshotFile(SnapshotFilePtr file) {
    if (nextSeq_ != startSeq_)
        throw std::logic_error("JournalRaftPersistence::seedSnapshotFile called after append");
    currentSnapshotFile_ = std::move(file);
}

seastar::future<> JournalRaftPersistence::persistHardState(HardState hs) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    lastHardStateSeq_ = r.vshardSeq;  // the newest HardState record pins the floor (D-34)
    r.kind = JournalRecordKind::HardState;
    r.raftTerm = hs.currentTerm;
    r.payload.reserve(8);
    putU64(r.payload, hs.votedFor);
    co_await appendFenced(r);
}

// NOTE FOR A FUTURE EDITOR (debt D-34). `JournalRecordKind::Truncation` has NO producer
// in this tree -- a suffix truncation is expressed as a RE-APPEND at the lower index, and
// that is what the pop_back below mirrors. If anyone ever adds a real Truncation producer
// it MUST also drop entrySeqs_ at/above the truncation index, exactly as recoverRaftState's
// Truncation case erases `entrySeq`; forgetting to would leave dead records pinning the
// floor (harmless) or, if the mirror drifted the other way, release live ones (not).
seastar::future<> JournalRaftPersistence::persistEntries(std::vector<LogEntry> entries) {
    for (const auto& e : entries) {
        JournalRecord r;
        r.vshard = vshard_;
        r.vshardSeq = nextSeq_++;
        r.kind = (e.type == EntryType::ConfigChange) ? JournalRecordKind::Config : JournalRecordKind::Data;
        r.raftTerm = e.term;
        r.raftIndex = e.index;
        r.payload = e.data;
        // Mirror what recoverRaftState will rebuild (D-34): a re-append at index I
        // supersedes every record at or above I, so those records stop pinning the
        // floor. Keeps entrySeqs_ ascending in BOTH index and seq.
        while (!entrySeqs_.empty() && entrySeqs_.back().first >= e.index)
            entrySeqs_.pop_back();
        entrySeqs_.emplace_back(e.index, r.vshardSeq);
        co_await appendFenced(r);
    }
}

seastar::future<> JournalRaftPersistence::hydrateSnapshotChunk(InstallSnapshot& chunk) {
    if (!chunk.sourceFile)
        co_return;
    const auto file = chunk.sourceFile;
    if (file->path.empty() || chunk.totalBytes != file->size || chunk.offset > file->size ||
        chunk.sourceLength > file->size - chunk.offset)
        throw std::runtime_error("JournalRaftPersistence: invalid file-backed snapshot slice");
    const auto path = file->path;
    const uint64_t offset = chunk.offset;
    const size_t length = chunk.sourceLength;
    chunk.data = co_await seastar::async([path, offset, length] {
        std::ifstream in(path, std::ios::binary);
        if (!in)
            throw std::runtime_error("cannot open servable snapshot sidecar " + path.string());
        in.seekg(static_cast<std::streamoff>(offset));
        if (!in)
            throw std::runtime_error("cannot seek servable snapshot sidecar " + path.string());
        std::string bytes(length, '\0');
        if (length != 0)
            in.read(bytes.data(), static_cast<std::streamsize>(length));
        if (static_cast<size_t>(in.gcount()) != length)
            throw std::runtime_error("short read from servable snapshot sidecar " + path.string());
        return bytes;
    });
    // Internal backing is intentionally absent from the encoded envelope. Keep
    // the local Ready copy simple and make an accidental second hydration fail.
    chunk.sourceFile.reset();
    chunk.sourceLength = 0;
    co_return;
}

seastar::future<> JournalRaftPersistence::stageSnapshotChunk(InstallSnapshot& chunk) {
    if (snapshotDirectory_.empty())
        co_return;  // bounded inline v1 path (tests and Group 0)

    const uint64_t total = chunk.totalBytes == 0 ? chunk.data.size() : chunk.totalBytes;
    if (chunk.offset > total || chunk.data.size() > total - chunk.offset)
        co_return;  // the wire decoder normally rejects this; let the core answer safely
    if (total > kMaxVShardSnapshotFileBytes)
        co_return;  // core's inline total bound refuses it without touching disk

    if (incomingStage_ &&
        (incomingStage_->index != chunk.lastIncludedIndex || incomingStage_->term != chunk.lastIncludedTerm))
        incomingStage_.reset();

    if (chunk.offset == 0) {
        incomingStage_.reset();
        const uint64_t nonce = ++snapshotNonce_;
        const auto path =
            snapshotDirectory_ /
            ("snapshot_v1_stage_g" + std::to_string(vshard_.value()) + "_i" + std::to_string(chunk.lastIncludedIndex) +
             "_t" + std::to_string(chunk.lastIncludedTerm) + "_n" + std::to_string(nonce) + ".bin");
        co_await seastar::async([directory = snapshotDirectory_, path] {
            createDirectoriesDurable(directory);
            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            if (!out)
                throw std::runtime_error("cannot create incoming snapshot stage " + path.string());
        });
        IncomingSnapshotStage fresh;
        fresh.index = chunk.lastIncludedIndex;
        fresh.term = chunk.lastIncludedTerm;
        fresh.totalBytes = total;
        fresh.file = std::make_shared<SnapshotFile>();
        fresh.file->path = path;
        fresh.file->hash = kFnvOffset;
        fresh.file->removeOnDestroy = true;
        incomingStage_ = std::move(fresh);
    } else if (!incomingStage_) {
        co_return;  // follower has no prefix; the core replies with resume offset zero
    }

    auto& stage = *incomingStage_;
    chunk.externallyStaged = true;
    chunk.stagedBytes = stage.receivedBytes;
    if (chunk.offset != stage.receivedBytes || total != stage.totalBytes) {
        chunk.data.clear();
        co_return;  // duplicate/gap: report the actual durable-in-process prefix
    }

    if (chunk.done && chunk.data.size() != total - stage.receivedBytes)
        throw std::runtime_error("JournalRaftPersistence: final snapshot chunk has inconsistent length");
    const uint64_t nextHash = extendFnv(stage.hash, chunk.data.data(), chunk.data.size());
    const size_t writeBytes = chunk.data.size();
    const auto path = stage.file->path;
    const uint64_t expectedPrefix = stage.receivedBytes;
    std::string bytes = std::move(chunk.data);
    co_await seastar::async([path, expectedPrefix, bytes = std::move(bytes)]() mutable {
        std::error_code ec;
        const uint64_t actual = std::filesystem::file_size(path, ec);
        if (ec || actual < expectedPrefix)
            throw std::runtime_error("incoming snapshot stage lost an accepted prefix: " + path.string());
        if (actual != expectedPrefix) {
            std::filesystem::resize_file(path, expectedPrefix, ec);
            if (ec)
                throw std::system_error(ec, "repair partial incoming snapshot append");
        }
        std::ofstream out(path, std::ios::binary | std::ios::app);
        if (!out)
            throw std::runtime_error("cannot append incoming snapshot stage " + path.string());
        size_t offset = 0;
        while (offset < bytes.size()) {
            const size_t count = std::min<size_t>(size_t{1} << 20, bytes.size() - offset);
            out.write(bytes.data() + offset, static_cast<std::streamsize>(count));
            if (!out)
                throw std::runtime_error("cannot write incoming snapshot stage " + path.string());
            offset += count;
            seastar::thread::yield();
        }
        out.flush();
        if (!out)
            throw std::runtime_error("cannot write incoming snapshot stage " + path.string());
    });

    if (chunk.done) {
        co_await seastar::async([path] { fsyncFile(path); });
    }
    // Publish progress only after the append, and for the final chunk its fsync,
    // succeeded. A retry after a partial write truncates back to this accepted
    // prefix before re-appending, so disk and the Raft core cannot diverge.
    stage.hash = nextHash;
    stage.receivedBytes += writeBytes;
    stage.file->size = stage.receivedBytes;
    stage.file->hash = stage.hash;
    chunk.stagedBytes = stage.receivedBytes;
    if (chunk.done) {
        chunk.completedFile = stage.file;
        incomingStage_.reset();
    }
    co_return;
}

seastar::future<> JournalRaftPersistence::persistSnapshot(Snapshot snap, bool receivedFromPeer) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    lastSnapshotSeq_ = r.vshardSeq;
    r.kind = JournalRecordKind::Snapshot;
    r.raftTerm = snap.term;
    r.raftIndex = snap.index;
    // Entries at or below the boundary are compacted away, so their records stop
    // pinning the reclaim floor (D-34). This is what usually lets the INTENDED floor
    // move; only a successful sync() promotes it into the reported one.
    while (!entrySeqs_.empty() && entrySeqs_.front().first <= snap.index)
        entrySeqs_.pop_front();
    try {
        if (snap.fileBacked()) {
            if (snapshotDirectory_.empty())
                throw std::runtime_error("JournalRaftPersistence: file-backed snapshot has no sidecar directory");
            co_await validateSnapshotFile(*snap.file);
            const auto target = snapshotDirectory_ /
                                ("snapshot_v1_g" + std::to_string(vshard_.value()) + "_i" + std::to_string(snap.index) +
                                 "_t" + std::to_string(snap.term) + "_s" + std::to_string(r.vshardSeq) + ".bin");
            if (snap.file->path != target) {
                const auto source = snap.file->path;
                co_await seastar::async([directory = snapshotDirectory_, source, target] {
                    createDirectoriesDurable(directory);
                    if (source.parent_path() != directory)
                        throw std::runtime_error("snapshot sidecar promotion crossed its private directory");
                    std::error_code ec;
                    if (std::filesystem::exists(target, ec) || ec)
                        throw std::runtime_error("snapshot sidecar target already exists or cannot be inspected: " +
                                                 target.string());
                    std::filesystem::rename(source, target, ec);
                    if (ec)
                        throw std::system_error(ec, "promote snapshot sidecar");
                    fsyncFile(target);
                });
                // Rename has already changed the durable object's name. Update the
                // shared handle before another suspension so the node/Ready copies
                // cannot retain a stale source path.
                snap.file->path = target;
            }
            co_await seastar::sync_directory(snapshotDirectory_.string());
            snap.file->removeOnDestroy = true;  // committed only after sync() below
            pendingSnapshotFile_ = snap.file;
            pendingSupersededFile_ = currentSnapshotFile_;
        } else {
            pendingSnapshotFile_.reset();
            pendingSupersededFile_ = currentSnapshotFile_;
        }
        pendingSnapshotUpdate_ = true;
        r.payload = encodeSnapshotPayload(snap, receivedFromPeer);
        co_await appendFenced(r);
    } catch (...) {
        // Sequence/floor bookkeeping above already describes this record. Once
        // preparation or append fails, allowing a later operation to proceed
        // could promote a reclaim floor for a descriptor that never became
        // durable. Match append/sync failure handling and fail closed.
        fenced_ = true;
        throw;
    }
}

seastar::future<> JournalRaftPersistence::sync() {
    // SAMPLE THE FLOOR BEFORE THE BARRIER, PROMOTE IT AFTER (debt D-34). At this point
    // every append of this Ready has been awaited, so the records the sample depends on
    // are already in the writer's buffer and barrier() -- a whole-buffer flush -- is
    // guaranteed to cover them. Anything appended AFTER this line lands in a later
    // sample, which is the conservative direction.
    const uint64_t candidate = intendedFloor();
    if (fenced_)
        throw std::runtime_error("JournalRaftPersistence: journal fenced; the reclaim floor is frozen");
    try {
        co_await sink_.sync();
    } catch (...) {
        // The sync did NOT happen. Freeze the floor permanently rather than let a later
        // success promote a sample that assumed these records landed. The writer fences
        // itself too, so every subsequent append/sync throws anyway -- this makes the
        // floor's own invariant independent of that.
        fenced_ = true;
        throw;
    }
    durableFloor_ = std::max(durableFloor_, candidate);
    if (pendingSnapshotUpdate_) {
        // The journal descriptor is now durable. Only now may this become the
        // retained sidecar and the prior one become collectible. An ordinary
        // entry/hard-state sync does not enter this branch: clearing the current
        // handle on every sync made the next snapshot forget its predecessor and
        // leaked every canonical sidecar until restart cleanup.
        if (pendingSupersededFile_ && pendingSupersededFile_ != pendingSnapshotFile_) {
            pendingSupersededFile_->removeOnDestroy = true;
            // Do not wait for the last shared_ptr to disappear before unlinking the
            // superseded payload. Ready copies and completed snapshot-transfer state
            // may legitimately retain the handle after a later descriptor is durable;
            // relying on SnapshotFile's destructor therefore leaked one sidecar per
            // compaction until those unrelated owners happened to drain.  Unlinking is
            // safe now: the new descriptor is past the journal durability barrier and
            // Raft clears transfers whenever it replaces its servable snapshot.  Keep
            // removeOnDestroy armed as a best-effort retry if the unlink fails.  A crash
            // before the directory entry is durably retired is harmless because startup
            // cleanup retains only the latest descriptor.
            if (!pendingSupersededFile_->pinned()) {
                std::error_code ec;
                std::filesystem::remove(pendingSupersededFile_->path, ec);
            }
        }
        if (pendingSnapshotFile_)
            pendingSnapshotFile_->removeOnDestroy = false;
        currentSnapshotFile_ = pendingSnapshotFile_;
        pendingSnapshotFile_.reset();
        pendingSupersededFile_.reset();
        pendingSnapshotUpdate_ = false;
    }
}

seastar::future<> validateSnapshotFile(const SnapshotFile& file) {
    const auto path = file.path;
    const uint64_t size = file.size;
    const uint64_t expectedHash = file.hash;
    // Return the cooperative-thread future directly: hashing and validation
    // are a single operation and need no reactor-side coroutine frame.
    return seastar::async([path, size, expectedHash] {
        if (hashFileBounded(path, size) != expectedHash)
            throw std::runtime_error("snapshot sidecar hash mismatch: " + path.string());
    });
}

seastar::future<> cleanupSnapshotDirectory(const std::filesystem::path& directory, const SnapshotFilePtr& retained) {
    if (directory.empty())
        co_return;
    const auto retainedPath = retained ? retained->path : std::filesystem::path{};
    const bool changed = co_await seastar::async([directory, retainedPath] {
        std::error_code ec;
        const auto rootStatus = std::filesystem::symlink_status(directory, ec);
        if (ec) {
            if (ec == std::errc::no_such_file_or_directory)
                return false;
            throw std::system_error(ec, "inspect snapshot sidecar root");
        }
        if (!std::filesystem::exists(rootStatus))
            return false;
        if (!std::filesystem::is_directory(rootStatus))
            throw std::runtime_error("snapshot sidecar root is not a directory: " + directory.string());
        bool removed = false;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto status = entry.symlink_status();
            const auto name = entry.path().filename().string();
            if (name == "extract_tmp") {
                if (!std::filesystem::is_directory(status))
                    throw std::runtime_error("snapshot extraction namespace is not a directory: " +
                                             entry.path().string());
                std::filesystem::remove_all(entry.path());
                removed = true;
                continue;
            }
            if (!std::filesystem::is_regular_file(status) || !validSnapshotFilename(name))
                throw std::runtime_error("unknown snapshot sidecar artifact: " + entry.path().string());
            if (entry.path() == retainedPath)
                continue;
            std::filesystem::remove(entry.path());
            removed = true;
        }
        return removed;
    });
    if (changed)
        co_await seastar::sync_directory(directory.string());
    co_return;
}

RecoveredRaftState recoverRaftState(const std::vector<JournalRecord>& records, VShardId vshard,
                                    const std::filesystem::path& snapshotDirectory) {
    // Collect this VShard's records and replay them in vshard_seq order (their
    // true append order, independent of physical position in the shared stream).
    std::vector<const JournalRecord*> mine;
    for (const auto& r : records)
        if (r.vshard.value() == vshard.value())
            mine.push_back(&r);
    std::sort(mine.begin(), mine.end(),
              [](const JournalRecord* a, const JournalRecord* b) { return a->vshardSeq < b->vshardSeq; });

    RecoveredRaftState out;
    std::map<LogIndex, LogEntry> entries;  // ordered by index; a re-append overwrites
    // Parallel to `entries` in EVERY mutation below: the vshard_seq of the record that
    // put each surviving entry there. It is what seeds the reclaim floor (D-34), and it
    // must be derived here rather than guessed later -- a fresh JournalRaftPersistence
    // has no idea which seqs the records already on disk carry.
    std::map<LogIndex, uint64_t> entrySeq;
    LogIndex snapIndex = kNoIndex;
    Term snapTerm = kNoTerm;
    const JournalRecord* latestSnapshot = nullptr;

    for (const JournalRecord* r : mine) {
        out.nextSeq = r->vshardSeq + 1;
        switch (r->kind) {
            case JournalRecordKind::HardState:
                out.hardState.currentTerm = r->raftTerm;
                out.hardState.votedFor = (r->payload.size() >= 8) ? getU64(r->payload.data()) : kNoNode;
                out.retention.latestHardStateSeq = r->vshardSeq;
                break;
            case JournalRecordKind::Data:
            case JournalRecordKind::Config: {
                LogEntry e;
                e.term = r->raftTerm;
                e.index = r->raftIndex;
                e.type = (r->kind == JournalRecordKind::Config) ? EntryType::ConfigChange : EntryType::Normal;
                e.data = r->payload;
                // Drop any entries above this index that a prior append left; a
                // lower-index re-append means the higher suffix was superseded.
                entries.erase(entries.upper_bound(r->raftIndex), entries.end());
                entrySeq.erase(entrySeq.upper_bound(r->raftIndex), entrySeq.end());
                entries[r->raftIndex] = std::move(e);
                entrySeq[r->raftIndex] = r->vshardSeq;
                break;
            }
            case JournalRecordKind::Truncation: {
                // payload = the index to truncate from (inclusive).
                if (r->payload.size() >= 8) {
                    const LogIndex from = getU64(r->payload.data());
                    entries.erase(entries.lower_bound(from), entries.end());
                    entrySeq.erase(entrySeq.lower_bound(from), entrySeq.end());
                }
                break;
            }
            case JournalRecordKind::Snapshot: {
                snapIndex = r->raftIndex;
                snapTerm = r->raftTerm;
                latestSnapshot = r;
                out.retention.latestSnapshotSeq = r->vshardSeq;
                // Entries at or below the snapshot boundary are compacted away.
                entries.erase(entries.begin(), entries.upper_bound(snapIndex));
                entrySeq.erase(entrySeq.begin(), entrySeq.upper_bound(snapIndex));
                break;
            }
            default:
                break;  // CatalogCreate/Retention are not Raft state
        }
    }

    // Decode only the newest snapshot record. A durable replacement makes its
    // predecessor's sidecar collectible immediately, while the append-only
    // journal may retain that older descriptor until segment GC. Requiring
    // every superseded sidecar during replay would therefore make the first
    // restart after a second compaction fail closed despite a complete latest
    // snapshot.
    if (latestSnapshot) {
        bool fromPeer = false;
        if (auto snap = decodeSnapshotPayload(latestSnapshot->payload, &fromPeer, snapshotDirectory)) {
            if (snap->index != snapIndex || snap->term != snapTerm)
                throw std::runtime_error("recoverRaftState: snapshot record/header boundary mismatch");
            out.snapshot = std::move(*snap);
            out.snapshotFromPeer = fromPeer;
        } else {
            throw std::runtime_error("recoverRaftState: malformed or unavailable exact-v1 snapshot payload");
        }
    }

    // Rebuild the log: start from the snapshot boundary (if any), then append the
    // surviving entries in index order.
    if (snapIndex != kNoIndex)
        out.log.restoreFromSnapshot(snapIndex, snapTerm);
    std::vector<LogEntry> ordered;
    ordered.reserve(entries.size());
    for (auto& [idx, e] : entries)
        ordered.push_back(std::move(e));
    if (!ordered.empty())
        out.log.append(std::move(ordered));
    out.retention.entrySeqs.reserve(entrySeq.size());
    for (const auto& [idx, seq] : entrySeq)
        out.retention.entrySeqs.emplace_back(idx, seq);
    return out;
}

}  // namespace timestar::raft

#include "engine.hpp"

#include "aggregator.hpp"
#include "compact_vshard.hpp"
#include "key_encoding.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "migrate_vshard.hpp"
#include "placement_table.hpp"
#include "query_runner.hpp"
#include "restore_vshard.hpp"
#include "series_catalog.hpp"
#include "series_key.hpp"
#include "tsm_compactor.hpp"
#include "tsm_writer.hpp"
#include "util.hpp"
#include "value_coercion.hpp"       // lossless coercion into a series' bound type
#include "value_type_dispatch.hpp"  // dispatchValueType / valueTypeOf / valueTypeName
#include "vshard_snapshot_builder.hpp"
#include "vshard_snapshot_extents.hpp"
#include "vshard_snapshot_read.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/file.hh>
#include <set>
#include <system_error>
#include <unordered_set>
#include <variant>
#include <vector>

namespace fs = std::filesystem;

Engine::Engine(timestar::StorageLayout layout)
    : layout_(std::move(layout)),
      shardId(seastar::this_shard_id()),
      tsmFileManager(layout_, shardId),
      walFileManager(layout_, shardId),
      index(layout_, static_cast<int>(shardId)) {
          // Directory creation moved to init() to avoid blocking the reactor thread
      };

seastar::future<> Engine::init() {
    co_await createDirectoryStructure();
    co_await index.open();
    co_await tsmFileManager.init();
    // Order-independent handshake: the server creates scheduling groups AFTER
    // init(), in which case setIOSchedulingGroups() does the forwarding. Tests
    // and embedders that create them first are covered here.
    if (_schedulingGroupsCreated) {
        tsmFileManager.setCompactionGroup(_compactionGroup);
        tsmFileManager.setFlushGroup(_flushGroup);
    }
    co_await walFileManager.init(*this, tsmFileManager);

    // Restore the per-shard revision counter above every revision already durable
    // in the WAL-replayed memory stores and in the flushed TSM files, so a
    // post-restart write can never be assigned a revision that loses the LWW to
    // older data (ADR 0003). Cheap: TSM contributes its file-level trailer only.
    restoreRevisionCounter();

    // Register per-shard Prometheus metrics
    _metrics.setup(*this);

    // Retention policy loading is done post-startup via loadAndBroadcastRetentionPolicies()
    // after setShardedRef() has been called on all shards.

    // The background compaction loop is now REQUIRED, not optional. writeMemstore()
    // no longer compacts inline (that coupling is what let a deep tier merge hold
    // the WAL conversion slot and stall ingest), so this loop is the only thing
    // that merges tiers. It is started by startBackgroundCompaction() rather than
    // here, because it must not run until the scheduling groups exist -- otherwise
    // merges would land in `main` and preempt foreground writes, which is exactly
    // the failure being fixed.
};

seastar::future<> Engine::startBackgroundCompaction() {
    if (!_schedulingGroupsCreated) {
        // Refuse rather than silently running merges in `main`. Starting the
        // loop before the groups exist reintroduces the exact regression this
        // path was added to prevent.
        timestar::compactor_log.warn("Shard {}: background compaction not started -- scheduling groups not yet set",
                                     shardId);
        co_return;
    }
    co_await tsmFileManager.startCompactionLoop();
}

void Engine::setVShardPartitionedCompaction(bool on) {
    auto* compactor = tsmFileManager.getCompactor();
    if (compactor == nullptr)
        throw std::logic_error("setVShardPartitionedCompaction requires Engine::init");
    compactor->setVShardPartitioning(on);
}

bool Engine::vshardPartitionedCompactionEnabled() const {
    const auto* compactor = tsmFileManager.getCompactor();
    return compactor != nullptr && compactor->vshardPartitioningEnabled();
}

seastar::future<> Engine::createDirectoryStructure() {
    std::string shardPath = layout_.tsmDir(shardId).string();
    // Wrap blocking std::filesystem call in seastar::async to avoid
    // blocking the Seastar reactor thread.
    co_await seastar::async([&shardPath]() {
        fs::create_directories(shardPath);
        // Snapshot staging directories are never live state. A crash can leave
        // complete-looking .tsm files inside them, so remove them before the
        // file manager scans the live directory. Per-operation subdirectories
        // make concurrent installs independent during normal operation.
        std::error_code ec;
        fs::remove_all(fs::path(shardPath) / "snapin_tmp", ec);
        ec.clear();
        fs::remove_all(fs::path(shardPath) / "snapout_tmp", ec);
    });
}

std::string Engine::basePath() {
    return layout_.shardDir(shardId).string();
}

seastar::future<> Engine::stop() {
    auto stopStart = std::chrono::steady_clock::now();
    auto elapsedMs = [&]() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - stopStart)
            .count();
    };

    timestar::engine_log.info("[ENGINE_STOP] Starting shutdown on shard {}", shardId);

    if (!_insertGate.is_closed()) {
        timestar::engine_log.info("[ENGINE_STOP] Closing insert gate ({} in-flight) on shard {}",
                                  _insertGate.get_count(), shardId);
        co_await _insertGate.close();
        timestar::engine_log.info("[ENGINE_STOP] Insert gate closed ({}ms) on shard {}", elapsedMs(), shardId);
    }

    if (!_streamingGate.is_closed()) {
        co_await _streamingGate.close();
        timestar::engine_log.info("[ENGINE_STOP] Streaming gate closed ({}ms) on shard {}", elapsedMs(), shardId);
    }

    if (shardId == 0) {
        _retentionTimer.cancel();
    }
    if (!_retentionGate.is_closed()) {
        co_await _retentionGate.close();
        if (shardId == 0) {
            timestar::engine_log.info("[ENGINE_STOP] Retention drained ({}ms) on shard 0", elapsedMs());
        }
    }

    co_await tsmFileManager.stopCompactionLoop();
    timestar::engine_log.info("[ENGINE_STOP] Compaction loop stopped ({}ms) on shard {}", elapsedMs(), shardId);

    co_await tsmFileManager.stop();
    timestar::engine_log.info("[ENGINE_STOP] TSM files closed ({}ms) on shard {}", elapsedMs(), shardId);

    co_await walFileManager.close();
    timestar::engine_log.info("[ENGINE_STOP] WAL closed ({}ms) on shard {}", elapsedMs(), shardId);

    co_await index.close();
    timestar::engine_log.info("[ENGINE_STOP] Index closed ({}ms) on shard {}", elapsedMs(), shardId);

    timestar::engine_log.info("[ENGINE_STOP] Shutdown complete ({}ms) on shard {}", elapsedMs(), shardId);
}

template <class T>
void Engine::stampRevisions(TimeStarInsert<T>& req) {
    if (!assignRevisions_)
        return;  // non-cluster: leave the revision column empty (zero overhead)
    // Honor caller-provided revisions: the replicated apply path
    // (EngineDataStateMachine) stamps revision = Raft log index so a value is a pure
    // function of the log and identical on every replica (ADR 0003). Re-stamping
    // those with the local per-shard counter would break cross-replica determinism
    // after a restart (which flips assignRevisions_ on via restoreRevisionCounter).
    // See write_record.hpp: "the Engine must not re-stamp a non-empty revision vector".
    if (!req.revisions.empty())
        return;
    const size_t count = req.getTimestamps().size();
    if (count == 0)
        return;
    // One replicated revision per insert (ADR 0003: a record's points share its
    // sequence). Monotonic per shard, hence per VShard (a subsequence).
    const uint64_t rev = nextRevision_++;
    req.revisions.assign(count, rev);
}

seastar::future<timestar::VShardSnapshotManifest> Engine::createVShardSnapshot(timestar::VShardId vshard,
                                                                               std::string catalogHash) {
    // Snapshot this shard's flushed TSM files (the caller flushes memory first).
    // Collect them with their rank as the extent file id.
    //
    // TAKE THE FILE LIST BY VALUE FIRST -- iterating the LIVE map across the co_await
    // below is a use-after-free, and it crashed a node. `getSequencedTsmFiles()` returns
    // a reference to the manager's own map, and background tier compaction ERASES entries
    // from it (`removeTSMFiles`) when a merge completes. A merge landing during
    // `addTsmFileExtents`' suspension destroys the node the loop is standing on, so the
    // structured binding refers to freed storage and `++it` walks a dangling next-pointer:
    // the loop then copies garbage `shared_ptr`s into `files` and touches a control block
    // at a nonsense address. Observed exactly that way on a restart replay --
    // "Compacted 4 files from tier 0 to tier 1" in the millisecond before
    // `si_addr: 0x0a` on shard 0, inside the vector growth this loop performs.
    //
    // A COPY IS THE FIX, not a lock: the copied `shared_ptr`s keep every TSM alive for the
    // whole snapshot even if compaction unregisters and unlinks it, so the manifest
    // describes a consistent point-in-time set. A file that vanishes before its BYTES are
    // read is already handled downstream -- `buildVShardSnapshotFiles` fails closed on a
    // manifest fileId it can no longer resolve rather than shipping a partial snapshot.
    std::vector<std::pair<uint64_t, seastar::shared_ptr<::TSM>>> sequenced;
    {
        const auto& live = tsmFileManager.getSequencedTsmFiles();
        sequenced.reserve(live.size());
        for (const auto& [rank, file] : live)
            if (file)
                sequenced.emplace_back(rank, file);
    }

    std::vector<seastar::shared_ptr<::TSM>> files;
    files.reserve(sequenced.size());
    timestar::VShardExtentMap extents;
    for (const auto& [rank, file] : sequenced) {
        files.push_back(file);
        co_await timestar::addTsmFileExtents(extents, rank, *file);
    }

    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, files, builder);
    co_return builder.build(extents, std::move(catalogHash));
}

namespace {
// Open a TSM file by path (its ctor parses tier/seq from the filename, so
// dataRank ordering is correct) and load its sparse index.
seastar::future<seastar::shared_ptr<::TSM>> openTsmForVShardOp(std::string path) {
    auto tsm = seastar::make_shared<::TSM>(path);
    co_await tsm->open();
    co_await tsm->readSparseIndex();
    co_return tsm;
}

bool containsVShard(const ::TSM& file, uint16_t vshard) {
    bool contains = false;
    file.forEachSeriesId([&](const SeriesId128& id) { contains = contains || timestar::virtualShard(id) == vshard; });
    return contains;
}

bool isPureVShardObject(const ::TSM& file, uint16_t vshard) {
    bool any = false;
    bool pure = true;
    file.forEachSeriesId([&](const SeriesId128& id) {
        any = true;
        pure = pure && timestar::virtualShard(id) == vshard;
    });
    return any && pure;
}

seastar::future<bool> validateSnapshotObjects(const timestar::VShardSnapshotManifest& manifest,
                                              const std::vector<seastar::shared_ptr<::TSM>>& files) {
    if (files.size() != manifest.dataExtents.size())
        co_return false;

    timestar::VShardExtentMap actualExtents;
    for (size_t i = 0; i < files.size(); ++i) {
        const auto& file = files[i];
        if (!file || file->hasTombstones())
            co_return false;
        bool pure = true;
        file->forEachSeriesId(
            [&](const SeriesId128& id) { pure = pure && timestar::virtualShard(id) == manifest.vshard.value(); });
        if (!pure)
            co_return false;
        co_await timestar::addTsmFileExtents(actualExtents, manifest.dataExtents[i].fileId, *file);
    }
    if (actualExtents.extents(manifest.vshard) != manifest.dataExtents)
        co_return false;
    co_return co_await timestar::verifyVShardSnapshot(manifest, files);
}

std::optional<std::vector<timestar::CatalogRecord>> validateSnapshotCatalog(
    timestar::VShardId vshard, std::string_view catalogBytes, std::string_view expectedHash,
    const std::vector<seastar::shared_ptr<::TSM>>& files) {
    if (!vshard.valid() || timestar::SeriesCatalog::snapshotHash(
                               std::span<const char>(catalogBytes.data(), catalogBytes.size())) != expectedHash)
        return std::nullopt;
    auto catalog =
        timestar::SeriesCatalog::loadSnapshot(std::span<const char>(catalogBytes.data(), catalogBytes.size()));
    if (!catalog)
        return std::nullopt;

    std::set<SeriesId128> dataIds;
    for (const auto& file : files) {
        if (file)
            file->forEachSeriesId([&](const SeriesId128& id) { dataIds.insert(id); });
    }

    auto records = catalog->records();
    std::set<SeriesId128> catalogIds;
    for (const auto& record : records) {
        if (timestar::virtualShard(record.seriesId) != vshard.value())
            return std::nullopt;
        std::optional<TSMValueType> storedType;
        for (const auto& file : files) {
            const auto type = file->getSeriesType(record.seriesId);
            if (!type)
                continue;
            if (storedType && *storedType != *type)
                return std::nullopt;
            storedType = *type;
        }
        if (!storedType || *storedType != record.entry.valueType)
            return std::nullopt;
        std::map<std::string, std::string> tags(record.entry.tags.begin(), record.entry.tags.end());
        if (tags.size() != record.entry.tags.size())
            return std::nullopt;
        const auto canonicalId =
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey(record.entry.measurement, tags, record.entry.field));
        if (canonicalId != record.seriesId || !catalogIds.insert(record.seriesId).second)
            return std::nullopt;
    }
    if (catalogIds != dataIds)
        return std::nullopt;
    return records;
}

// Exact retry identity without a second snapshot-sized allocation. The payload
// bytes already live in the install coroutine; compare a resident object in
// small blocking-I/O chunks on a Seastar worker thread.
seastar::future<bool> fileMatchesBytes(std::string path, const std::string& expected) {
    co_return co_await seastar::async([path = std::move(path), &expected] {
        std::ifstream in(path, std::ios::binary);
        if (!in)
            return false;
        std::error_code ec;
        const auto size = fs::file_size(path, ec);
        if (ec || size != expected.size())
            return false;
        std::array<char, 64 * 1024> buffer{};
        size_t offset = 0;
        while (offset < expected.size()) {
            const size_t wanted = std::min(buffer.size(), expected.size() - offset);
            in.read(buffer.data(), static_cast<std::streamsize>(wanted));
            if (in.gcount() != static_cast<std::streamsize>(wanted) ||
                std::memcmp(buffer.data(), expected.data() + offset, wanted) != 0)
                return false;
            offset += wanted;
        }
        return in.peek() == std::ifstream::traits_type::eof();
    });
}

template <typename T>
seastar::future<std::vector<uint64_t>> snapshotSeriesTimestamps(const SeriesId128& id,
                                                                const std::vector<seastar::shared_ptr<::TSM>>& files) {
    std::vector<uint64_t> timestamps;
    for (const auto& file : files) {
        if (!file || file->getSeriesType(id) != timestar::valueTypeOf<T>())
            continue;
        TSMResult<T> result(file->dataRank());
        co_await file->readSeries<T>(id, 0, UINT64_MAX, result);
        auto [part, values] = result.getAllData();
        timestamps.insert(timestamps.end(), part.begin(), part.end());
    }
    co_return timestamps;
}
}  // namespace

seastar::future<bool> Engine::restoreVShardSnapshot(const timestar::VShardSnapshotManifest& manifest,
                                                    std::vector<std::string> sourcePaths,
                                                    std::vector<std::string> targetNames) {
    if (sourcePaths.size() != targetNames.size())
        throw std::invalid_argument("Engine::restoreVShardSnapshot: sourcePaths/targetNames size mismatch");
    if (!manifest.valid() || sourcePaths.size() != manifest.dataExtents.size())
        co_return false;

    std::vector<seastar::shared_ptr<::TSM>> files;
    bool ok = false;
    std::exception_ptr error;
    try {
        for (auto& p : sourcePaths)
            files.push_back(co_await openTsmForVShardOp(std::move(p)));

        // Bind every shipped object to the manifest, not just to the
        // resolved-value hash. The old verifier ignored unrelated series, so a
        // mixed tier-0 file (or a malicious extra file) could install another
        // VShard's data while still reproducing the target VShard's hash.
        // Snapshot objects are now required to be VShard-pure and their
        // revision extent must exactly match the descriptor.
        timestar::VShardExtentMap actualExtents;
        bool structurallyMatches = true;
        for (size_t i = 0; i < files.size(); ++i) {
            const auto& file = files[i];
            if (!file || file->hasTombstones()) {
                structurallyMatches = false;
                break;
            }
            for (const auto& sid : file->getSeriesIds()) {
                if (timestar::virtualShard(sid) != manifest.vshard.value()) {
                    structurallyMatches = false;
                    break;
                }
            }
            if (!structurallyMatches)
                break;
            co_await timestar::addTsmFileExtents(actualExtents, manifest.dataExtents[i].fileId, *file);
        }
        if (structurallyMatches)
            structurallyMatches = actualExtents.extents(manifest.vshard) == manifest.dataExtents;

        if (structurallyMatches) {
            std::vector<std::string> targets;
            targets.reserve(targetNames.size());
            for (const auto& name : targetNames)
                // StorageLayout validates that this is exactly one .tsm
                // basename; direct restore callers therefore cannot escape the
                // shard directory.
                targets.push_back(layout_.tsmFile(shardId, name).string());

            ok = co_await timestar::restoreVShardSnapshot(manifest, files, std::move(targets));
        }
    } catch (...) {
        error = std::current_exception();
    }
    for (const auto& file : files) {
        if (!file)
            continue;
        try {
            co_await file->close();
        } catch (...) {
            if (!error)
                error = std::current_exception();
        }
    }
    if (error)
        std::rethrow_exception(error);
    co_return ok;
}

seastar::future<EngineVShardSnapshot> Engine::buildVShardSnapshotFiles(timestar::VShardId vshard) {
    if (!vshard.valid())
        throw std::invalid_argument("buildVShardSnapshotFiles: invalid VShard");

    // Pin one immutable source generation. Shipping the raw source files is not
    // correct: tier-0 files are intentionally multiplexed across VShards and
    // tombstones live in sidecars that the old payload omitted. Materialise the
    // resolved view into ONE snapshot-only, VShard-pure TSM instead. This both
    // applies existing tombstones and prevents unrelated VShards crossing the
    // wire or being installed by a valid target hash.
    struct Source {
        seastar::shared_ptr<::TSM> file;
        uint64_t tombstoneGeneration;
    };
    std::vector<Source> pinned;
    std::vector<seastar::shared_ptr<::TSM>> inputs;
    uint64_t maxDataSeq = 0;
    for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
        if (!file)
            continue;
        pinned.push_back(Source{file, file->tombstoneGeneration()});
        inputs.push_back(file);
        maxDataSeq = std::max(maxDataSeq, file->dataSeq);
    }

    if (inputs.empty()) {
        timestar::SeriesCatalog catalog;
        std::string catalogBytes = catalog.snapshot();
        const std::string catalogHash = timestar::SeriesCatalog::snapshotHash(catalogBytes);
        timestar::VShardSnapshotBuilder empty(vshard);
        co_return EngineVShardSnapshot{empty.build({}, catalogHash), {}, std::move(catalogBytes)};
    }

    const uint64_t sequence = tsmFileManager.allocateSequenceId();
    const auto stageDir = layout_.tsmDir(shardId) / "snapout_tmp";
    const std::string name = "9_" + std::to_string(sequence) + "_d" + std::to_string(maxDataSeq) + ".tsm";
    const std::string path = (stageDir / name).string();
    co_await seastar::async([stageDir] { std::filesystem::create_directories(stageDir); });

    seastar::shared_ptr<::TSM> materialized;
    std::exception_ptr error;
    timestar::VShardSnapshotManifest manifest;
    std::string bytes;
    std::string catalogBytes;
    try {
        const size_t seriesWritten = co_await timestar::compactVShardToFile(vshard, inputs, path);
        if (seriesWritten == 0) {
            timestar::SeriesCatalog catalog;
            catalogBytes = catalog.snapshot();
            timestar::VShardSnapshotBuilder empty(vshard);
            manifest = empty.build({}, timestar::SeriesCatalog::snapshotHash(catalogBytes));
        } else {
            materialized = co_await openTsmForVShardOp(path);

            // Export exactly the series present in the resolved snapshot object,
            // not every index entry currently on the core. A concurrent newer
            // unflushed series belongs to the retained Raft suffix and must not
            // appear early in this snapshot's discovery state.
            const auto ids = materialized->getSeriesIds();
            auto metadata = co_await index.getSeriesMetadataBatch(ids);
            if (metadata.size() != ids.size())
                throw std::runtime_error("buildVShardSnapshotFiles: incomplete NativeIndex metadata batch");
            timestar::SeriesCatalog catalog;
            for (const auto& [id, maybeMetadata] : metadata) {
                if (!maybeMetadata)
                    throw std::runtime_error("buildVShardSnapshotFiles: data series " + id.toHex() +
                                             " has no NativeIndex metadata");
                const auto type = materialized->getSeriesType(id);
                if (!type)
                    throw std::runtime_error("buildVShardSnapshotFiles: data series " + id.toHex() +
                                             " has no value type");
                timestar::CatalogEntry entry;
                entry.measurement = maybeMetadata->measurement;
                entry.tags.assign(maybeMetadata->tags.begin(), maybeMetadata->tags.end());
                entry.field = maybeMetadata->field;
                entry.valueType = *type;
                const auto canonicalId = SeriesId128::fromSeriesKey(
                    timestar::buildSeriesKey(entry.measurement, maybeMetadata->tags, entry.field));
                if (canonicalId != id || !catalog.apply(timestar::CatalogRecord{id, std::move(entry)}))
                    throw std::runtime_error("buildVShardSnapshotFiles: conflicting/non-canonical catalog identity " +
                                             id.toHex());
            }
            if (catalog.size() != ids.size())
                throw std::runtime_error("buildVShardSnapshotFiles: duplicate/incomplete catalog identity set");
            catalogBytes = catalog.snapshot();
            timestar::VShardExtentMap extents;
            co_await timestar::addTsmFileExtents(extents, materialized->rankAsInteger(), *materialized);
            timestar::VShardSnapshotBuilder builder(vshard);
            co_await timestar::feedVShardResolvedView(vshard, {materialized}, builder);
            manifest = builder.build(extents, timestar::SeriesCatalog::snapshotHash(catalogBytes));
        }

        // A delete can append to a source sidecar while the materialiser is
        // suspended in DMA. Refuse that raced view. The Raft snapshot sweep will
        // retry; retaining more log is safe, truncating the delete is not.
        for (const auto& source : pinned) {
            if (source.file->tombstoneGeneration() != source.tombstoneGeneration)
                throw std::runtime_error("buildVShardSnapshotFiles: tombstone changed during materialisation");
        }

        if (seriesWritten != 0) {
            seastar::sstring raw = co_await seastar::util::read_entire_file_contiguous(path);
            bytes.assign(raw.data(), raw.size());
        }
    } catch (...) {
        error = std::current_exception();
    }
    if (materialized) {
        try {
            co_await materialized->close();
        } catch (...) {
            if (!error)
                error = std::current_exception();
        }
    }
    co_await seastar::async([path] {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    });
    if (error)
        std::rethrow_exception(error);

    std::vector<std::pair<std::string, std::string>> files;
    if (!bytes.empty())
        files.emplace_back(name, std::move(bytes));
    co_return EngineVShardSnapshot{std::move(manifest), std::move(files), std::move(catalogBytes)};
}

seastar::future<Engine::SnapshotInstallDisposition> Engine::classifySnapshotInstall(
    const timestar::VShardSnapshotManifest& manifest, const std::vector<std::pair<std::string, std::string>>& files) {
    // This is the compatibility classifier for the data-only low-level install
    // API. It has no catalog transaction and therefore remains fresh-target
    // only. Production uses installVShardSnapshotBundleOwned(), which performs
    // the complete live data+catalog generation replacement.
    for (const auto& store : walFileManager.pinMemoryStores()) {
        if (!store)
            continue;
        for (auto it = store->series.begin(); it != store->series.end(); ++it) {
            if (timestar::virtualShard(it.key()) == manifest.vshard.value())
                co_return SnapshotInstallDisposition::Reject;
        }
    }

    std::vector<seastar::shared_ptr<::TSM>> residentFiles;
    for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
        if (!file)
            continue;
        bool containsTarget = false;
        bool containsOther = false;
        for (const auto& sid : file->getSeriesIds()) {
            if (timestar::virtualShard(sid) == manifest.vshard.value())
                containsTarget = true;
            else
                containsOther = true;
        }
        if (!containsTarget)
            continue;
        if (containsOther || file->hasTombstones())
            co_return SnapshotInstallDisposition::Reject;
        residentFiles.push_back(file);
    }

    if (!residentFiles.empty()) {
        // Raft may replay after file publication but before catalog/index
        // reconstruction. Values plus an aggregate revision range are not
        // enough for identity: different block ranges can change later LWW.
        if (residentFiles.size() != files.size())
            co_return SnapshotInstallDisposition::Reject;
        seastar::sstring residentBytes =
            co_await seastar::util::read_entire_file_contiguous(residentFiles.front()->getFilePath());
        if (residentBytes.size() != files.front().second.size() ||
            !std::equal(residentBytes.begin(), residentBytes.end(), files.front().second.begin()))
            co_return SnapshotInstallDisposition::Reject;

        timestar::VShardExtentMap residentExtents;
        co_await timestar::addTsmFileExtents(residentExtents, manifest.dataExtents.front().fileId,
                                             *residentFiles.front());
        if (residentExtents.extents(manifest.vshard) != manifest.dataExtents)
            co_return SnapshotInstallDisposition::Reject;
        const bool verified = co_await timestar::verifyVShardSnapshot(manifest, residentFiles);
        co_return verified ? SnapshotInstallDisposition::Idempotent : SnapshotInstallDisposition::Reject;
    }

    // Data-free is not state-free: an earlier generation may have index entries
    // for a fully deleted or not-yet-flushed series. Merging that derived state
    // is not safe without a generation swap either.
    auto indexed = co_await index.extractVShardSeriesMetadata(manifest.vshard.value());
    co_return indexed.empty() ? SnapshotInstallDisposition::Fresh : SnapshotInstallDisposition::Reject;
}

seastar::future<bool> Engine::installVShardSnapshotFiles(timestar::VShardSnapshotManifest manifest,
                                                         std::vector<std::pair<std::string, std::string>> files) {
    auto owned = std::make_shared<const timestar::VShardSnapshotManifest>(std::move(manifest));
    return installVShardSnapshotFilesOwned(std::move(owned), std::move(files));
}

seastar::future<bool> Engine::installVShardSnapshotFilesOwned(
    std::shared_ptr<const timestar::VShardSnapshotManifest> manifestOwner,
    std::vector<std::pair<std::string, std::string>> files) {
    const auto& manifest = *manifestOwner;
    if (!manifest.valid() || files.size() != manifest.dataExtents.size())
        co_return false;

    // The current producer deliberately materialises one immutable object. The
    // lower-level restore helper publishes multiple targets with successive
    // renames, which is not crash-atomic. Keep this compatibility API aligned
    // with the production wire format's current one-object bound.
    if (files.size() > 1)
        co_return false;

    const auto disposition = co_await classifySnapshotInstall(manifest, files);
    if (disposition == SnapshotInstallDisposition::Reject)
        co_return false;
    if (disposition == SnapshotInstallDisposition::Idempotent)
        co_return true;

    const auto tsmDir = layout_.tsmDir(shardId);
    // Stage each shipped file under a receiver-allocated valid TSM basename.
    // A per-operation subdirectory prevents concurrent VShard installs on this
    // core from clearing one another's files. restoreVShardSnapshot copies the
    // staged source to its live target, so the exact subdirectory is disposable.
    const uint64_t installNonce = tsmFileManager.allocateSequenceId();
    const auto stageDir = tsmDir / "snapin_tmp" /
                          (std::to_string(manifest.vshard.value()) + "-" + std::to_string(manifest.snapshotRevision) +
                           "-" + std::to_string(installNonce));
    co_await seastar::async([stageDir] { std::filesystem::create_directories(stageDir); });
    bool ok = false;
    std::exception_ptr err;
    std::vector<std::string> tempPaths;
    std::vector<std::string> names;
    try {
        tempPaths.reserve(files.size());
        names.reserve(files.size());
        std::set<std::string> wireNames;
        for (auto& [wireName, bytes] : files) {
            const std::filesystem::path wirePath(wireName);
            if (wireName.empty() || wirePath.has_parent_path() || wirePath.filename() != wirePath ||
                wirePath.extension() != ".tsm" || !wireNames.insert(wireName).second) {
                ok = false;
                break;
            }
            // Never use a peer-supplied name as a live path. Allocate a local
            // immutable rank, with dataSeq equal to that rank so the next local
            // flush is unambiguously newer. This also removes cross-node rank
            // collisions from the install path.
            const uint64_t localSeq = tsmFileManager.allocateSequenceId();
            const std::string name = layout_.compactedTsmFile(shardId, 9, localSeq, localSeq).filename().string();
            const std::string tmp = (stageDir / name).string();
            co_await seastar::async([&tmp, &bytes] {
                std::ofstream o(tmp, std::ios::binary | std::ios::trunc);
                if (!o)
                    throw std::runtime_error("installVShardSnapshotFiles: cannot open temp " + tmp);
                o.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
                o.flush();
                if (!o)
                    throw std::runtime_error("installVShardSnapshotFiles: write failed for " + tmp);
            });
            tempPaths.push_back(tmp);
            names.push_back(name);
        }
        if (names.size() == files.size())
            ok = co_await restoreVShardSnapshot(manifest, tempPaths, names);
        if (ok)
            // restoreVShardSnapshot only writes the files to disk; a running Engine
            // does not see them until they are registered in the TSMFileManager (this
            // is what Engine::init does for pre-existing files). Without this, an
            // installed snapshot is invisible to queries. Open each installed target
            // (open + sparse index) and hand it to the manager.
            for (const auto& name : names) {
                auto tsm = co_await openTsmForVShardOp((tsmDir / name).string());
                co_await tsmFileManager.addTSMFile(tsm);
            }
    } catch (...) {
        err = std::current_exception();
    }
    // Always clean up the staging dir (restore copied, not moved).
    co_await seastar::async([stageDir] {
        std::error_code ec;
        std::filesystem::remove_all(stageDir, ec);
    });
    if (err)
        std::rethrow_exception(err);
    co_return ok;
}

seastar::future<bool> Engine::installVShardSnapshotBundle(timestar::VShardSnapshotManifest manifest,
                                                          std::vector<std::pair<std::string, std::string>> files,
                                                          std::string catalog) {
    auto owned = std::make_shared<const timestar::VShardSnapshotManifest>(std::move(manifest));
    return installVShardSnapshotBundleOwned(std::move(owned), std::move(files), std::move(catalog));
}

seastar::future<bool> Engine::installVShardSnapshotBundleOwned(
    std::shared_ptr<const timestar::VShardSnapshotManifest> manifestOwner,
    std::vector<std::pair<std::string, std::string>> files, std::string catalogBytes) {
    auto installUnits = co_await seastar::get_units(_snapshotInstallSemaphore, 1);
    const auto& manifest = *manifestOwner;

    bool rejected = false;
    bool installed = false;
    bool compactionWasRunning = false;
    std::exception_ptr error;
    std::optional<WALFileManager::SnapshotQuiesce> walQuiesce;
    std::vector<seastar::shared_ptr<::TSM>> stagedFiles;
    std::optional<std::vector<timestar::CatalogRecord>> catalogRecords;

    const auto tsmDir = layout_.tsmDir(shardId);
    fs::path stageDir;
    try {
        if (!manifest.valid() || files.size() != manifest.dataExtents.size() || files.size() > 1) {
            rejected = true;
        }

        // Validate peer-controlled names before creating anything. The names
        // are wire metadata only; live names are receiver-allocated below.
        std::set<std::string> wireNames;
        if (!rejected) {
            for (const auto& [wireName, bytes] : files) {
                (void)bytes;
                const fs::path wirePath(wireName);
                if (wireName.empty() || wirePath.has_parent_path() || wirePath.filename() != wirePath ||
                    wirePath.extension() != ".tsm" || !wireNames.insert(wireName).second) {
                    rejected = true;
                    break;
                }
            }
        }

        if (!rejected) {
            const uint64_t stageNonce = tsmFileManager.allocateSequenceId();
            stageDir = tsmDir / "snapin_tmp" /
                       (std::to_string(manifest.vshard.value()) + "-" + std::to_string(manifest.snapshotRevision) +
                        "-" + std::to_string(stageNonce));
            co_await seastar::async([stageDir] { fs::create_directories(stageDir); });

            for (auto& [wireName, bytes] : files) {
                (void)wireName;
                // TSM parses rank from its basename, including for staged
                // files, so give the stage a locally valid identity. A fresh
                // live identity is allocated only after orphan reconciliation.
                const uint64_t stageSeq = tsmFileManager.allocateSequenceId();
                const auto stagedPath = stageDir / layout_.compactedTsmFile(shardId, 9, stageSeq, stageSeq).filename();
                co_await seastar::async([stagedPath, &bytes] {
                    std::ofstream out(stagedPath, std::ios::binary | std::ios::trunc);
                    if (!out)
                        throw std::runtime_error("installVShardSnapshotBundle: cannot open stage " +
                                                 stagedPath.string());
                    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
                    out.flush();
                    if (!out)
                        throw std::runtime_error("installVShardSnapshotBundle: write failed for " +
                                                 stagedPath.string());
                });
                stagedFiles.push_back(co_await openTsmForVShardOp(stagedPath.string()));
            }

            if (!co_await validateSnapshotObjects(manifest, stagedFiles)) {
                rejected = true;
            } else {
                catalogRecords =
                    validateSnapshotCatalog(manifest.vshard, catalogBytes, manifest.catalogHash, stagedFiles);
                rejected = !catalogRecords.has_value();
            }
        }

        if (!rejected) {
            // From here onward every incoming byte and catalog identity is
            // proven. Stop the only other mutator of the TSM manager before
            // reconciling old and incoming immutable generations.
            compactionWasRunning = tsmFileManager.compactionLoopEnabled();
            if (compactionWasRunning)
                co_await tsmFileManager.stopCompactionLoop();

            const uint16_t vshard = manifest.vshard.value();
            walQuiesce.emplace(co_await walFileManager.quiesceForVShardSnapshot(vshard));

            // A prior attempt can die after publishing a root .tsm name but
            // before registering it. Reconcile every such object now: ignoring
            // a non-matching orphan would make it reappear as live old state on
            // restart. The directory barrier is repeated before registration,
            // making rename-success/sync-failure retries durable and idempotent.
            std::set<std::string> registeredNames;
            for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
                (void)rank;
                if (file)
                    registeredNames.insert(fs::path(file->getFilePath()).filename().string());
            }
            auto rootTsmPaths = co_await seastar::async([tsmDir] {
                std::vector<std::string> paths;
                for (const auto& entry : fs::directory_iterator(tsmDir)) {
                    if (entry.path().extension() != ".tsm")
                        continue;
                    if (!fs::is_regular_file(entry.symlink_status()))
                        throw std::runtime_error("Non-regular TSM snapshot-install entry: " + entry.path().string());
                    paths.push_back(entry.path().string());
                }
                std::sort(paths.begin(), paths.end());
                return paths;
            });
            bool haveUnregistered = false;
            for (const auto& path : rootTsmPaths) {
                if (!registeredNames.contains(fs::path(path).filename().string())) {
                    haveUnregistered = true;
                    break;
                }
            }
            if (haveUnregistered)
                co_await tsmFileManager.syncPublishedDirectory(tsmDir.string());
            for (auto& path : rootTsmPaths) {
                const auto name = fs::path(path).filename().string();
                if (registeredNames.contains(name))
                    continue;
                auto orphan = co_await openTsmForVShardOp(path);
                co_await tsmFileManager.addTSMFile(orphan);
                registeredNames.insert(std::move(name));
            }

            std::set<SeriesId128> catalogIds;
            for (const auto& record : *catalogRecords)
                catalogIds.insert(record.seriesId);

            // Find the exact immutable object from an interrupted earlier
            // attempt. Raw byte identity is intentional: logical values plus a
            // min/max revision extent do not prove identical per-point revision
            // placement, which controls later LWW against the retained suffix.
            seastar::shared_ptr<::TSM> incoming;
            if (!files.empty()) {
                std::vector<seastar::shared_ptr<::TSM>> resident;
                for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
                    (void)rank;
                    if (file)
                        resident.push_back(file);
                }
                for (const auto& file : resident) {
                    if (file->hasTombstones() || !isPureVShardObject(*file, vshard))
                        continue;
                    std::set<SeriesId128> fileIds;
                    file->forEachSeriesId([&](const SeriesId128& id) { fileIds.insert(id); });
                    if (fileIds != catalogIds || !co_await fileMatchesBytes(file->getFilePath(), files.front().second))
                        continue;
                    if (!incoming || file->dataRank() > incoming->dataRank())
                        incoming = file;
                }
            }

            // Durable old-generation fences. WAL first ensures a crash can
            // never recover an untombstoned memory generation after the TSM
            // side has moved forward. The quiesce holder prevents a rolled
            // store from publishing target data between these two barriers.
            co_await walFileManager.deleteVShardFromMemoryStores(vshard);
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::WalGenerationDeleted);

            std::vector<seastar::shared_ptr<::TSM>> currentFiles;
            std::vector<seastar::shared_ptr<::TSM>> supersededPureFiles;
            for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
                (void)rank;
                if (file) {
                    currentFiles.push_back(file);
                    if (file != incoming && isPureVShardObject(*file, vshard))
                        supersededPureFiles.push_back(file);
                }
            }
            for (const auto& file : currentFiles) {
                if (file != incoming && containsVShard(*file, vshard))
                    co_await file->deleteVShard(vshard);
            }
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::TsmGenerationDeleted);

            if (!files.empty() && !incoming) {
                const uint64_t localSeq = tsmFileManager.allocateSequenceId();
                const auto liveName = layout_.compactedTsmFile(shardId, 9, localSeq, localSeq).filename();
                const auto livePath = tsmDir / liveName;
                const bool restored = co_await timestar::restoreVShardSnapshot(
                    manifest, stagedFiles, std::vector<std::string>{livePath.string()});
                if (!restored)
                    throw std::runtime_error("installVShardSnapshotBundle: prevalidated data failed publication");
                // restore_vshard performs mandatory file+directory fsyncs. Repeat
                // the manager's injectable barrier before exposing the object;
                // a failure leaves an exact root orphan that the retry above
                // discovers, syncs, and registers.
                co_await tsmFileManager.syncPublishedDirectory(tsmDir.string());
                incoming = co_await openTsmForVShardOp(livePath.string());
                co_await tsmFileManager.addTSMFile(incoming);
            }
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::DataPublished);

            // Primary discovery/catalog state becomes the exact incoming set.
            // Evict every target binding, including retained identities: a
            // snapshot can legitimately carry a newer type binding for the same
            // canonical series ID after a full delete/recreate.
            co_await index.removeVShardSeriesMetadataExcept(vshard, catalogIds);
            std::erase_if(_seriesTypeCache,
                          [vshard](const auto& entry) { return timestar::virtualShard(entry.first) == vshard; });
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::CatalogPruned);

            std::vector<MetadataOp> ops;
            ops.reserve(catalogRecords->size());
            for (const auto& record : *catalogRecords) {
                MetadataOp op;
                op.valueType = record.entry.valueType;
                op.measurement = record.entry.measurement;
                op.fieldName = record.entry.field;
                op.tags = std::map<std::string, std::string>(record.entry.tags.begin(), record.entry.tags.end());
                op.seriesId = record.seriesId;
                ops.push_back(std::move(op));
            }
            co_await index.indexMetadataBatch(ops);
            std::vector<seastar::shared_ptr<::TSM>> snapshotFiles;
            if (incoming)
                snapshotFiles.push_back(incoming);
            for (const auto& record : *catalogRecords) {
                co_await index.putSeriesValueType(record.seriesId, record.entry.valueType);
                cacheSeriesType(record.seriesId, record.entry.valueType);
                std::vector<uint64_t> timestamps;
                switch (record.entry.valueType) {
                    case TSMValueType::Float:
                        timestamps = co_await snapshotSeriesTimestamps<double>(record.seriesId, snapshotFiles);
                        break;
                    case TSMValueType::Boolean:
                        timestamps = co_await snapshotSeriesTimestamps<bool>(record.seriesId, snapshotFiles);
                        break;
                    case TSMValueType::String:
                        timestamps = co_await snapshotSeriesTimestamps<std::string>(record.seriesId, snapshotFiles);
                        break;
                    case TSMValueType::Integer:
                        timestamps = co_await snapshotSeriesTimestamps<int64_t>(record.seriesId, snapshotFiles);
                        break;
                }
                co_await index.recordInsertDays(record.entry.measurement, record.seriesId, timestamps);
            }
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::CatalogInstalled);

            // A snapshot object is written at the terminal tier, so ordinary
            // tier compaction will never reclaim an older pure snapshot. Once
            // the replacement data and catalog are complete, retire those
            // fully superseded objects synchronously. Mixed legacy objects
            // stay registered with their durable VShard tombstone so their
            // unrelated data remains live and the tombstone-rewrite sweeper
            // can later materialise them safely.
            co_await tsmFileManager.removeTSMFiles(supersededPureFiles);
            if (_snapshotInstallCheckpointHook)
                _snapshotInstallCheckpointHook(SnapshotInstallCheckpoint::SupersededObjectsRetired);

            // Release quiescence before acquiring the rollover semaphore again.
            // The VShard's Raft apply isolation still prevents a target suffix
            // entry from landing until this method returns.
            walQuiesce.reset();
            co_await walFileManager.forceRolloverMemoryStore();
            installed = true;
        }
    } catch (...) {
        error = std::current_exception();
    }

    walQuiesce.reset();

    // Restore background compaction even after an injected failure. It was
    // fully drained before the mutation, so restarting here cannot race any
    // cleanup below (the stage directory is outside the manager's root scan).
    if (compactionWasRunning) {
        try {
            co_await tsmFileManager.startCompactionLoop();
        } catch (...) {
            if (!error)
                error = std::current_exception();
        }
    }

    for (const auto& staged : stagedFiles) {
        if (!staged)
            continue;
        try {
            co_await staged->close();
        } catch (...) {
            if (!error)
                error = std::current_exception();
        }
    }
    if (!stageDir.empty()) {
        try {
            co_await seastar::async([stageDir] {
                std::error_code ec;
                fs::remove_all(stageDir, ec);
                if (ec)
                    throw std::system_error(ec, "remove snapshot stage " + stageDir.string());
            });
        } catch (...) {
            if (!error)
                error = std::current_exception();
        }
    }

    if (error)
        std::rethrow_exception(error);
    co_return !rejected && installed;
}

seastar::future<bool> Engine::installVShardSnapshotCatalog(timestar::VShardId vshard, std::string catalogBytes,
                                                           std::string expectedHash) {
    if (!vshard.valid() || timestar::SeriesCatalog::snapshotHash(catalogBytes) != expectedHash)
        co_return false;
    auto catalog =
        timestar::SeriesCatalog::loadSnapshot(std::span<const char>(catalogBytes.data(), catalogBytes.size()));
    if (!catalog)
        co_return false;
    const auto records = catalog->records();

    // Snapshot the newly-installed pure files and require exact agreement
    // between their series identities and the catalog before mutating the
    // index. This prevents both undiscoverable data and catalog-only phantom
    // series from being accepted as a complete snapshot.
    std::vector<seastar::shared_ptr<::TSM>> files;
    std::set<SeriesId128> dataIds;
    for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
        if (!file)
            continue;
        bool belongs = false;
        bool containsOther = false;
        for (const auto& id : file->getSeriesIds()) {
            if (timestar::virtualShard(id) == vshard.value()) {
                belongs = true;
                dataIds.insert(id);
            } else
                containsOther = true;
        }
        if (belongs) {
            if (containsOther || file->hasTombstones())
                co_return false;
            files.push_back(file);
        }
    }
    std::set<SeriesId128> catalogIds;
    std::vector<MetadataOp> ops;
    ops.reserve(records.size());
    for (const auto& record : records) {
        if (timestar::virtualShard(record.seriesId) != vshard.value())
            co_return false;
        std::optional<TSMValueType> storedType;
        for (const auto& file : files) {
            const auto type = file->getSeriesType(record.seriesId);
            if (!type)
                continue;
            if (storedType && *storedType != *type)
                co_return false;
            storedType = *type;
        }
        if (!storedType || *storedType != record.entry.valueType)
            co_return false;
        std::map<std::string, std::string> tags(record.entry.tags.begin(), record.entry.tags.end());
        if (tags.size() != record.entry.tags.size())
            co_return false;
        const auto canonicalId =
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey(record.entry.measurement, tags, record.entry.field));
        if (canonicalId != record.seriesId || !catalogIds.insert(record.seriesId).second)
            co_return false;
        MetadataOp op;
        op.valueType = record.entry.valueType;
        op.measurement = record.entry.measurement;
        op.fieldName = record.entry.field;
        op.tags = std::move(tags);
        op.seriesId = record.seriesId;
        ops.push_back(std::move(op));
    }
    if (catalogIds != dataIds)
        co_return false;

    // Rebuild the derived NativeIndex state only after the full catalog has
    // passed structural/hash/data agreement checks. Exact day membership is
    // recovered from the installed TSM timestamps; using only min/max would
    // create false positives, while omitting days makes older snapshot series
    // disappear as soon as a later write creates the first day bitmap.
    co_await index.indexMetadataBatch(ops);
    for (const auto& record : records) {
        co_await index.putSeriesValueType(record.seriesId, record.entry.valueType);
        std::vector<uint64_t> timestamps;
        switch (record.entry.valueType) {
            case TSMValueType::Float:
                timestamps = co_await snapshotSeriesTimestamps<double>(record.seriesId, files);
                break;
            case TSMValueType::Boolean:
                timestamps = co_await snapshotSeriesTimestamps<bool>(record.seriesId, files);
                break;
            case TSMValueType::String:
                timestamps = co_await snapshotSeriesTimestamps<std::string>(record.seriesId, files);
                break;
            case TSMValueType::Integer:
                timestamps = co_await snapshotSeriesTimestamps<int64_t>(record.seriesId, files);
                break;
        }
        co_await index.recordInsertDays(record.entry.measurement, record.seriesId, timestamps);
    }
    co_return true;
}

seastar::future<size_t> Engine::migrateVShard(timestar::VShardId vshard, std::vector<std::string> sourcePaths,
                                              std::string outputName) {
    std::vector<seastar::shared_ptr<::TSM>> files;
    for (auto& p : sourcePaths)
        files.push_back(co_await openTsmForVShardOp(std::move(p)));

    const std::string out = (layout_.tsmDir(shardId) / outputName).string();
    const size_t n = co_await timestar::migrateVShardToFile(vshard, files, out);
    for (const auto& f : files)
        co_await f->close();
    co_return n;
}

seastar::future<std::vector<std::pair<timestar::VShardId, std::string>>> Engine::repartitionByVShard() {
    std::vector<seastar::shared_ptr<::TSM>> files;
    for (const auto& [rank, file] : tsmFileManager.getSequencedTsmFiles()) {
        if (file)
            files.push_back(file);
    }
    const auto tsmDir = layout_.tsmDir(shardId);
    auto pathFor = [tsmDir](timestar::VShardId vs) {
        char name[64];
        std::snprintf(name, sizeof(name), "09_%010u.tsm", static_cast<unsigned>(vs.value()));
        return (tsmDir / name).string();
    };
    co_return co_await timestar::partitionByVShard(std::move(files), pathFor);
}

void Engine::restoreRevisionCounter() {
    uint64_t maxRev = 0;
    // WAL-replayed memory (the un-flushed tail carries the highest revisions).
    for (const auto& store : walFileManager.pinMemoryStores()) {
        if (!store)
            continue;
        for (auto it = store->series.begin(); it != store->series.end(); ++it) {
            std::visit(
                [&](const auto& s) {
                    for (uint64_t r : s.revisions)
                        if (r > maxRev)
                            maxRev = r;
                },
                it.value());
        }
    }
    // Flushed data: the file-level max-revision trailer (cheap, no full-index load).
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        if (tsmFile && tsmFile->maxRevision() > maxRev)
            maxRev = tsmFile->maxRevision();
    }
    nextRevision_ = maxRev + 1;  // == kFirstAssignedRevision (1) when nothing is tracked

    // Tracking is STICKY: if any durable data already carries a revision, this
    // store is revision-tracked, so keep assigning -- otherwise new writes would
    // land at the migrated floor (0) and LOSE the LWW to recovered data. This
    // makes enablement a property of the data, not a runtime flag that a restart
    // could forget (adversarial-review finding).
    if (maxRev > 0)
        assignRevisions_ = true;
}

template <class T>
seastar::future<> Engine::insert(TimeStarInsert<T> insertRequest, bool skipMetadataIndexing) {
    auto holder = _insertGate.hold();

    rejectIfIngestBacklogged();

    ++_metrics.inserts_total;
    _metrics.insert_points_total += insertRequest.values.size();

    LOG_INSERT_PATH(timestar::engine_log, debug,
                    "[ENGINE] Insert called for series: '{}', measurement: '{}', field: '{}', {} values",
                    insertRequest.seriesKey(), insertRequest.measurement, insertRequest.field,
                    insertRequest.values.size());

    // Enforce the series type binding here too. This overload has no production
    // callers (everything routes through insertBatch), but leaving a public
    // entry point unguarded is how the invariant gets lost later.
    {
        std::vector<TimeStarInsert<T>> one;
        one.push_back(std::move(insertRequest));
        auto kept = co_await enforceSeriesTypes<T>(std::move(one));
        if (kept.empty()) {
            // Converted into the bound type and already stored via insertBatch.
            co_return;
        }
        insertRequest = std::move(kept.front());
    }

    // Index metadata locally — each shard maintains its own NativeIndex
    // for the series it owns. Schema changes are broadcast via indexMetadataSync.
    if (!skipMetadataIndexing) {
        LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Indexing metadata locally for series: '{}'",
                        insertRequest.seriesKey());
        co_await index.indexInsert(insertRequest);
    }

    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Processing data storage for series: '{}'",
                    insertRequest.seriesKey());

    // Notify streaming subscribers BEFORE WAL insert, because the WAL+MemoryStore
    // path moves data out of the insert (insertMemory takes by rvalue).
    if (_subscriptionManager.hasSubscribers(insertRequest.measurement)) {
        auto remotes = _subscriptionManager.notifySubscribers(insertRequest.measurement, insertRequest.getTags(),
                                                              insertRequest.field, insertRequest.getTimestamps(),
                                                              insertRequest.values);
        for (auto& rd : remotes) {
            if (shardedRef) {
                (void)seastar::with_gate(_streamingGate, [this, rd = std::move(rd)]() mutable {
                    return shardedRef
                        ->invoke_on(rd.targetShard,
                                    [subId = rd.subscriptionId, b = std::move(rd.batch)](Engine& engine) mutable {
                                        engine.getSubscriptionManager().deliverBatch(subId, std::move(b));
                                        return seastar::make_ready_future<>();
                                    })
                        .handle_exception([](std::exception_ptr ep) {
                            try {
                                std::rethrow_exception(ep);
                            } catch (const std::exception& e) {
                                timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: {}", e.what());
                            } catch (...) {
                                timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: unknown error");
                            }
                        });
                }).handle_exception([](std::exception_ptr) {
                    // Gate is closed (shutting down) — delivery silently dropped, which is correct
                });
            }
        }
    }

    // Assign the replicated revision AFTER type enforcement (so a rejected write
    // consumes no revision) and BEFORE the durable WAL+MemoryStore write, so the
    // WAL persists it and the memory store tracks it (cluster LWW).
    stampRevisions(insertRequest);

    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Starting WAL insert for single series");
    co_await walFileManager.insert(insertRequest);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] WAL insert completed for single series");
}

seastar::future<std::optional<TSMValueType>> Engine::resolveSeriesType(const SeriesId128& seriesId) {
    // 1. Hot path: shard-local cache, one hash lookup, no suspension.
    if (auto it = _seriesTypeCache.find(seriesId); it != _seriesTypeCache.end()) {
        co_return it->second;
    }

    // 2. Synchronous oracles. Same probe order as isNonNumericSeries(): memory
    //    first because it is a pure hash lookup, then TSM (bloom + sparse
    //    index, no disk I/O). A series is exactly one type per source, so the
    //    first source that knows it decides.
    if (auto t = walFileManager.getSeriesType(seriesId)) {
        cacheSeriesType(seriesId, *t);
        co_return t;
    }
    if (auto t = tsmFileManager.getSeriesType(seriesId)) {
        cacheSeriesType(seriesId, *t);
        co_return t;
    }

    // 3. Durable binding. Only reached for a series neither memory nor TSM can
    //    see — e.g. one written on a previous boot whose data has since aged
    //    out. Paying an LSM read solely here keeps the hot path suspension-free.
    if (auto t = co_await index.getSeriesValueType(seriesId)) {
        cacheSeriesType(seriesId, *t);
        co_return t;
    }

    // 4. Genuinely unknown: first write for this series.
    co_return std::nullopt;
}

seastar::future<> Engine::bindSeriesType(const SeriesId128& seriesId, TSMValueType type) {
    co_await index.putSeriesValueType(seriesId, type);
    cacheSeriesType(seriesId, type);
}

template <class T>
seastar::future<std::vector<TimeStarInsert<T>>> Engine::enforceSeriesTypes(std::vector<TimeStarInsert<T>> requests,
                                                                           bool enforceAdmission) {
    constexpr TSMValueType kIncoming = timestar::valueTypeOf<T>();

    std::vector<TimeStarInsert<T>> kept;
    kept.reserve(requests.size());

    // Requests bound to some other type, grouped by that type. Converted and
    // re-inserted below.
    std::vector<TimeStarInsert<double>> asDouble;
    std::vector<TimeStarInsert<bool>> asBool;
    std::vector<TimeStarInsert<std::string>> asString;
    std::vector<TimeStarInsert<int64_t>> asInteger;

    std::vector<std::string> rejections;

    // Bindings for series seen for the first time in THIS request. Held back
    // until the whole request is known to be acceptable, so a rejected request
    // leaves nothing behind — not even a binding. Consulted during the loop so
    // that two elements for the same new series agree on one type rather than
    // both claiming to be first.
    std::unordered_map<SeriesId128, TSMValueType, SeriesId128::Hash> pendingBinds;

    for (auto& req : requests) {
        const SeriesId128 seriesId = req.seriesId128();
        auto bound = co_await resolveSeriesType(seriesId);
        if (!bound.has_value()) {
            if (auto p = pendingBinds.find(seriesId); p != pendingBinds.end())
                bound = p->second;
        }

        if (!bound.has_value()) {
            // First write wins: this is what the series is from now on.
            pendingBinds.emplace(seriesId, kIncoming);
            kept.push_back(std::move(req));
            continue;
        }
        if (*bound == kIncoming) {
            kept.push_back(std::move(req));
            continue;
        }

        // Bound to a different type. Convert losslessly or reject the whole
        // series batch — part-accepting would leave the caller unable to tell
        // which points landed.
        auto convert = [&]<class U>() -> bool {
            TimeStarInsert<U> out(req.measurement, req.field);
            out.setSharedTags(std::make_shared<const std::map<std::string, std::string>>(req.getTags()));
            out.setSharedTimestamps(std::make_shared<const std::vector<uint64_t>>(req.getTimestamps()));
            out.setCachedSeriesId128(seriesId);
            out.values.reserve(req.values.size());
            for (size_t i = 0; i < req.values.size(); ++i) {
                // Bind to T explicitly: std::vector<bool> yields a proxy
                // reference, which would deduce the wrong source type.
                const T v = req.values[i];
                auto c = timestar::coerceValue<U>(v);
                if (!c.has_value()) {
                    rejections.push_back(
                        fmt::format("{} {}: series is bound to type '{}', refusing '{}' value {} "
                                    "(delete the series to re-type it)",
                                    req.measurement, req.field, timestar::valueTypeName(*bound),
                                    timestar::valueTypeName(kIncoming), timestar::describeValue(v)));
                    return false;
                }
                out.values.push_back(std::move(*c));
            }
            if constexpr (std::is_same_v<U, double>)
                asDouble.push_back(std::move(out));
            else if constexpr (std::is_same_v<U, bool>)
                asBool.push_back(std::move(out));
            else if constexpr (std::is_same_v<U, std::string>)
                asString.push_back(std::move(out));
            else
                asInteger.push_back(std::move(out));
            return true;
        };

        timestar::dispatchValueType(*bound, convert);
    }

    if (!rejections.empty()) {
        std::string msg = rejections.front();
        if (rejections.size() > 1)
            msg += fmt::format(" (and {} more type conflicts in this request)", rejections.size() - 1);
        throw std::invalid_argument(msg);
    }

    // Request is acceptable — now make the first-write bindings durable.
    for (const auto& [id, type] : pendingBinds) {
        co_await bindSeriesType(id, type);
    }

    // Re-enter for each converted group. These terminate: a converted batch
    // matches its binding by construction, so it takes the `kept` path.
    if constexpr (!std::is_same_v<T, double>)
        if (!asDouble.empty())
            co_await insertBatch<double>(std::move(asDouble), enforceAdmission);
    if constexpr (!std::is_same_v<T, bool>)
        if (!asBool.empty())
            co_await insertBatch<bool>(std::move(asBool), enforceAdmission);
    if constexpr (!std::is_same_v<T, std::string>)
        if (!asString.empty())
            co_await insertBatch<std::string>(std::move(asString), enforceAdmission);
    if constexpr (!std::is_same_v<T, int64_t>)
        if (!asInteger.empty())
            co_await insertBatch<int64_t>(std::move(asInteger), enforceAdmission);

    co_return kept;
}

template <class T>
seastar::future<WALTimingInfo> Engine::insertBatch(std::vector<TimeStarInsert<T>> insertRequests,
                                                   bool enforceAdmission) {
    auto holder = _insertGate.hold();

    if (insertRequests.empty()) {
        co_return WALTimingInfo{};  // No work to do
    }

    if (enforceAdmission)
        rejectIfIngestBacklogged();

    // Enforce the per-series type binding BEFORE anything durable happens, so a
    // rejected write leaves no WAL record and no memstore entry, and before
    // subscribers are notified, so a stream never sees a value the store
    // rejected. Recovery is unaffected: WALReader::readAll calls
    // MemoryStore::insertMemory directly and never reaches this method.
    insertRequests = co_await enforceSeriesTypes<T>(std::move(insertRequests), enforceAdmission);
    if (insertRequests.empty()) {
        co_return WALTimingInfo{};  // everything was converted into other types
    }

    // Update Prometheus metrics for batch inserts
    ++_metrics.inserts_total;
    for (const auto& req : insertRequests) {
        _metrics.insert_points_total += req.getTimestamps().size();
    }

    // Metadata indexing is now handled at the HTTP handler level on shard 0
    // This Engine method now only handles data storage (WAL + MemoryStore)
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Processing batch data storage for {} requests",
                    insertRequests.size());

    // Notify streaming subscribers BEFORE WAL insert, because the WAL+MemoryStore
    // path moves data out of the request elements (insertMemory takes by rvalue).
    // Fast-path: skip the entire loop when there are no subscribers at all.
    if (_subscriptionManager.subscriptionCount() > 0) {
        for (const auto& req : insertRequests) {
            if (_subscriptionManager.hasSubscribers(req.measurement)) {
                auto remotes = _subscriptionManager.notifySubscribers(req.measurement, req.getTags(), req.field,
                                                                      req.getTimestamps(), req.values);
                for (auto& rd : remotes) {
                    if (shardedRef) {
                        (void)seastar::with_gate(_streamingGate, [this, rd = std::move(rd)]() mutable {
                            return shardedRef
                                ->invoke_on(
                                    rd.targetShard,
                                    [subId = rd.subscriptionId, b = std::move(rd.batch)](Engine& engine) mutable {
                                        engine.getSubscriptionManager().deliverBatch(subId, std::move(b));
                                        return seastar::make_ready_future<>();
                                    })
                                .handle_exception([](std::exception_ptr ep) {
                                    try {
                                        std::rethrow_exception(ep);
                                    } catch (const std::exception& e) {
                                        timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: {}", e.what());
                                    } catch (...) {
                                        timestar::engine_log.warn(
                                            "[STREAM] Cross-shard delivery failed: unknown error");
                                    }
                                });
                        }).handle_exception([](std::exception_ptr) {
                            // Gate is closed (shutting down) — delivery silently dropped, which is correct
                        });
                    }
                }
            }
        }
    }

    // Assign the replicated revision to each request (one per batch element)
    // before the durable write, so WAL + MemoryStore both carry it (cluster LWW).
    for (auto& req : insertRequests) {
        stampRevisions(req);
    }

    // Use WAL file manager batch insert
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Starting unified WAL batch insert for {} requests",
                    insertRequests.size());

#if TIMESTAR_LOG_INSERT_PATH
    auto start_wal_batch = std::chrono::high_resolution_clock::now();
#endif
    co_await walFileManager.insertBatch(insertRequests);

    // Record day bitmaps for time-scoped discovery (0x0D postings). The batch
    // path previously never recorded them, so batch-only series were wrongly
    // pruned from time-scoped queries once any day bitmap existed for the
    // measurement. Skips per series when the LocalId doesn't exist yet (first
    // batch of a new series — covered by the MetadataOp day-span path).
    for (const auto& req : insertRequests) {
        co_await index.recordInsertDays(req.measurement, req.seriesId128(), req.getTimestamps());
    }

    // Create timing info
    WALTimingInfo walTiming;
#if TIMESTAR_LOG_INSERT_PATH
    auto end_wal_batch = std::chrono::high_resolution_clock::now();
    walTiming.walWriteTime = std::chrono::duration_cast<std::chrono::microseconds>(end_wal_batch - start_wal_batch);
#endif
    walTiming.walWriteCount = insertRequests.size();

    co_return walTiming;
}

template <class T>
seastar::future<SeriesId128> Engine::indexMetadata(TimeStarInsert<T> insertRequest) {
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Indexing metadata for series: '{}' on shard {}",
                    insertRequest.seriesKey(), shardId);
    SeriesId128 seriesId = co_await index.indexInsert(insertRequest);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Metadata indexed, series ID: {}", seriesId.toHex());
    co_return seriesId;
}

seastar::future<> Engine::indexMetadataBatch(const std::vector<MetadataOp>& ops) {
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Batch indexing metadata for {} ops on shard {}", ops.size(),
                    shardId);
    co_await index.indexMetadataBatch(ops);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Batch metadata indexing complete on shard {}", shardId);
}

seastar::future<> Engine::indexMetadataSync(std::vector<MetadataOp> metaOps) {
    if (metaOps.empty()) [[unlikely]] {
        co_return;
    }

    if (shardedRef == nullptr) [[unlikely]] {
        timestar::engine_log.warn("[METADATA] No shardedRef, dropping {} metadata ops", metaOps.size());
        co_return;
    }

    // Group MetadataOps by target shard based on series hash
    unsigned shardCount = seastar::smp::count;
    std::vector<std::vector<MetadataOp>> opsByShard(shardCount);

    for (auto& op : metaOps) {
        // The write handler pre-computes op.seriesId with the same hash used for
        // data routing, so metadata lands on the same shard as the data without
        // rebuilding + rehashing the series key here. Fall back to the canonical
        // buildSeriesKey + fromSeriesKey path only if a producer left it unset.
        if (op.seriesId.isZero()) [[unlikely]] {
            op.seriesId = SeriesId128::fromSeriesKey(timestar::buildSeriesKey(op.measurement, op.tags, op.fieldName));
        }
        unsigned targetShard = timestar::routeToCore(op.seriesId);
        opsByShard[targetShard].push_back(std::move(op));
    }

    // Dispatch each group to its owning shard IN PARALLEL (not sequential co_await).
    // Sequential dispatch was a critical scaling bottleneck: N cross-shard RPCs in series.
    std::vector<seastar::future<timestar::index::SchemaUpdate>> futures;
    futures.reserve(shardCount);
    for (unsigned s = 0; s < shardCount; ++s) {
        if (opsByShard[s].empty())
            continue;
        futures.push_back(shardedRef->invoke_on(
            s,
            [ops = std::move(opsByShard[s])](Engine& engine) mutable -> seastar::future<timestar::index::SchemaUpdate> {
                co_return co_await engine.index.indexMetadataBatchWithSchema(ops);
            }));
    }

    timestar::index::SchemaUpdate combined;
    if (!futures.empty()) {
        auto results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        for (auto& update : results) {
            combined.merge(update);
        }
    }

    // Broadcast schema changes — fire-and-forget (don't block write response).
    // Schema caches are eventually consistent; queries will see updates on next read.
    if (!combined.empty()) {
        (void)seastar::try_with_gate(_insertGate, [this, combined = std::move(combined)]() mutable {
            return broadcastSchemaUpdate(std::move(combined));
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // Shutting down — safe to discard.
            } catch (const std::exception& e) {
                timestar::engine_log.error("[METADATA] Schema broadcast failed: {}", e.what());
            }
        });
    }
}

seastar::future<> Engine::broadcastSchemaUpdate(timestar::index::SchemaUpdate update) {
    if (!shardedRef || update.empty())
        co_return;
    co_await shardedRef->invoke_on_all([update = std::move(update)](Engine& e) {
        // applySchemaUpdate is now a coroutine: it persists the deltas to the
        // receiving shard's KV store (complete schema replica per shard).
        return e.getIndex().applySchemaUpdate(update);
    });
}

seastar::future<std::optional<VariantQueryResult>> Engine::query(std::string series, uint64_t startTime,
                                                                 uint64_t endTime) {
    // Compute SeriesId128 once and pass through to avoid redundant SHA1 hashing
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(series);
    co_return co_await query(std::move(series), seriesId, startTime, endTime);
}

seastar::future<std::optional<VariantQueryResult>> Engine::query(std::string series, const SeriesId128& seriesId,
                                                                 uint64_t startTime, uint64_t endTime) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    try {
        co_return co_await runner.runQuery(series, seriesId, startTime, endTime);
    } catch (const SeriesNotFoundException&) {
        co_return std::nullopt;
    }
}

seastar::future<std::optional<timestar::PushdownResult>> Engine::queryAggregated(
    [[maybe_unused]] const std::string& seriesKey, const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
    uint64_t aggregationInterval, timestar::AggregationMethod method, bool foldNoInterval, bool boolLatestAsNumeric) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    co_return co_await runner.queryTsmAggregated(seriesId, startTime, endTime, aggregationInterval, method,
                                                 foldNoInterval, boolLatestAsNumeric);
}

std::optional<TSMValueType> Engine::localSeriesValueType(const SeriesId128& seriesId) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    return runner.localSeriesValueType(seriesId);
}

// True when a series is non-numeric (Boolean or String).  A series is exactly
// one type across every source, so the first source that knows it decides;
// memory is probed first because it is a pure hash lookup.
static bool isNonNumericSeries(const std::vector<seastar::shared_ptr<MemoryStore>>& pinnedStores,
                               const std::vector<seastar::shared_ptr<TSM>>& tsmFiles, const SeriesId128& seriesId) {
    for (const auto& store : pinnedStores) {
        if (auto type = store->getSeriesType(seriesId))
            return isNonNumericValueType(*type);
    }
    for (const auto& tsmFile : tsmFiles) {
        if (auto type = tsmFile->getSeriesType(seriesId))
            return isNonNumericValueType(*type);
    }
    return false;
}

seastar::future<> Engine::batchLatest(std::vector<BatchLatestEntry>& entries, uint64_t startTime, uint64_t endTime,
                                      bool wantFirst) {
    if (entries.empty())
        co_return;

    // Pin the memory stores BEFORE snapshotting the TSM file list (visibility
    // invariant, see WALFileManager::pinMemoryStores): background WAL->TSM
    // conversion registers the TSM file first and erases the store second, so
    // this ordering guarantees Phase 3 still sees the data of any store whose
    // conversion completes while Phase 2 is suspended on DMA reads.
    const auto pinnedStores = walFileManager.pinMemoryStores();

    // --- Phase 1: TSM sparse index scan (zero I/O) ---
    // Snapshot TSM files once, ordered by dataRank DESCENDING (newest write
    // generation first).  On equal timestamps the strict comparisons below
    // keep the first-visited copy, so a rewrite of a point always beats the
    // stale copy in an older-generation file (last-write-wins).
    std::vector<seastar::shared_ptr<TSM>> tsmFiles;
    tsmFiles.reserve(tsmFileManager.getSequencedTsmFiles().size());
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmFiles.push_back(tsmFile);
    }
    std::sort(tsmFiles.begin(), tsmFiles.end(),
              [](const seastar::shared_ptr<TSM>& a, const seastar::shared_ptr<TSM>& b) {
                  return a->dataRank() > b->dataRank();
              });

    // Non-numeric series (Boolean, String) never aggregate arithmetically and
    // must reach the caller in their written type, which BatchLatestEntry's
    // double cannot represent.  Leave them unresolved so the caller routes them
    // to the per-series path.
    std::vector<bool> skip(entries.size(), false);
    size_t skipCount = 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        if (isNonNumericSeries(pinnedStores, tsmFiles, entries[i].seriesId)) {
            skip[i] = true;
            ++skipCount;
        }
    }

    // Track how many series are still unresolved (drives Phase 2 only).
    size_t unresolvedCount = entries.size() - skipCount;

    // Every file's sparse stats must be consulted — resolving an entry from
    // one file must NOT stop the scan, because an out-of-order flush can put
    // the series' true latest (or first) point in any file.  The scan is pure
    // in-memory hash lookups, so this stays zero-I/O.
    for (const auto& tsmFile : tsmFiles) {
        if (tsmFile->hasTombstones())
            continue;

        for (size_t i = 0; i < entries.size(); ++i) {
            if (skip[i])
                continue;
            auto& entry = entries[i];
            auto pt =
                wantFirst ? tsmFile->getFirstFromSparse(entry.seriesId) : tsmFile->getLatestFromSparse(entry.seriesId);
            if (!pt.has_value())
                continue;
            if (pt->timestamp < startTime || pt->timestamp > endTime)
                continue;

            if (!entry.resolved) {
                entry.timestamp = pt->timestamp;
                entry.value = pt->value;
                entry.resolved = true;
                --unresolvedCount;
            } else if (wantFirst ? (pt->timestamp < entry.timestamp) : (pt->timestamp > entry.timestamp)) {
                entry.timestamp = pt->timestamp;
                entry.value = pt->value;
            }
        }
    }

    // --- Phase 2: If sparse index missed some (no extended stats or tombstones),
    // fall back to selective DMA reads for unresolved series. ---
    if (unresolvedCount > 0) {
        for (const auto& tsmFile : tsmFiles) {
            if (unresolvedCount == 0)
                break;
            for (size_t i = 0; i < entries.size(); ++i) {
                if (skip[i])
                    continue;
                auto& entry = entries[i];
                if (entry.resolved)
                    continue;
                if (!tsmFile->seriesMayOverlapTime(entry.seriesId, startTime, endTime))
                    continue;

                timestar::BlockAggregator agg(
                    0, startTime, endTime,
                    wantFirst ? timestar::AggregationMethod::FIRST : timestar::AggregationMethod::LATEST, true);
                agg.enableFoldToSingleState();
                size_t pts =
                    co_await tsmFile->aggregateSeriesSelective(entry.seriesId, startTime, endTime, agg, !wantFirst, 1);
                if (pts > 0) {
                    auto state = agg.takeSingleState();
                    if (wantFirst) {
                        entry.timestamp = state.firstTimestamp;
                        entry.value = state.first;
                    } else {
                        entry.timestamp = state.latestTimestamp;
                        entry.value = state.latest;
                    }
                    entry.resolved = true;
                    --unresolvedCount;
                }
            }
        }
    }

    // --- Phase 3: Memory stores (may have newer/older data than TSM) ---
    // Iterate stores in outer loop (fewer stores than entries) for better
    // cache locality on the memory store's internal hash map.
    // Last-write-wins: memory writes are newer than any flushed copy, so on
    // EQUAL timestamps the memory value must override the TSM value picked in
    // Phase 1/2 (>= and <= comparisons below).  Stores are iterated
    // oldest→newest (pinnedStores is newest-first) so that among stores the
    // newest store's copy overrides on ties as well.
    auto checkMemSeries = [&](const auto* series, auto& entry) {
        if (!series || series->timestamps.empty())
            return;
        if (!wantFirst) {
            // LATEST: check last element (timestamps are sorted ascending)
            auto it = std::upper_bound(series->timestamps.begin(), series->timestamps.end(), endTime);
            if (it == series->timestamps.begin())
                return;
            --it;
            if (*it < startTime)
                return;
            size_t idx = static_cast<size_t>(it - series->timestamps.begin());
            if (!entry.resolved || series->timestamps[idx] >= entry.timestamp) {
                entry.timestamp = series->timestamps[idx];
                entry.value = static_cast<double>(series->values[idx]);
                entry.resolved = true;
            }
        } else {
            // FIRST: check first element in range
            auto it = std::lower_bound(series->timestamps.begin(), series->timestamps.end(), startTime);
            if (it == series->timestamps.end() || *it > endTime)
                return;
            size_t idx = static_cast<size_t>(it - series->timestamps.begin());
            if (!entry.resolved || series->timestamps[idx] <= entry.timestamp) {
                entry.timestamp = series->timestamps[idx];
                entry.value = static_cast<double>(series->values[idx]);
                entry.resolved = true;
            }
        }
    };

    for (const auto& memStore : pinnedStores | std::views::reverse) {
        for (size_t i = 0; i < entries.size(); ++i) {
            if (skip[i])
                continue;
            // Bool series are always skipped above (non-numeric), so only the
            // two numeric types are probed here.
            checkMemSeries(memStore->querySeries<double>(entries[i].seriesId), entries[i]);
            checkMemSeries(memStore->querySeries<int64_t>(entries[i].seriesId), entries[i]);
        }
    }
}

seastar::future<> Engine::prefetchSeriesIndices(const std::vector<SeriesId128>& seriesIds) {
    // Snapshot TSM file pointers so compaction cannot invalidate iterators
    // across co_await suspension points.
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsmFile);
    }
    // Prefetch all TSM files in parallel (was sequential: one co_await per file).
    co_await seastar::parallel_for_each(tsmSnapshot, [&seriesIds](seastar::shared_ptr<TSM>& tsmFile) {
        return tsmFile->prefetchFullIndexEntries(seriesIds);
    });
}

seastar::future<> Engine::startBackgroundTasks() {
    // Background TSM conversions are launched per-rollover via seastar::try_with_gate
    // in WALFileManager::rolloverMemoryStore(). No separate startup needed.
    co_return;
}

seastar::future<VariantQueryResult> Engine::queryBySeries(std::string measurement,
                                                          std::map<std::string, std::string> tags, std::string field,
                                                          uint64_t startTime, uint64_t endTime) {
    // Get series ID from index
    auto seriesIdOpt = co_await index.getSeriesId(measurement, tags, field);

    if (!seriesIdOpt.has_value()) {
        // Series doesn't exist, return empty result
        co_return VariantQueryResult{QueryResult<double>{}};
    }

    // Convert back to series key for now (until we fully migrate to numeric IDs)
    TimeStarInsert<double> temp(measurement, field);
    temp.tags = tags;
    std::string seriesKey = temp.seriesKey();

    // Pre-compute SeriesId128 once and pass through to avoid redundant SHA1
    SeriesId128 dataSeriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Use existing query infrastructure - handle optional return
    auto resultOpt = co_await query(seriesKey, dataSeriesId, startTime, endTime);
    if (resultOpt.has_value()) {
        co_return std::move(resultOpt.value());
    }
    // Series data not found, return empty result
    co_return VariantQueryResult{QueryResult<double>{}};
}

seastar::future<std::vector<std::string>> Engine::getAllMeasurements() {
    auto measurements = co_await index.getAllMeasurements();
    // Convert set to vector (already sorted since std::set maintains order)
    std::vector<std::string> result(measurements.begin(), measurements.end());
    co_return result;
}

seastar::future<std::set<std::string>> Engine::getMeasurementFields(const std::string& measurement) {
    co_return co_await index.getFields(measurement);
}

seastar::future<std::set<std::string>> Engine::getMeasurementTags(const std::string& measurement) {
    co_return co_await index.getTags(measurement);
}

seastar::future<std::set<std::string>> Engine::getTagValues(const std::string& measurement, const std::string& tagKey) {
    co_return co_await index.getTagValues(measurement, tagKey);
}

seastar::future<std::vector<timestar::SeriesResult>> Engine::executeLocalQuery(const timestar::ShardQuery& shardQuery) {
    std::vector<timestar::SeriesResult> results;

    if (shardQuery.seriesIds.empty()) {
        co_return results;
    }

    // Fetch metadata from local index — each shard owns its own series metadata
    auto metadataBatch = co_await index.getSeriesMetadataBatch(shardQuery.seriesIds);

    // Process each series with its pre-fetched metadata
    for (const auto& [seriesId, seriesMetaOpt] : metadataBatch) {
        if (!seriesMetaOpt.has_value()) {
            continue;  // Series no longer exists
        }

        const auto& meta = seriesMetaOpt.value();

        // Convert to old-style series key for now (until we fully migrate)
        TimeStarInsert<double> temp(meta.measurement, meta.field);
        temp.tags = meta.tags;
        std::string seriesKey = temp.seriesKey();

        // Query data using existing infrastructure
        QueryRunner runner(&tsmFileManager, &walFileManager);
        auto variantResult = co_await runner.runQuery(seriesKey, shardQuery.startTime, shardQuery.endTime);

        // Convert to SeriesResult format
        timestar::SeriesResult seriesResult;
        seriesResult.measurement = meta.measurement;
        seriesResult.tags = meta.tags;

        // Handle different value types (all branches are identical)
        std::visit(
            [&](auto&& result) {
                if (!result.timestamps.empty()) {
                    seriesResult.fields[meta.field] =
                        std::make_pair(std::move(result.timestamps), timestar::FieldValues(std::move(result.values)));
                }
            },
            variantResult);

        // Only add if we got data
        if (!seriesResult.fields.empty()) {
            results.push_back(std::move(seriesResult));
        }
    }

    co_return results;
}

seastar::future<bool> Engine::deleteRange(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    co_return co_await deleteRangeImpl(std::move(seriesKey), startTime, endTime);
}

seastar::future<bool> Engine::deleteRangeImpl(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    // Delete from all TSM files that contain this series in the time range
    bool anyDeleted = false;

    // Compute SeriesId128 once for all lookups (avoids redundant XXH3 hashes)
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Check if the series exists in memory stores BEFORE deleting.
    // getSeriesType is type-agnostic and scans the memory stores once, replacing
    // the previous four type-templated queryMemoryStores<T> passes.
    bool existsInMemory = walFileManager.getSeriesType(seriesId).has_value();

    // Delete from memory stores and write to WAL
    co_await walFileManager.deleteFromMemoryStores(seriesKey, startTime, endTime);
    if (existsInMemory) {
        anyDeleted = true;
    }

    // Snapshot TSM file pointers to avoid iterator invalidation across co_await
    // (background compaction can mutate getSequencedTsmFiles() during suspension)
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsmFile);
    }
    for (const auto& tsmFile : tsmSnapshot) {
        bool deleted = co_await tsmFile->deleteRange(seriesId, startTime, endTime);
        if (deleted) {
            anyDeleted = true;
        }
    }

    // A delete spanning all of time is how a series is dropped outright, so it
    // releases the value-type binding and the series can be re-created with a
    // different type. A bounded-range delete must NOT release it: the remaining
    // points still have the old type, and re-typing around them is exactly the
    // conflict that wedges compaction.
    if (startTime == 0 && endTime == UINT64_MAX) {
        // The trigger is syntactic, not a proof of emptiness — tombstoned TSM
        // blocks survive until a rewrite materialises them. Record when we
        // release a binding while a source still reports the series, since that
        // is the window in which a re-type could still collide.
        if (walFileManager.getSeriesType(seriesId).has_value() || tsmFileManager.getSeriesType(seriesId).has_value()) {
            timestar::engine_log.warn(
                "Releasing value-type binding for series {} while data is still reported by a storage source; "
                "a re-type before compaction materialises the delete could conflict",
                seriesId.toHex());
        }
        co_await index.removeSeriesValueType(seriesId);
        _seriesTypeCache.erase(seriesId);
    }

    co_return anyDeleted;
}

seastar::future<bool> Engine::deleteRangeBySeries(std::string measurement, std::map<std::string, std::string> tags,
                                                  std::string field, uint64_t startTime, uint64_t endTime) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    // Look up series ID without creating it — deleting a non-existent series is a no-op.
    // Each shard has its own index now — look up locally.
    auto seriesIdOpt = co_await index.getSeriesId(measurement, tags, field);

    if (!seriesIdOpt.has_value()) {
        co_return false;  // Series doesn't exist, nothing to delete
    }

    // Use canonical series key construction
    TimeStarInsert<double> temp(measurement, field);
    temp.tags = tags;
    std::string seriesKey = temp.seriesKey();

    // Call the internal impl directly — we already hold the gate
    co_return co_await deleteRangeImpl(seriesKey, startTime, endTime);
}

seastar::future<Engine::DeleteResult> Engine::deleteByPattern(const DeleteRequest& request) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    DeleteResult result;

    // Debug logging disabled - uncomment for troubleshooting
    // timestar::engine_log.info("deleteByPattern called on shard {} for measurement: {}",
    //                       shardId, request.measurement);

    // Step 1: Find all series IDs that match the pattern from local index
    // Each shard's index only has its own series
    std::vector<SeriesId128> seriesIds;
    if (request.tags.empty()) {
        auto findResult = co_await index.getAllSeriesForMeasurement(request.measurement);
        if (findResult.has_value())
            seriesIds = std::move(findResult.value());
    } else {
        auto findResult = co_await index.findSeries(request.measurement, request.tags);
        if (findResult.has_value())
            seriesIds = std::move(findResult.value());
    }

    // Step 2: Get metadata for each series to check field filters
    // Batch all metadata lookups into a single RPC to shard 0 to avoid N sequential RPCs.
    std::vector<std::pair<SeriesId128, std::string>> seriesToDelete;  // (seriesId, seriesKey)

    // Convert fields to unordered_set for O(1) lookup (vs O(n) linear search)
    std::unordered_set<std::string> fieldFilter(request.fields.begin(), request.fields.end());

    auto filterAndBuild = [&fieldFilter](const std::vector<SeriesId128>& ids, timestar::index::NativeIndex& idx)
        -> seastar::future<std::vector<std::pair<SeriesId128, std::string>>> {
        std::vector<std::pair<SeriesId128, std::string>> matched;
        for (const auto& seriesId : ids) {
            auto metadata = co_await idx.getSeriesMetadata(seriesId);
            if (!metadata.has_value())
                continue;
            if (!fieldFilter.empty() && fieldFilter.count(metadata->field) == 0)
                continue;
            TimeStarInsert<double> temp(metadata->measurement, metadata->field);
            temp.tags = metadata->tags;
            matched.push_back({seriesId, temp.seriesKey()});
        }
        co_return matched;
    };

    // Metadata is local — each shard's index has only its own series
    seriesToDelete = co_await filterAndBuild(seriesIds, index);

    // Step 3: Delete each matching series
    for (const auto& [seriesId, seriesKey] : seriesToDelete) {
        bool deleted = co_await deleteRangeImpl(seriesKey, request.startTime, request.endTime);
        if (deleted) {
            result.seriesDeleted++;
            result.deletedSeries.push_back(seriesKey);

            // Estimate points deleted (this is rough - actual count would require reading the data)
            // For now, we'll just track that deletion occurred
            result.pointsDeleted++;  // This is a placeholder
        }
    }

    // timestar::engine_log.info("[DELETE_DEBUG] Shard {} deleted {} series",
    //                       shardId, result.seriesDeleted);

    co_return result;
}

seastar::future<> Engine::rolloverMemoryStore() {
    auto gate_holder = _insertGate.hold();
    ++_metrics.wal_rollovers_total;
    timestar::engine_log.debug("[ENGINE] Rolling over memory store on shard {}", shardId);
    co_return co_await walFileManager.rolloverMemoryStore();
}

seastar::future<bool> Engine::forceRolloverMemoryStoreForVShardSnapshot(uint16_t vshard) {
    auto gateHolder = _insertGate.hold();
    timestar::engine_log.debug("[ENGINE] Forcing memory-store rollover for VShard {} snapshot progress on shard {}",
                               vshard, shardId);
    const bool rolled = co_await walFileManager.forceRolloverMemoryStoreForVShard(vshard);
    if (rolled)
        ++_metrics.wal_rollovers_total;
    co_return rolled;
}

template seastar::future<> Engine::insert<bool>(TimeStarInsert<bool> insertRequest, bool skipMetadataIndexing);
template seastar::future<> Engine::insert<double>(TimeStarInsert<double> insertRequest, bool skipMetadataIndexing);
template seastar::future<> Engine::insert<std::string>(TimeStarInsert<std::string> insertRequest,
                                                       bool skipMetadataIndexing);
template seastar::future<> Engine::insert<int64_t>(TimeStarInsert<int64_t> insertRequest, bool skipMetadataIndexing);

template seastar::future<WALTimingInfo> Engine::insertBatch<bool>(std::vector<TimeStarInsert<bool>> insertRequests,
                                                                  bool enforceAdmission);
template seastar::future<WALTimingInfo> Engine::insertBatch<double>(std::vector<TimeStarInsert<double>> insertRequests,
                                                                    bool enforceAdmission);
template seastar::future<WALTimingInfo> Engine::insertBatch<std::string>(
    std::vector<TimeStarInsert<std::string>> insertRequests, bool enforceAdmission);
template seastar::future<WALTimingInfo> Engine::insertBatch<int64_t>(
    std::vector<TimeStarInsert<int64_t>> insertRequests, bool enforceAdmission);

#define TIMESTAR_INST_ENFORCE(T)                                                            \
    template seastar::future<std::vector<TimeStarInsert<T>>> Engine::enforceSeriesTypes<T>( \
        std::vector<TimeStarInsert<T>>, bool);
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(TIMESTAR_INST_ENFORCE)
#undef TIMESTAR_INST_ENFORCE

template seastar::future<SeriesId128> Engine::indexMetadata<bool>(TimeStarInsert<bool> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<double>(TimeStarInsert<double> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<std::string>(TimeStarInsert<std::string> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<int64_t>(TimeStarInsert<int64_t> insertRequest);

// --- Retention policy management ---

void Engine::updateRetentionPolicyCache(const RetentionPolicy& policy) {
    _retentionPolicies[policy.measurement] = policy;
}

void Engine::removeRetentionPolicyCache(const std::string& measurement) {
    _retentionPolicies.erase(measurement);
}

void Engine::setRetentionPolicies(std::unordered_map<std::string, RetentionPolicy> policies) {
    _retentionPolicies = std::move(policies);
}

std::optional<RetentionPolicy> Engine::getRetentionPolicy(const std::string& measurement) const {
    auto it = _retentionPolicies.find(measurement);
    if (it != _retentionPolicies.end()) {
        return it->second;
    }
    return std::nullopt;
}

seastar::future<> Engine::loadAndBroadcastRetentionPolicies() {
    if (shardId != 0) {
        co_return;
    }

    if (!shardedRef) {
        co_return;
    }

    auto policies = co_await index.getAllRetentionPolicies();
    std::unordered_map<std::string, RetentionPolicy> policyMap;
    for (auto& p : policies) {
        policyMap[p.measurement] = p;
    }

    timestar::engine_log.info("[RETENTION] Loaded {} retention policies from NativeIndex", policyMap.size());

    // Broadcast to all shards (shared_ptr avoids N deep copies of the map)
    auto sharedPolicies =
        std::make_shared<const std::unordered_map<std::string, RetentionPolicy>>(std::move(policyMap));
    co_await shardedRef->invoke_on_all([sharedPolicies](Engine& engine) {
        engine.setRetentionPolicies(*sharedPolicies);
        return seastar::make_ready_future<>();
    });
}

void Engine::startRetentionSweepTimer() {
    if (shardId != 0)
        return;

    _retentionTimer.set_callback([this] {
        if (!shardedRef)
            return;

        // Use try_with_gate to safely handle the gate being closed during shutdown.
        // This avoids the TOCTOU race between is_closed() check and enter().
        (void)seastar::try_with_gate(_retentionGate, [this] {
            return shardedRef->invoke_on_all([](Engine& engine) {
                return engine.sweepExpiredFiles().then([&engine] { return engine.sweepTombstoneRewrites(); });
            });
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // Shutdown in progress — expected, ignore.
            } catch (const std::exception& e) {
                timestar::engine_log.warn("[RETENTION] Sweep failed: {}", e.what());
            } catch (...) {
                timestar::engine_log.warn("[RETENTION] Sweep failed with unknown error");
            }
        });
    });

    auto sweepMinutes = timestar::config().engine.retention_sweep_interval_minutes;
    _retentionTimer.arm_periodic(std::chrono::minutes(sweepMinutes));
    timestar::engine_log.info("[RETENTION] Started retention sweep timer ({}min interval)", sweepMinutes);
}

seastar::future<> Engine::sweepExpiredFiles() {
    if (_retentionPolicies.empty()) {
        co_return;
    }

    uint64_t now =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    // Build measurement -> ttlCutoff map from local cache
    std::unordered_map<std::string, uint64_t> ttlCutoffs;
    for (const auto& [measurement, policy] : _retentionPolicies) {
        if (policy.ttlNanos > 0 && now > policy.ttlNanos) {
            ttlCutoffs[measurement] = now - policy.ttlNanos;
        }
    }

    if (ttlCutoffs.empty()) {
        co_return;
    }

    // Build seriesId -> measurement map from local index
    // Each shard's index only has series owned by this shard
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurementMap;
    for (const auto& [measurement, cutoff] : ttlCutoffs) {
        auto result = co_await index.getAllSeriesForMeasurement(measurement);
        std::vector<SeriesId128> seriesIds;
        if (result.has_value()) {
            seriesIds = std::move(result.value());
        }
        for (const auto& sid : seriesIds) {
            seriesMeasurementMap[sid] = measurement;
        }
    }

    if (seriesMeasurementMap.empty()) {
        co_return;
    }

    // Snapshot TSM file pointers — background compaction can mutate the live map
    // between co_await calls above and this iteration.
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (auto& [rank, tsm] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsm);
    }

    // Iterate TSM files, check block metadata against TTL cutoffs
    std::vector<seastar::shared_ptr<TSM>> fullyExpiredFiles;

    for (const auto& tsmFile : tsmSnapshot) {
        bool allExpired = true;

        auto allSeriesIds = tsmFile->getSeriesIds();
        for (const auto& seriesId : allSeriesIds) {
            auto it = seriesMeasurementMap.find(seriesId);
            if (it == seriesMeasurementMap.end()) {
                // Series not under TTL policy - file is not fully expired
                allExpired = false;
                continue;
            }

            uint64_t cutoff = ttlCutoffs[it->second];
            // Use sparse index maxTime (always in memory, no I/O) instead of
            // getSeriesBlocks() which only returns data from the LRU cache.
            // A cache miss would return empty blocks, leaving allExpired=true
            // and causing premature file deletion.
            uint64_t maxTime = tsmFile->getSeriesMaxTime(seriesId);
            if (maxTime >= cutoff) {
                allExpired = false;
            }
        }

        if (allExpired && !allSeriesIds.empty()) {
            fullyExpiredFiles.push_back(tsmFile);
        }
        // Partially expired files will be cleaned up during normal compaction
        // (which now applies TTL filtering)
    }

    if (!fullyExpiredFiles.empty()) {
        timestar::engine_log.info("[RETENTION] Shard {}: removing {} fully expired TSM files", shardId,
                                  fullyExpiredFiles.size());
        co_await tsmFileManager.removeTSMFiles(fullyExpiredFiles);
    }

    // Phase 3: Clean up expired day bitmaps from the index
    for (const auto& [measurement, cutoff] : ttlCutoffs) {
        uint32_t cutoffDay = timestar::index::keys::dayBucketFromNs(cutoff);
        co_await index.removeExpiredDayBitmaps(measurement, cutoffDay);
    }
}

seastar::future<> Engine::sweepTombstoneRewrites() {
    const double DEAD_FRACTION_THRESHOLD = timestar::config().engine.tombstone_dead_fraction_threshold;
    const size_t MAX_REWRITES_PER_SWEEP = timestar::config().engine.max_tombstone_rewrites_per_sweep;

    auto* compactor = tsmFileManager.getCompactor();
    if (!compactor) {
        co_return;
    }

    // --- Phase 1: Snapshot ---
    // Snapshot file pointers before any co_await.  The compaction loop runs
    // as a separate coroutine on this shard; during any suspension it may
    // call addTSMFile / removeTSMFiles, which mutate sequencedTsmFiles and
    // would invalidate a live iterator over the map.
    std::vector<seastar::shared_ptr<TSM>> tombstonedFiles;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        if (tsmFile->hasTombstones()) {
            tombstonedFiles.push_back(tsmFile);
        }
    }
    // hasTombstones() is O(1), no suspension points — snapshot is consistent.

    if (tombstonedFiles.empty()) {
        co_return;
    }

    // --- Phase 2: Estimate coverage (suspension points here) ---
    struct Candidate {
        seastar::shared_ptr<TSM> file;
        double deadFraction;
    };
    std::vector<Candidate> candidates;

    for (auto& tsmFile : tombstonedFiles) {
        // Skip files currently being compacted (saves the DMA prefetch cost).
        if (compactor->isFileInActiveCompaction(tsmFile)) {
            continue;
        }

        double deadFraction = co_await tsmFile->estimateTombstoneCoverage();
        if (deadFraction > DEAD_FRACTION_THRESHOLD) {
            candidates.push_back({tsmFile, deadFraction});
        }
    }

    if (candidates.empty()) {
        co_return;
    }

    // Sort by dead fraction descending (worst offenders first)
    std::sort(candidates.begin(), candidates.end(),
              [](const Candidate& a, const Candidate& b) { return a.deadFraction > b.deadFraction; });

    // --- Phase 3: Rewrite (at most MAX_REWRITES_PER_SWEEP) ---
    size_t rewriteCount = std::min(candidates.size(), MAX_REWRITES_PER_SWEEP);

    for (size_t i = 0; i < rewriteCount; ++i) {
        auto& candidate = candidates[i];

        // Non-blocking: skip remaining rewrites if both compaction slots are busy.
        // Single-threaded guarantee: no task can acquire the semaphore between
        // this check and the get_units() call inside executeCompaction(), because
        // there are no suspension points in between.
        if (!compactor->hasCompactionCapacity()) {
            break;
        }

        // Staleness guard: the file may have been removed from the file manager
        // by a compaction that completed during one of our estimation co_awaits.
        // The shared_ptr keeps the TSM object alive, but executeCompaction would
        // try to removeTSMFiles + scheduleDelete on an already-deleted file.
        auto rank = candidate.file->rankAsInteger();
        auto it = tsmFileManager.getSequencedTsmFiles().find(rank);
        if (it == tsmFileManager.getSequencedTsmFiles().end() || it->second.get() != candidate.file.get()) {
            continue;
        }

        // Also re-check active compaction: the compaction loop may have picked
        // up this file while we were estimating other files.
        if (compactor->isFileInActiveCompaction(candidate.file)) {
            continue;
        }

        timestar::engine_log.info(
            "[TOMBSTONE-REWRITE] Shard {}: rewriting file (tier {}, seq {}) "
            "with {:.1f}% estimated dead data",
            shardId, candidate.file->tierNum, candidate.file->seqNum, candidate.deadFraction * 100.0);
        try {
            auto stats = co_await compactor->executeTombstoneRewrite(candidate.file);
            timestar::engine_log.info(
                "[TOMBSTONE-REWRITE] Shard {}: rewrite complete, "
                "{} points written in {}ms",
                shardId, stats.pointsWritten, stats.duration.count());
        } catch (const std::exception& e) {
            timestar::engine_log.warn("[TOMBSTONE-REWRITE] Shard {}: rewrite failed: {}", shardId, e.what());
        }
    }
}

#include "engine_local_store.hpp"

#include "../../core/placement_table.hpp"
#include "../../index/index_backend.hpp"
#include "../scatter_gather.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <set>
#include <unordered_set>
#include <utility>

namespace timestar::cluster {

using ::MetadataOp;  // MetadataOp is in the global namespace (like TSMValueType/SeriesId128)

EngineLocalStore::EngineLocalStore(seastar::sharded<Engine>& engines) : engines_(engines) {}

seastar::future<data::SnapshotPayload> EngineLocalStore::buildVShardSnapshot(VShardId vshard) {
    if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
        throw std::runtime_error(
            "buildVShardSnapshot: core count is not VShard-cohesive; a single-core snapshot would omit "
            "series that scatter across cores (refusing to ship a partial snapshot)");
    const unsigned core = timestar::assignCore(vshard, seastar::smp::count);
    auto built = co_await engines_.invoke_on(core, [vshard](Engine& e) { return e.buildVShardSnapshotFiles(vshard); });
    data::SnapshotPayload payload;
    payload.manifest = std::move(built.manifest);
    payload.catalog = std::move(built.catalog);
    payload.files.reserve(built.files.size());
    for (auto& [name, bytes] : built.files)
        payload.files.push_back(data::SnapshotFile{std::move(name), std::move(bytes)});
    co_return payload;
}

seastar::future<bool> EngineLocalStore::hasUnconvertedStores(VShardId vshard) {
    const unsigned core = timestar::assignCore(vshard, seastar::smp::count);
    // ONE predicate, one spelling (review F7): `WALFileManager` is the authority on "a
    // rolled store has not reached TSM yet", and re-deriving it from the retained count
    // here would drift the moment that vector's shape changes. See the header for why
    // compaction must wait on it.
    //
    // NARROWED TO THIS VSHARD (debt D-35). It used to ask the per-SHARD question -- "does
    // ANY rolled store on this core still await conversion?" -- which is the same answer
    // for all ~1365 groups on the core, so a shard under sustained ingest (never at zero
    // pending conversions) let NONE of them compact. The bit probe below asks whether an
    // unconverted store holds THIS VShard's data, which is the condition the safety
    // argument actually needs; see WALFileManager::hasPendingConversionsForVShard.
    const uint16_t vs = vshard.value();
    co_return co_await engines_.invoke_on(core, [vs](Engine& e) { return e.hasPendingWalConversionsForVShard(vs); });
}

seastar::future<bool> EngineLocalStore::installVShardSnapshot(VShardId vshard, data::SnapshotPayload payload) {
    if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
        throw std::runtime_error("installVShardSnapshot: core count is not VShard-cohesive");
    if (payload.manifest.vshard != vshard)
        co_return false;
    const unsigned core = timestar::assignCore(vshard, seastar::smp::count);
    // The replicated apply path already runs on assignCore(vshard), so this invoke_on
    // is inline (no cross-core transfer of the payload's strings).
    co_return co_await engines_.invoke_on(core, [payload = std::move(payload)](Engine& e) mutable {
        if (payload.catalog.empty())
            return seastar::make_ready_future<bool>(false);
        std::vector<std::pair<std::string, std::string>> files;
        files.reserve(payload.files.size());
        for (auto& f : payload.files)
            files.emplace_back(std::move(f.name), std::move(f.bytes));
        return e.installVShardSnapshotBundle(std::move(payload.manifest), std::move(files), std::move(payload.catalog));
    });
}

unsigned EngineLocalStore::coreFor(const SeriesId128& id) const {
    // MUST match the routing authority the metadata/query path uses:
    // Engine::indexMetadataSync routes each op by routeToCore(id), and query
    // discovery is per-core-local, so data and its index entry must co-locate on
    // routeToCore's core. Choosing assignCore(virtualShard) here instead splits a
    // new series' data and index across cores whenever routeToCore != assignCore
    // (they diverge for non-power-of-2 core counts, hash >= 4096) -> the series is
    // invisible to queries. VShard-cohesive single-core placement (the M3
    // per-VShard-Raft precondition) must therefore be a GLOBAL routeToCore change
    // in cluster mode, not an adapter-local one; the adapter always follows
    // routeToCore so it stays consistent with the rest of the node.
    return timestar::routeToCore(id);
}

namespace {
// Build a typed TimeStarInsert<T> from a WriteSeries (already known to be
// consistent). fromSeriesKey parses measurement/field/tags; the cached key/id
// skip a rebuild+rehash downstream.
template <typename T>
TimeStarInsert<T> makeInsert(const data::WriteSeries& s, const SeriesId128& id, std::vector<T> values) {
    TimeStarInsert<T> ins = TimeStarInsert<T>::fromSeriesKey(s.seriesKey);
    ins.timestamps = s.timestamps;
    ins.values = std::move(values);
    ins.revisions = s.revisions;  // pass-through; Engine must not re-stamp a non-empty vector
    ins.setCachedSeriesKey(s.seriesKey);
    ins.setCachedSeriesId128(id);
    return ins;
}
}  // namespace

seastar::future<> EngineLocalStore::applyWrites(data::WriteBatch batch) {
    return applyWritesImpl(std::move(batch), true);
}

seastar::future<> EngineLocalStore::applyCommittedWrites(data::WriteBatch batch) {
    return applyWritesImpl(std::move(batch), false);
}

seastar::future<> EngineLocalStore::checkWriteAdmission(const data::WriteBatch& batch) {
    std::set<unsigned> cores;
    for (const auto& s : batch.series)
        cores.insert(coreFor(SeriesId128::fromSeriesKey(s.seriesKey)));
    try {
        for (unsigned core : cores)
            co_await engines_.invoke_on(core, [](Engine& e) { e.checkIngestAdmission(); });
    } catch (const timestar::IngestBacklogException& e) {
        throw data::WriteOverloadedError(e.what());
    }
}

seastar::future<> EngineLocalStore::checkWriteAdmission(data::VShardBatchView view) {
    std::set<unsigned> cores;
    for (const auto* group : view)
        for (const auto& s : group->second.series)
            cores.insert(coreFor(SeriesId128::fromSeriesKey(s.seriesKey)));
    try {
        for (unsigned core : cores)
            co_await engines_.invoke_on(core, [](Engine& e) { e.checkIngestAdmission(); });
    } catch (const timestar::IngestBacklogException& e) {
        throw data::WriteOverloadedError(e.what());
    }
}

seastar::future<> EngineLocalStore::applyWritesImpl(data::WriteBatch batch, bool enforceAdmission) {
    std::map<unsigned, std::vector<TimeStarInsert<double>>> doubles;
    std::map<unsigned, std::vector<TimeStarInsert<int64_t>>> ints;
    std::map<unsigned, std::vector<TimeStarInsert<bool>>> bools;
    std::map<unsigned, std::vector<TimeStarInsert<std::string>>> strings;
    std::vector<MetadataOp> metaOps;
    metaOps.reserve(batch.series.size());

    for (auto& s : batch.series) {
        if (!s.consistent())
            throw std::runtime_error("cluster applyWrites: inconsistent WriteSeries");
        // A point-less series carries no data; emitting a MetadataOp for it would
        // register a phantom measurement/field/tag schema the normal write path
        // (every point has a value) can never produce. Skip it entirely.
        if (s.timestamps.empty())
            continue;
        const SeriesId128 id = SeriesId128::fromSeriesKey(s.seriesKey);
        const unsigned core = coreFor(id);
        uint64_t minTs = 0, maxTs = 0;
        if (!s.timestamps.empty()) {
            minTs = s.timestamps.front();
            maxTs = s.timestamps.front();
            for (uint64_t t : s.timestamps) {
                minTs = std::min(minTs, t);
                maxTs = std::max(maxTs, t);
            }
        }
        std::string measurement, field;
        std::map<std::string, std::string> tags;
        switch (s.type) {
            case TSMValueType::Float: {
                auto ins = makeInsert<double>(s, id, std::get<0>(s.values));
                measurement = ins.measurement;
                field = ins.field;
                tags = ins.tags;
                doubles[core].push_back(std::move(ins));
                break;
            }
            case TSMValueType::Integer: {
                auto ins = makeInsert<int64_t>(s, id, std::get<1>(s.values));
                measurement = ins.measurement;
                field = ins.field;
                tags = ins.tags;
                ints[core].push_back(std::move(ins));
                break;
            }
            case TSMValueType::Boolean: {
                auto ins = makeInsert<bool>(s, id, std::get<2>(s.values));
                measurement = ins.measurement;
                field = ins.field;
                tags = ins.tags;
                bools[core].push_back(std::move(ins));
                break;
            }
            case TSMValueType::String: {
                auto ins = makeInsert<std::string>(s, id, std::get<3>(s.values));
                measurement = ins.measurement;
                field = ins.field;
                tags = ins.tags;
                strings[core].push_back(std::move(ins));
                break;
            }
        }
        MetadataOp op;
        op.valueType = s.type;
        op.measurement = std::move(measurement);
        op.fieldName = std::move(field);
        op.tags = std::move(tags);
        op.minTs = minTs;
        op.maxTs = maxTs;
        op.seriesId = id;
        metaOps.push_back(std::move(op));
    }

    // Dispatch each per-core typed batch to its owning core, concurrently, then
    // await all. Data first, then the schema sync (mirrors the write handler).
    std::vector<seastar::future<>> pending;
    for (auto& [core, v] : doubles)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v), enforceAdmission](Engine& e) mutable {
            return e.insertBatch<double>(std::move(v), enforceAdmission).discard_result();
        }));
    for (auto& [core, v] : ints)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v), enforceAdmission](Engine& e) mutable {
            return e.insertBatch<int64_t>(std::move(v), enforceAdmission).discard_result();
        }));
    for (auto& [core, v] : bools)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v), enforceAdmission](Engine& e) mutable {
            return e.insertBatch<bool>(std::move(v), enforceAdmission).discard_result();
        }));
    for (auto& [core, v] : strings)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v), enforceAdmission](Engine& e) mutable {
            return e.insertBatch<std::string>(std::move(v), enforceAdmission).discard_result();
        }));

    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            co_await std::move(f);
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);

    if (!metaOps.empty())
        co_await engines_.local().indexMetadataSync(std::move(metaOps));
    co_return;
}

seastar::future<bool> EngineLocalStore::applyDelete(std::string seriesKey, uint64_t start, uint64_t end) {
    const SeriesId128 id = SeriesId128::fromSeriesKey(seriesKey);
    const unsigned core = coreFor(id);
    co_return co_await engines_.invoke_on(core, [seriesKey = std::move(seriesKey), start, end](Engine& e) mutable {
        return e.deleteRange(std::move(seriesKey), start, end);
    });
}

seastar::future<data::NodeQueryPartial> EngineLocalStore::queryLocal(data::NodeQueryRequest req) {
    // Produce this node's UNFINALIZED partials (F.5b): the coordinator unions them
    // with peers' partials and finalizes ONCE, so cross-node group-by / spread are
    // correct. A local early-exit (incomplete/timeout/limit) becomes an
    // incompleteReason, fail-closed -- never a silent empty success.
    // Confirm CURRENT-TERM LEADERSHIP WITH A QUORUM before touching storage. Merely
    // observing `isLeader()` is not sufficient: a partitioned former leader keeps that
    // local role until CheckQuorum expires while the majority may already have elected
    // another leader and committed newer data. readBarrier() also waits until this
    // leader has applied through the confirmed index.
    if (leaderReadFence_) {
        if (req.vshards.empty()) {
            data::NodeQueryPartial unfenced;
            unfenced.incompleteReasons.push_back(
                "replicated leader read has no VShard filter; quorum freshness cannot be established");
            co_return unfenced;
        }
        if (!co_await leaderReadFence_(req.vshards)) {
            data::NodeQueryPartial unfenced;
            unfenced.incompleteReasons.push_back(
                "one or more VShards could not confirm current leadership with a quorum");
            co_return unfenced;
        }
    }

    // FENCE ON THIS NODE'S OWN APPLY LAG NEXT (debt D-36). The Engine answers from
    // APPLIED state; a replicated node's committed log can be ahead of it, and every
    // entry in that gap is an acknowledged write this query would silently omit. Failing
    // closed here is the same rule the single-node path already applies to an unreadable
    // series -- an empty result must mean "this range genuinely holds no data".
    if (applyFence_) {
        const bool caughtUp = co_await applyFence_();
        if (!caughtUp) {
            data::NodeQueryPartial behind;
            behind.incompleteReasons.push_back(
                "node has committed but unapplied writes (still catching up); its answer would omit acknowledged "
                "points");
            co_return behind;
        }
    }
    http::HttpQueryHandler handler(&engines_);
    // RF=3: req.vshards names the VShards this node must answer for (the ones it
    // leads); restrict discovery to them so a replicated series is not double-counted
    // across replicas. Empty => no restriction (RF=1/M2 disjoint placement).
    std::optional<std::set<uint16_t>> filter;
    if (!req.vshards.empty())
        filter.emplace(req.vshards.begin(), req.vshards.end());
    http::HttpQueryHandler::NodePartials np =
        co_await handler.queryLocalPartials(std::move(req.request), filter ? &*filter : nullptr);
    data::NodeQueryPartial partial;
    if (!np.ok) {
        partial.incompleteReasons.push_back(np.errorResponse.errorMessage.empty() ? std::string("query failed")
                                                                                  : np.errorResponse.errorMessage);
        co_return partial;
    }
    partial.partials = std::move(np.partials);
    partial.nonNumeric = std::move(np.nonNumeric);
    partial.seriesFound = np.seriesFound;
    co_return partial;
}

seastar::future<data::PatternSeriesResult> EngineLocalStore::findPatternSeries(data::PatternSeriesRequest req) {
    if (req.vshards.empty() || req.maxSeries == 0 || req.maxSeries > data::kPatternSeriesMaxResults)
        throw std::invalid_argument("pattern-series discovery requires VShards and a supported result bound");
    if (!leaderReadFence_ || !applyFence_)
        throw std::logic_error("pattern-series discovery is unavailable until both cluster read fences are wired");

    // This is the same catalog that query discovery reads. Prove current-term
    // leadership and apply through the confirmed ReadIndex before inspecting it;
    // otherwise a partitioned former leader could omit an acknowledged series and
    // turn a broad delete into a silent partial success.
    if (!co_await leaderReadFence_(req.vshards))
        throw std::runtime_error("pattern-series discovery could not confirm current leadership with a quorum");
    if (!co_await applyFence_())
        throw std::runtime_error(
            "node has committed but unapplied writes; pattern-series discovery would omit acknowledged series");

    std::map<unsigned, std::vector<uint16_t>> byCore;
    for (uint16_t vshard : req.vshards) {
        if (vshard >= timestar::VIRTUAL_SHARD_COUNT)
            throw std::invalid_argument("pattern-series discovery contains an invalid VShard");
        byCore[timestar::assignCore(timestar::VShardId{vshard}, seastar::smp::count)].push_back(vshard);
    }

    data::PatternSeriesResult result;
    size_t resultKeyBytes = 0;
    for (auto& [core, vshards] : byCore) {
        const uint32_t remaining = result.seriesKeys.size() < req.maxSeries
                                       ? req.maxSeries - static_cast<uint32_t>(result.seriesKeys.size())
                                       : 0;
        const size_t remainingKeyBytes =
            resultKeyBytes < data::kPatternSeriesMaxKeyBytes ? data::kPatternSeriesMaxKeyBytes - resultKeyBytes : 0;
        auto part = co_await engines_.invoke_on(
            core,
            [selector = req.selector, vshards = std::move(vshards), remaining,
             remainingKeyBytes](Engine& engine) mutable -> seastar::future<data::PatternSeriesResult> {
                data::PatternSeriesResult local;
                size_t localKeyBytes = 0;
                std::unordered_set<std::string> fieldFilter(selector.fields.begin(), selector.fields.end());
                for (uint16_t vshard : vshards) {
                    const size_t left = remaining > local.seriesKeys.size() ? remaining - local.seriesKeys.size() : 0;
                    const size_t bytesLeft = remainingKeyBytes > localKeyBytes ? remainingKeyBytes - localKeyBytes : 0;
                    // When the global budget is exactly full, probe one key per
                    // remaining VShard: empty is valid; any match proves overflow.
                    const size_t scanLimit = left == 0 ? 1 : left;
                    auto found = co_await engine.getIndex().findVShardSeriesKeys(
                        vshard, selector.measurement, selector.tags, fieldFilter, scanLimit, bytesLeft);
                    if (!found.has_value()) {
                        local.limitExceeded = true;
                        co_return local;
                    }
                    if (left == 0 && !found->empty()) {
                        local.limitExceeded = true;
                        co_return local;
                    }
                    for (const auto& key : *found)
                        localKeyBytes += sizeof(uint32_t) + key.size();
                    local.seriesKeys.insert(local.seriesKeys.end(), std::make_move_iterator(found->begin()),
                                            std::make_move_iterator(found->end()));
                }
                co_return local;
            });
        if (part.limitExceeded) {
            result.limitExceeded = true;
            result.seriesKeys.clear();
            co_return result;
        }
        for (const auto& key : part.seriesKeys)
            resultKeyBytes += sizeof(uint32_t) + key.size();
        result.seriesKeys.insert(result.seriesKeys.end(), std::make_move_iterator(part.seriesKeys.begin()),
                                 std::make_move_iterator(part.seriesKeys.end()));
    }
    co_return result;
}

seastar::future<data::MetadataResult> EngineLocalStore::queryMetadata(data::MetadataRequest req) {
    // FENCE THIS LEG TOO (debt D-36). Metadata is a READ, and it is answered from the
    // same applied state a query is: a measurement, field, tag value or cardinality
    // estimate that exists only in committed-but-unapplied entries is missing from a
    // successful 200 exactly as a data point would be. `MetadataResult` has no
    // incompleteReason channel, so this THROWS -- which is the right shape here, because
    // `ClusterDataPlane::metadata` already fails the whole request on any node's
    // exception for precisely this reason ("a partial metadata answer would be silently
    // incomplete, same contract as queries").
    if (applyFence_ && !co_await applyFence_())
        throw std::runtime_error(
            "node has committed but unapplied writes (still catching up); its metadata would omit acknowledged series");

    // Snapshot replacement is subtractive, whereas the legacy schema blobs and
    // HLLs are append-oriented broadcasts. Derive cluster-facing metadata from
    // authoritative primary series rows on every core so a superseded
    // generation cannot leave a phantom measurement/field/tag or over-count.
    data::MetadataResult out;
    std::set<std::string> measurements;
    std::set<std::string> fields;
    std::map<std::string, std::string> fieldTypes;
    std::set<std::string> tagKeys;
    std::set<std::string> tagValues;
    uint64_t measurementSeries = 0;
    uint64_t matchingTagSeries = 0;
    for (unsigned core = 0; core < seastar::smp::count; ++core) {
        auto summary = co_await engines_.invoke_on(
            core, [measurement = req.measurement, tagKey = req.tagKey, tagValue = req.tagValue](Engine& engine) {
                return engine.getIndex().summarizeExactMetadata(measurement, tagKey, tagValue);
            });
        measurements.insert(summary.measurements.begin(), summary.measurements.end());
        fields.insert(summary.fields.begin(), summary.fields.end());
        for (auto& [field, type] : summary.fieldTypes) {
            auto [it, inserted] = fieldTypes.emplace(field, type);
            if (!inserted && it->second != type)
                throw std::runtime_error("conflicting field types across local shards for " + req.measurement + " " +
                                         field);
        }
        tagKeys.insert(summary.tagKeys.begin(), summary.tagKeys.end());
        tagValues.insert(summary.tagValues.begin(), summary.tagValues.end());
        measurementSeries += summary.measurementSeries;
        matchingTagSeries += summary.matchingTagSeries;
    }

    switch (req.kind) {
        case data::MetadataKind::Measurements: {
            out.items.assign(measurements.begin(), measurements.end());
            break;
        }
        case data::MetadataKind::Fields: {
            // Carry the field TYPE alongside the name as "name\x1ftype" so the
            // coordinator's set-union preserves it (a field has one global type, so
            // identical pairs dedup). The handler splits on \x1f.
            for (const auto& field : fields) {
                const auto type = fieldTypes.find(field);
                if (type == fieldTypes.end())
                    throw std::runtime_error("series metadata has no durable value type for " + req.measurement + " " +
                                             field);
                out.items.push_back(field + std::string(1, '\x1f') + type->second);
            }
            break;
        }
        case data::MetadataKind::TagKeys: {
            out.items.assign(tagKeys.begin(), tagKeys.end());
            break;
        }
        case data::MetadataKind::TagValues: {
            out.items.assign(tagValues.begin(), tagValues.end());
            break;
        }
        case data::MetadataKind::MeasurementCardinality: {
            out.cardinality = static_cast<double>(measurementSeries);
            break;
        }
        case data::MetadataKind::TagCardinality: {
            out.cardinality = static_cast<double>(matchingTagSeries);
            break;
        }
    }
    co_return out;
}

seastar::future<> EngineLocalStore::applyRetention(std::string, uint64_t) {
    // Retention-cutoff application is wired in a later milestone (M1.x/M6); the
    // command type carries it, but the M2 write path does not use it yet.
    co_return;
}

}  // namespace timestar::cluster

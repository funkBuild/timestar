#include "engine_local_store.hpp"

#include "../../core/placement_table.hpp"
#include "../../index/index_backend.hpp"
#include "../scatter_gather.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
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
    auto built = co_await engines_.invoke_on(
        core, [vshard](Engine& e) { return e.buildVShardSnapshotFiles(vshard, std::string(32, '0')); });
    data::SnapshotPayload payload;
    payload.manifest = std::move(built.first);
    payload.files.reserve(built.second.size());
    for (auto& [name, bytes] : built.second)
        payload.files.push_back(data::SnapshotFile{std::move(name), std::move(bytes)});
    co_return payload;
}

seastar::future<bool> EngineLocalStore::hasUnconvertedStores(VShardId vshard) {
    const unsigned core = timestar::assignCore(vshard, seastar::smp::count);
    // ONE predicate, one spelling (review F7): `WALFileManager::hasPendingConversions` is
    // the authority on "a rolled store has not reached TSM yet", and re-deriving it from
    // the retained count here would drift the moment that vector's shape changes. See the
    // header for why compaction must wait on it.
    co_return co_await engines_.invoke_on(core, [](Engine& e) { return e.hasPendingWalConversions(); });
}

seastar::future<bool> EngineLocalStore::installVShardSnapshot(VShardId vshard, data::SnapshotPayload payload) {
    if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
        throw std::runtime_error("installVShardSnapshot: core count is not VShard-cohesive");
    const unsigned core = timestar::assignCore(vshard, seastar::smp::count);
    // The replicated apply path already runs on assignCore(vshard), so this invoke_on
    // is inline (no cross-core transfer of the payload's strings).
    co_return co_await engines_.invoke_on(core, [payload = std::move(payload)](Engine& e) mutable {
        std::vector<std::pair<std::string, std::string>> files;
        files.reserve(payload.files.size());
        for (auto& f : payload.files)
            files.emplace_back(std::move(f.name), std::move(f.bytes));
        return e.installVShardSnapshotFiles(payload.manifest, std::move(files));
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
        pending.push_back(engines_.invoke_on(core, [v = std::move(v)](Engine& e) mutable {
            return e.insertBatch<double>(std::move(v)).discard_result();
        }));
    for (auto& [core, v] : ints)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v)](Engine& e) mutable {
            return e.insertBatch<int64_t>(std::move(v)).discard_result();
        }));
    for (auto& [core, v] : bools)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v)](Engine& e) mutable {
            return e.insertBatch<bool>(std::move(v)).discard_result();
        }));
    for (auto& [core, v] : strings)
        pending.push_back(engines_.invoke_on(core, [v = std::move(v)](Engine& e) mutable {
            return e.insertBatch<std::string>(std::move(v)).discard_result();
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

seastar::future<data::MetadataResult> EngineLocalStore::queryMetadata(data::MetadataRequest req) {
    // Schema getters read the node's local schema cache (broadcast across its shards),
    // so one local() call yields this node's full owned-series schema. Cardinality is
    // per-shard HLL, summed across this node's shards.
    data::MetadataResult out;
    Engine& e = engines_.local();
    switch (req.kind) {
        case data::MetadataKind::Measurements: {
            out.items = co_await e.getAllMeasurements();
            break;
        }
        case data::MetadataKind::Fields: {
            // Carry the field TYPE alongside the name as "name\x1ftype" so the
            // coordinator's set-union preserves it (a field has one global type, so
            // identical pairs dedup). The handler splits on \x1f.
            auto s = co_await e.getMeasurementFields(req.measurement);
            for (const auto& f : s) {
                std::string type = co_await e.getIndex().getFieldType(req.measurement, f);
                out.items.push_back(f + std::string(1, '\x1f') + type);
            }
            break;
        }
        case data::MetadataKind::TagKeys: {
            auto s = co_await e.getMeasurementTags(req.measurement);
            out.items.assign(s.begin(), s.end());
            break;
        }
        case data::MetadataKind::TagValues: {
            auto s = co_await e.getTagValues(req.measurement, req.tagKey);
            out.items.assign(s.begin(), s.end());
            break;
        }
        case data::MetadataKind::MeasurementCardinality: {
            out.cardinality = co_await timestar::cluster::scatterAndSum(engines_, [m = req.measurement](Engine& eng) {
                return eng.getIndex().estimateMeasurementCardinality(m);
            });
            break;
        }
        case data::MetadataKind::TagCardinality: {
            out.cardinality = co_await timestar::cluster::scatterAndSum(
                engines_, [m = req.measurement, k = req.tagKey, v = req.tagValue](Engine& eng) {
                    return eng.getIndex().estimateTagCardinality(m, k, v);
                });
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

#include "engine_local_store.hpp"

#include "../../core/placement_table.hpp"
#include "../../index/index_backend.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <utility>

namespace timestar::cluster {

using ::MetadataOp;  // MetadataOp is in the global namespace (like TSMValueType/SeriesId128)

EngineLocalStore::EngineLocalStore(seastar::sharded<Engine>& engines) : engines_(engines) {}

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
        pending.push_back(engines_.invoke_on(
            core, [v = std::move(v)](Engine& e) mutable { return e.insertBatch<double>(std::move(v)).discard_result(); }));
    for (auto& [core, v] : ints)
        pending.push_back(engines_.invoke_on(
            core, [v = std::move(v)](Engine& e) mutable { return e.insertBatch<int64_t>(std::move(v)).discard_result(); }));
    for (auto& [core, v] : bools)
        pending.push_back(engines_.invoke_on(
            core, [v = std::move(v)](Engine& e) mutable { return e.insertBatch<bool>(std::move(v)).discard_result(); }));
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
    co_return co_await engines_.invoke_on(
        core, [seriesKey = std::move(seriesKey), start, end](Engine& e) mutable {
            return e.deleteRange(std::move(seriesKey), start, end);
        });
}

seastar::future<data::NodeQueryPartial> EngineLocalStore::queryLocal(data::NodeQueryRequest req) {
    http::HttpQueryHandler handler(&engines_);
    QueryResponse resp = co_await handler.executeQuery(std::move(req.request));
    data::NodeQueryPartial partial;
    if (!resp.success) {
        partial.incompleteReasons.push_back(resp.errorMessage.empty() ? std::string("query failed")
                                                                       : resp.errorMessage);
        co_return partial;
    }
    partial.series = std::move(resp.series);
    co_return partial;
}

seastar::future<> EngineLocalStore::applyRetention(std::string, uint64_t) {
    // Retention-cutoff application is wired in a later milestone (M1.x/M6); the
    // command type carries it, but the M2 write path does not use it yet.
    co_return;
}

}  // namespace timestar::cluster

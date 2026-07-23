#include "compact_vshard.hpp"

#include "../core/placement_table.hpp"  // virtualShard
#include "point_revision.hpp"
#include "tsm_result.hpp"
#include "tsm_writer.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <set>
#include <string>
#include <vector>

namespace timestar {

namespace {

// Resolve one series' LWW view across `files` (oldest-first) into (ts, values),
// accumulate the union of its input block revision ranges, and STREAM it to the
// output with that range stamped -- preserving revisions (vs migration's floor).
template <class T>
seastar::future<bool> compactSeries(TSMValueType type, const SeriesId128& seriesId,
                                    const std::vector<seastar::shared_ptr<::TSM>>& files, TSMWriter& writer) {
    std::map<uint64_t, T> resolved;
    RevisionRange range;
    for (const auto& file : files) {
        if (file->getSeriesType(seriesId) != type)
            continue;  // series absent or a different-typed generation
        // Union the series' block revision ranges in this file.
        ::TSMIndexEntry* entry = co_await file->getFullIndexEntry(seriesId);
        if (entry != nullptr) {
            for (const auto& block : entry->indexBlocks)
                range.merge(RevisionRange{block.blockMinRev, block.blockMaxRev, /*empty=*/false});
        }
        // Merge the points (last write wins; see snapshot read / migration).
        TSMResult<T> result(0);
        co_await file->readSeries<T>(seriesId, 0, UINT64_MAX, result);
        auto [timestamps, values] = result.getAllData();
        for (size_t i = 0; i < timestamps.size(); ++i)
            resolved[timestamps[i]] = std::move(values[i]);
    }
    if (resolved.empty())
        co_return false;

    std::vector<uint64_t> ts;
    std::vector<T> vals;
    ts.reserve(resolved.size());
    vals.reserve(resolved.size());
    for (auto& [t, v] : resolved) {
        ts.push_back(t);
        vals.push_back(std::move(v));
    }
    // Stamp the union range on the output blocks (preserves the file's max
    // revision so the recovery counter stays correct; see writeSeriesStreaming).
    co_await writer.writeSeriesStreaming<T>(type, seriesId, ts, vals, range);
    co_return true;
}

}  // namespace

seastar::future<size_t> compactVShardToFile(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> inputs,
                                            std::string outputPath) {
    std::sort(inputs.begin(), inputs.end(),
              [](const auto& a, const auto& b) { return a->dataRank() < b->dataRank(); });  // oldest-first

    std::set<SeriesId128> series;
    for (const auto& file : inputs) {
        for (const auto& sid : file->getSeriesIds()) {
            if (timestar::virtualShard(sid) == vshard.value())
                series.insert(sid);
        }
    }

    TSMWriter writer(outputPath);
    size_t written = 0;
    for (const auto& seriesId : series) {
        std::optional<TSMValueType> type;  // newest declared type (LWW winner)
        for (auto it = inputs.rbegin(); it != inputs.rend(); ++it) {
            if ((type = (*it)->getSeriesType(seriesId)))
                break;
        }
        if (!type)
            continue;

        bool wrote = false;
        switch (*type) {
            case TSMValueType::Float:
                wrote = co_await compactSeries<double>(*type, seriesId, inputs, writer);
                break;
            case TSMValueType::Integer:
                wrote = co_await compactSeries<int64_t>(*type, seriesId, inputs, writer);
                break;
            case TSMValueType::Boolean:
                wrote = co_await compactSeries<bool>(*type, seriesId, inputs, writer);
                break;
            case TSMValueType::String:
                wrote = co_await compactSeries<std::string>(*type, seriesId, inputs, writer);
                break;
        }
        if (wrote)
            ++written;
    }

    co_await writer.writeIndexStreaming();
    co_await writer.closeDMA();
    co_return written;
}

seastar::future<std::vector<std::pair<VShardId, std::string>>> partitionByVShard(
    std::vector<seastar::shared_ptr<::TSM>> inputs, std::function<std::string(VShardId)> pathForVShard) {
    std::set<uint16_t> vshards;
    for (const auto& file : inputs) {
        for (const auto& sid : file->getSeriesIds())
            vshards.insert(timestar::virtualShard(sid));
    }

    std::vector<std::pair<VShardId, std::string>> out;
    for (uint16_t vs : vshards) {  // ascending
        const VShardId id{vs};
        std::string path = pathForVShard(id);
        const size_t n = co_await compactVShardToFile(id, inputs, path);
        if (n > 0) {
            out.emplace_back(id, std::move(path));
        } else {
            // No surviving series for this VShard: compactVShardToFile still wrote
            // a header+index file. Remove it so it does not orphan on disk.
            co_await seastar::remove_file(path);
        }
    }
    co_return out;
}

}  // namespace timestar

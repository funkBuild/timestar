#include "migrate_vshard.hpp"

#include "../core/placement_table.hpp"  // virtualShard
#include "tsm_result.hpp"
#include "tsm_writer.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>
#include <vector>

namespace timestar {

namespace {

// Resolve one series' last-write-wins view across the given files (already sorted
// oldest-first) into (timestamps, values), then STREAM it to the output at the
// migrated floor (no revisions). `type` is the newest declared type; files where
// the series carries a DIFFERENT type are skipped -- a type-flipped series keeps
// only its newest-type data rather than aborting the whole migration on a block
// type mismatch. Returns whether anything was written.
template <class T>
seastar::future<bool> migrateSeries(TSMValueType type, const SeriesId128& seriesId,
                                    const std::vector<seastar::shared_ptr<::TSM>>& files, TSMWriter& writer) {
    std::map<uint64_t, T> resolved;
    for (const auto& file : files) {
        if (file->getSeriesType(seriesId) != type)
            continue;  // series absent, or a stale different-typed generation -> ignore
        TSMResult<T> result(0);
        co_await file->readSeries<T>(seriesId, 0, UINT64_MAX, result);
        auto [timestamps, values] = result.getAllData();
        for (size_t i = 0; i < timestamps.size(); ++i)
            resolved[timestamps[i]] = std::move(values[i]);  // last write wins (see snapshot read)
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
    // No revision range -> migrated-floor [0,0] blocks. Streamed to bound memory
    // and drain between blocks (offline migration may move a large VShard).
    co_await writer.writeSeriesStreaming<T>(type, seriesId, ts, vals);
    co_return true;
}

}  // namespace

seastar::future<size_t> migrateVShardToFile(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> sourceFiles,
                                            std::string outputPath) {
    // Oldest-first so keep-last assignment yields newest-wins across files.
    std::sort(sourceFiles.begin(), sourceFiles.end(),
              [](const auto& a, const auto& b) { return a->dataRank() < b->dataRank(); });

    // The VShard's series across all sources, in canonical order.
    std::set<SeriesId128> series;
    for (const auto& file : sourceFiles) {
        for (const auto& sid : file->getSeriesIds()) {
            if (timestar::virtualShard(sid) == vshard.value())
                series.insert(sid);
        }
    }

    TSMWriter writer(outputPath);
    size_t written = 0;
    for (const auto& seriesId : series) {
        // Resolve the type from the NEWEST file that declares the series (sources
        // are oldest-first, so scan from the back) -- the LWW winner's type.
        std::optional<TSMValueType> type;
        for (auto it = sourceFiles.rbegin(); it != sourceFiles.rend(); ++it) {
            if ((type = (*it)->getSeriesType(seriesId)))
                break;
        }
        if (!type)
            continue;

        bool wrote = false;
        switch (*type) {
            case TSMValueType::Float:
                wrote = co_await migrateSeries<double>(*type, seriesId, sourceFiles, writer);
                break;
            case TSMValueType::Integer:
                wrote = co_await migrateSeries<int64_t>(*type, seriesId, sourceFiles, writer);
                break;
            case TSMValueType::Boolean:
                wrote = co_await migrateSeries<bool>(*type, seriesId, sourceFiles, writer);
                break;
            case TSMValueType::String:
                wrote = co_await migrateSeries<std::string>(*type, seriesId, sourceFiles, writer);
                break;
        }
        if (wrote)
            ++written;
    }

    co_await writer.writeIndexStreaming();
    co_await writer.closeDMA();
    co_return written;
}

}  // namespace timestar

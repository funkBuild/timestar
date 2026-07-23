#include "vshard_snapshot_read.hpp"

#include "../core/placement_table.hpp"  // virtualShard
#include "tsm_result.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>

namespace timestar {

namespace {

// Read one series from one file and merge its points into `resolved` (ts -> value)
// by ASSIGNMENT, so the LAST write at each timestamp wins. This must match the
// query path's last-write-wins exactly (query_result.hpp dedupAdjacentKeepLast):
//   - INTRA-file: getAllData() concatenates blocks in append order, so a later
//     position is the newer write -- assignment keeps it (keep-last).
//   - CROSS-file: the caller processes files OLDEST-first, so a newer file's
//     value overwrites an older file's at a shared timestamp (newest generation
//     wins). keep-FIRST (emplace) would instead have returned the stale value.
template <class T>
seastar::future<> mergeSeriesFromFile(::TSM& file, const SeriesId128& seriesId, std::map<uint64_t, T>& resolved) {
    TSMResult<T> result(0);
    co_await file.readSeries<T>(seriesId, 0, UINT64_MAX, result);
    auto [timestamps, values] = result.getAllData();
    for (size_t i = 0; i < timestamps.size(); ++i)
        resolved[timestamps[i]] = std::move(values[i]);  // last write wins
}

}  // namespace

seastar::future<> feedVShardResolvedView(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> files,
                                         VShardSnapshotBuilder& builder) {
    // OLDEST generation first: mergeSeriesFromFile assigns (keep-last), so a newer
    // file processed later overwrites an older one at a shared timestamp (newest
    // wins), matching the query path's last-write-wins.
    std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) { return a->dataRank() < b->dataRank(); });

    // The VShard's series across all files, in canonical (byte) order for the hash.
    std::set<SeriesId128> series;
    for (const auto& file : files) {
        for (const auto& sid : file->getSeriesIds()) {
            if (timestar::virtualShard(sid) == vshard.value())
                series.insert(sid);
        }
    }

    for (const auto& seriesId : series) {
        // Determine the series type from whichever file declares it.
        std::optional<TSMValueType> type;
        for (const auto& file : files) {
            if ((type = file->getSeriesType(seriesId)))
                break;
        }
        if (!type)
            continue;

        switch (*type) {
            case TSMValueType::Float: {
                std::map<uint64_t, double> resolved;
                for (const auto& file : files)
                    co_await mergeSeriesFromFile<double>(*file, seriesId, resolved);
                for (const auto& [ts, v] : resolved)
                    builder.addFloat(seriesId, ts, v);
                break;
            }
            case TSMValueType::Integer: {
                std::map<uint64_t, int64_t> resolved;
                for (const auto& file : files)
                    co_await mergeSeriesFromFile<int64_t>(*file, seriesId, resolved);
                for (const auto& [ts, v] : resolved)
                    builder.addInteger(seriesId, ts, v);
                break;
            }
            case TSMValueType::Boolean: {
                std::map<uint64_t, bool> resolved;
                for (const auto& file : files)
                    co_await mergeSeriesFromFile<bool>(*file, seriesId, resolved);
                for (const auto& [ts, v] : resolved)
                    builder.addBoolean(seriesId, ts, v);
                break;
            }
            case TSMValueType::String: {
                std::map<uint64_t, std::string> resolved;
                for (const auto& file : files)
                    co_await mergeSeriesFromFile<std::string>(*file, seriesId, resolved);
                for (const auto& [ts, v] : resolved)
                    builder.addString(seriesId, ts, v);
                break;
            }
        }
    }

    co_return;
}

seastar::future<bool> verifyVShardSnapshot(const VShardSnapshotManifest& manifest,
                                           std::vector<seastar::shared_ptr<::TSM>> files) {
    VShardSnapshotBuilder builder(manifest.vshard);
    co_await feedVShardResolvedView(manifest.vshard, std::move(files), builder);
    co_return builder.verificationHash() == manifest.verificationHash;
}

}  // namespace timestar

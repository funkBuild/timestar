#pragma once

#include "series_id.hpp"

#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace timestar {

// Represents a query to be executed on a specific shard
struct ShardQuery {
    unsigned shardId;
    std::vector<SeriesId128> seriesIds;  // Series IDs to query on this shard
    std::set<std::string> fields;        // Fields to retrieve
    uint64_t startTime;
    uint64_t endTime;
};

}  // namespace timestar

#pragma once

#include <functional>
#include <map>
#include <string>
#include <vector>

namespace timestar {

struct GroupKeyResult {
    std::string key;
    size_t hash;
    std::map<std::string, std::string> tags;  // Only the group-by tags
};

// Build a composite group key directly from source tags (already sorted in std::map)
// and the group-by tag list, without creating an intermediate std::map for relevantTags.
//
// Key format: "measurement\0tag1=val1\0tag2=val2\0fieldName"
// Tags appear in sorted-key order (inherited from std::map iteration order).
inline GroupKeyResult buildGroupKeyDirect(const std::string& measurement, const std::string& fieldName,
                                          const std::map<std::string, std::string>& allTags,
                                          const std::vector<std::string>& groupByTags) {
    GroupKeyResult result;

    if (groupByTags.empty()) {
        // No group-by tags: key is just "measurement\0fieldName"
        size_t totalSize = measurement.size() + 1 + fieldName.size();
        result.key.reserve(totalSize);
        result.key.append(measurement);
        result.key += '\0';
        result.key.append(fieldName);
        result.hash = std::hash<std::string>{}(result.key);
        return result;
    }

    // Phase 1: Find matching tags by iterating the already-sorted allTags map.
    // Use stack buffer for up to 8 tags (covers 99.9% of real queries).
    struct TagMatch {
        const std::string* key;
        const std::string* value;
    };
    TagMatch matchBuf[8];
    std::vector<TagMatch> matchVec;
    TagMatch* matches = matchBuf;
    size_t matchCount = 0;
    size_t tagTotalBytes = 0;

    for (const auto& [k, v] : allTags) {
        // Linear scan of groupByTags (N is small, typically 1-3)
        bool found = false;
        for (const auto& gbt : groupByTags) {
            if (gbt == k) {
                found = true;
                break;
            }
        }
        if (found) {
            tagTotalBytes += 1 + k.size() + 1 + v.size();  // '\0' + key + '=' + value
            if (matchCount < 8) {
                matchBuf[matchCount++] = {&k, &v};
            } else {
                if (matchCount == 8) {
                    matchVec.assign(matchBuf, matchBuf + 8);
                }
                matchVec.push_back({&k, &v});
                matchCount++;
                matches = matchVec.data();
            }
        }
    }
    if (matchCount > 8)
        matches = matchVec.data();

    // Phase 2: Build key with pre-reserved capacity
    size_t totalSize = measurement.size() + tagTotalBytes + 1 + fieldName.size();
    result.key.reserve(totalSize);
    result.key.append(measurement);
    for (size_t i = 0; i < matchCount; ++i) {
        result.key += '\0';
        result.key.append(*matches[i].key);
        result.key += '=';
        result.key.append(*matches[i].value);
    }
    result.key += '\0';
    result.key.append(fieldName);

    // Phase 3: Compute hash
    result.hash = std::hash<std::string>{}(result.key);

    // Phase 4: Populate cachedTags from matched pointers
    for (size_t i = 0; i < matchCount; ++i) {
        result.tags[*matches[i].key] = *matches[i].value;
    }

    return result;
}

}  // namespace timestar

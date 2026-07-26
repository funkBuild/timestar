#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace timestar::data {

// The inter-node metadata request/result (integration plan M2 metadata scatter).
// In a VShard-partitioned cluster each node holds schema only for its owned series,
// so /measurements, /tags, /fields, /cardinality must scatter to every owner and
// merge. Because RF=1 partitions series DISJOINTLY across nodes, the merge is a
// string-set union for the list kinds and a SUM for cardinality (disjoint => exact,
// no HLL sketch needed on the wire).
enum class MetadataKind : uint8_t {
    Measurements = 0,            // all measurement names
    Fields = 1,                  // field names of `measurement`
    TagKeys = 2,                 // tag keys of `measurement`
    TagValues = 3,               // values of `measurement`/`tagKey`
    MeasurementCardinality = 4,  // series-count estimate of `measurement`
    TagCardinality = 5,          // series-count estimate of `measurement`/`tagKey`=`tagValue`
};

struct MetadataRequest {
    MetadataKind kind = MetadataKind::Measurements;
    std::string measurement;
    std::string tagKey;
    std::string tagValue;
};

struct MetadataResult {
    std::vector<std::string> items;  // list kinds
    double cardinality = 0.0;        // cardinality kinds (summed across owners)
};

// Bounds-checked, FNV-checksummed codecs; decode returns nullopt on ANY malformed
// input (same discipline as the other cluster codecs).
std::string encodeMetadataRequest(const MetadataRequest& req);
std::optional<MetadataRequest> decodeMetadataRequest(const std::string& bytes);
std::string encodeMetadataResult(const MetadataResult& res);
std::optional<MetadataResult> decodeMetadataResult(const std::string& bytes);

}  // namespace timestar::data

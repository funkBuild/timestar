#include "vshard_snapshot_builder.hpp"

#include <algorithm>

namespace timestar {

VShardSnapshotManifest VShardSnapshotBuilder::build(const VShardExtentMap& extents, std::string catalogHash,
                                                    std::vector<uint64_t> tombstoneObjectIds) const {
    VShardSnapshotManifest m;
    m.vshard = vshard_;
    m.dataExtents = extents.extents(vshard_);

    // snapshotRevision is the highest revision the snapshot captures = the max of
    // this VShard's extent ranges (0 when the VShard has no data).
    const RevisionRange range = extents.revRange(vshard_);
    m.snapshotRevision = range.empty ? 0 : range.maxRev;

    m.verificationHash = hash_.digestHex();
    m.catalogHash = std::move(catalogHash);

    m.tombstoneObjectIds = std::move(tombstoneObjectIds);
    // Tombstone ids must be strictly ascending for valid(); the caller supplies a
    // set of ids, so sort+unique defensively rather than trust ordering.
    std::sort(m.tombstoneObjectIds.begin(), m.tombstoneObjectIds.end());
    m.tombstoneObjectIds.erase(std::unique(m.tombstoneObjectIds.begin(), m.tombstoneObjectIds.end()),
                               m.tombstoneObjectIds.end());
    return m;
}

}  // namespace timestar

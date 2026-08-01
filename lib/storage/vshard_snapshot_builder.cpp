#include "vshard_snapshot_builder.hpp"

#include <algorithm>

namespace timestar {

VShardSnapshotManifest VShardSnapshotBuilder::build(const VShardExtentMap& extents, std::string catalogHash,
                                                    std::vector<uint64_t> tombstoneObjectIds) const {
    VShardSnapshotManifest m;
    m.vshard = vshard_;
    m.dataExtents = extents.extents(vshard_);

    // The storage builder emits the minimal fence: the max of this VShard's
    // extent ranges (0 when it has no data). The replicated producer can promote
    // it after proving a later applied prefix has no surviving unflushed point.
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

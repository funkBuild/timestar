#include "journal_segment.hpp"

#include <algorithm>

namespace timestar {

void JournalSegmentHeader::encodeInto(std::string& out) const {
    codec::putU32(out, kMagic);
    codec::putU32(out, kFormatVersion);
    out.append(reinterpret_cast<const char*>(clusterUuid.data()), clusterUuid.size());
    codec::putU32(out, coreNumber);
    codec::putU64(out, segmentNumber);
    out.append(reinterpret_cast<const char*>(bootId.data()), bootId.size());
}

std::string JournalSegmentHeader::encode() const {
    std::string out;
    out.reserve(kEncodedBytes);
    encodeInto(out);
    return out;
}

std::optional<JournalSegmentHeader> JournalSegmentHeader::decode(codec::Reader& reader) {
    const uint32_t magic = reader.u32();
    const uint32_t version = reader.u32();
    if (!reader.ok() || magic != kMagic || version != kFormatVersion)
        return std::nullopt;

    JournalSegmentHeader header;
    const std::string cluster = reader.bytes(header.clusterUuid.size());
    header.coreNumber = reader.u32();
    header.segmentNumber = reader.u64();
    const std::string boot = reader.bytes(header.bootId.size());
    if (!reader.ok())
        return std::nullopt;

    std::copy(cluster.begin(), cluster.end(), reinterpret_cast<char*>(header.clusterUuid.data()));
    std::copy(boot.begin(), boot.end(), reinterpret_cast<char*>(header.bootId.data()));
    return header;
}

std::optional<JournalSegmentScan> scanJournalSegment(std::span<const char> data) {
    codec::Reader reader(data);
    auto header = JournalSegmentHeader::decode(reader);
    if (!header)
        return std::nullopt;  // invalid header is a hard error, not a torn tail

    JournalSegmentScan scan;
    scan.header = *header;

    size_t offset = reader.consumed();
    scan.durableBytes = offset;
    while (offset < data.size()) {
        size_t consumed = 0;
        auto record =
            JournalRecord::decode(std::span<const char>(data.data() + offset, data.size() - offset), consumed);
        if (!record) {
            // A partial or CRC-failing trailing record: the durable tail ends at
            // the previous record. Everything before it is intact.
            scan.torn = true;
            break;
        }
        scan.records.push_back(std::move(*record));
        offset += consumed;
        scan.durableBytes = offset;
    }
    return scan;
}

}  // namespace timestar

#include "snapshot_pin_set.hpp"

#include "byte_codec.hpp"
#include "crc32.hpp"

#include <limits>
#include <span>

namespace timestar {

namespace {
constexpr uint32_t kMagic = 0x53504e53;  // "SPNS"
constexpr uint32_t kFormatVersion = 1;
}  // namespace

void SnapshotPinSet::pin(std::string_view holder, const SnapshotPin& pin) {
    pins_[std::string(holder)] = pin;  // re-pin replaces (renew)
}

bool SnapshotPinSet::release(std::string_view holder) {
    return pins_.erase(std::string(holder)) != 0;
}

std::optional<SnapshotPin> SnapshotPinSet::pinOf(std::string_view holder) const {
    auto it = pins_.find(std::string(holder));
    if (it == pins_.end())
        return std::nullopt;
    return it->second;
}

uint64_t SnapshotPinSet::gcFloor(uint64_t logicalNow) const {
    uint64_t floor = std::numeric_limits<uint64_t>::max();
    for (const auto& [holder, pin] : pins_) {
        if (logicalNow < pin.leaseDeadline)  // live
            floor = std::min(floor, pin.snapshotRevision);
    }
    return floor;
}

bool SnapshotPinSet::isPinned(uint64_t revision, uint64_t logicalNow) const {
    for (const auto& [holder, pin] : pins_) {
        if (logicalNow < pin.leaseDeadline && pin.snapshotRevision <= revision)
            return true;
    }
    return false;
}

size_t SnapshotPinSet::evictExpired(uint64_t logicalNow) {
    size_t removed = 0;
    for (auto it = pins_.begin(); it != pins_.end();) {
        if (logicalNow >= it->second.leaseDeadline) {
            it = pins_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}

std::string SnapshotPinSet::encode() const {
    std::string out;
    codec::putU32(out, kMagic);
    codec::putU32(out, kFormatVersion);
    codec::putU32(out, static_cast<uint32_t>(pins_.size()));
    for (const auto& [holder, pin] : pins_) {  // std::map -> ascending holder
        codec::putStr(out, holder);
        codec::putU64(out, pin.snapshotRevision);
        codec::putU64(out, pin.leaseDeadline);
    }
    const uint32_t crc = CRC32::compute(out.data(), out.size());
    codec::putU32(out, crc);
    return out;
}

std::optional<SnapshotPinSet> SnapshotPinSet::decode(std::string_view bytes) {
    constexpr size_t kCrcBytes = 4;
    if (bytes.size() < kCrcBytes)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - kCrcBytes;
    codec::Reader crcReader(std::span<const char>(bytes.data() + bodyLen, kCrcBytes));
    const uint32_t storedCrc = crcReader.u32();
    if (!crcReader.ok() || CRC32::compute(bytes.data(), bodyLen) != storedCrc)
        return std::nullopt;

    codec::Reader r(std::span<const char>(bytes.data(), bodyLen));
    const uint32_t magic = r.u32();
    const uint32_t version = r.u32();
    const uint32_t count = r.u32();
    if (!r.ok() || magic != kMagic || version != kFormatVersion)
        return std::nullopt;

    SnapshotPinSet set;
    std::string prevHolder;
    bool havePrev = false;
    for (uint32_t i = 0; i < count; ++i) {
        std::string holder = r.str();
        SnapshotPin pin;
        pin.snapshotRevision = r.u64();
        pin.leaseDeadline = r.u64();
        if (!r.ok())
            return std::nullopt;
        if (holder.empty())
            return std::nullopt;  // a pin must name a holder
        if (havePrev && holder <= prevHolder)
            return std::nullopt;  // ordered, unique holder
        havePrev = true;
        prevHolder = holder;
        set.pins_.emplace(std::move(holder), pin);
    }
    if (!r.exhausted())
        return std::nullopt;
    return set;
}

}  // namespace timestar

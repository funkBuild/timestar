#include "vshard_mem_accounting.hpp"

#include <algorithm>

namespace timestar {

VShardMemoryAccounting::VShardMemoryAccounting(uint64_t perVShardRolloverBytes, uint64_t globalRolloverBytes)
    : perVShardCap_(perVShardRolloverBytes), globalCap_(globalRolloverBytes) {}

void VShardMemoryAccounting::add(VShardId vshard, uint64_t bytes, uint64_t points) {
    auto& usage = byVShard_[vshard.value()];
    usage.bytes += bytes;
    usage.points += points;
    totalBytes_ += bytes;
    totalPoints_ += points;
}

VShardMemUsage VShardMemoryAccounting::usage(VShardId vshard) const {
    auto it = byVShard_.find(vshard.value());
    return it == byVShard_.end() ? VShardMemUsage{} : it->second;
}

bool VShardMemoryAccounting::vshardNeedsRollover(VShardId vshard) const {
    if (perVShardCap_ == 0)
        return false;
    return usage(vshard).bytes >= perVShardCap_;
}

bool VShardMemoryAccounting::globalNeedsRollover() const noexcept {
    if (globalCap_ == 0)
        return false;
    return totalBytes_ >= globalCap_;
}

std::vector<VShardId> VShardMemoryAccounting::overCapVShards() const {
    std::vector<VShardId> over;
    if (perVShardCap_ == 0)
        return over;
    for (const auto& [vs, usage] : byVShard_) {
        if (usage.bytes >= perVShardCap_)
            over.push_back(VShardId{vs});
    }
    std::sort(over.begin(), over.end());  // ascending id for deterministic handling
    return over;
}

void VShardMemoryAccounting::reset(VShardId vshard) {
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return;
    totalBytes_ -= it->second.bytes;
    totalPoints_ -= it->second.points;
    byVShard_.erase(it);
}

void VShardMemoryAccounting::resetAll() {
    byVShard_.clear();
    totalBytes_ = 0;
    totalPoints_ = 0;
}

}  // namespace timestar

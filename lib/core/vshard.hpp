#pragma once

#include <cstdint>

namespace timestar {

inline constexpr uint16_t VIRTUAL_SHARD_COUNT = 4096;
inline constexpr uint16_t VIRTUAL_SHARD_MASK = VIRTUAL_SHARD_COUNT - 1;

static_assert((VIRTUAL_SHARD_COUNT & VIRTUAL_SHARD_MASK) == 0, "VShard count must remain a power of two");

}  // namespace timestar

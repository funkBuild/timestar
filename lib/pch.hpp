#pragma once

// Precompiled header for libtimestar.
//
// Contains only headers that are (a) included by a large fraction of the
// library's translation units and (b) expensive to parse — chiefly the Seastar
// async core and the common STL containers. Parsing these once into a binary
// PCH and loading it per-TU is much cheaper than re-parsing them ~25-90 times.
//
// Deliberately excluded:
//   - glaze/json.hpp: heavy metaprogramming, but only 6 TUs use it; putting it
//     here would force the other ~80 TUs to load it for nothing.
//   - Highway headers: the SIMD encoders re-include themselves via Highway's
//     foreach_target mechanism and manage their own include order.

// ── Seastar async core (≈ every async TU; very expensive to parse) ──
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

// ── Common standard library ──
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

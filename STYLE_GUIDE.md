# TimeStar C++ Style Guide

Conventions extracted from the actual codebase. Follow these when contributing.

## Naming

| Element | Convention | Examples |
|---------|-----------|----------|
| Files | `snake_case.hpp` / `snake_case.cpp` | `native_index.hpp`, `query_runner.cpp` |
| Classes | `PascalCase` | `NativeIndex`, `TSMFileManager`, `HttpQueryHandler` |
| Methods | `camelCase` | `getOrCreateSeriesId()`, `readSparseIndex()` |
| Member variables | `camelCase` or `_prefixed` | `sparseIndex`, `_insertGate`, `memtable_` |
| Local variables | `camelCase` | `seriesId`, `startTime`, `tagFilters` |
| Constants | `UPPER_SNAKE_CASE` | `SSTABLE_MAGIC`, `MAX_TAG_VALUES_CACHE_ENTRIES` |
| Enums | `PascalCase` values | `TSMValueType::Float`, `IndexKeyType::SERIES_INDEX` |
| Namespaces | `lowercase` or `snake_case` | `timestar`, `timestar::index`, `timestar::index::simd` |

**Member variable convention is mixed.** The `Engine` class uses `_prefix` for Seastar infrastructure (`_insertGate`, `_metrics`) and bare `camelCase` for domain objects (`tsmFileManager`, `index`). The `NativeIndex` class uses `trailing_` for private members (`memtable_`, `shardId_`). Either convention is acceptable; be consistent within a class.

## File Organization

```cpp
#pragma once                           // Always; never use include guards

#include "project_header.hpp"          // Project headers first (unquoted path)
#include "another_project_header.hpp"

#include <tsl/robin_map.h>            // Third-party headers
#include <roaring.hh>
#include <glaze/glaze.hpp>

#include <map>                         // Standard library
#include <memory>
#include <seastar/core/future.hh>      // Seastar headers (alphabetical within group)
#include <seastar/core/gate.hh>
#include <vector>
```

Project headers use quotes with flat paths (CMake sets the include dirs). Standard and Seastar headers use angle brackets. Within each group, alphabetical order.

## Namespaces

- Core infrastructure lives in `timestar` namespace (`config()`, loggers, `SubscriptionManager`).
- Index subsystem uses `timestar::index` with `timestar::index::simd` for SIMD kernels.
- Query/HTTP types live in `timestar` namespace (`SeriesResult`, `QueryResponse`, `HttpQueryHandler`).
- Older storage classes (`TSM`, `BoolEncoder`, `Engine`) are in the global namespace.
- Use `namespace timestar::index {` (C++17 nested syntax), not separate nesting.
- Close with `}  // namespace timestar::index` comment (two spaces before `//`).

## C++ Standard (C++23)

Features actively used:
- **`co_await` coroutines** -- all async I/O returns `seastar::future<>` and uses `co_await`/`co_return`
- **`std::expected`** -- used for operations that can legitimately fail (`findSeries` returns `std::expected<vector, SeriesLimitExceeded>`)
- **`std::optional`** -- preferred over sentinel values for "not found" (`getSeriesId`, `getRetentionPolicy`)
- **`auto operator<=>(const T&) const = default`** -- defaulted spaceship operator (`SeriesId128`)
- **`std::span`** -- zero-copy view parameters (`std::span<const double>` in encoders)
- **Structured bindings** -- `auto [timestamps, values] = results.getAllData();` and `for (const auto& [k, v] : tags)`
- **`if constexpr`** -- compile-time type dispatch in template methods (`getValueType<T>()`)
- **`constexpr` functions and tables** -- CRC32 table generation, Glaze metadata
- **`std::erase_if`** -- C++20 container erasure

## Seastar Patterns

### Coroutines
All I/O operations return `seastar::future<>` and are awaited with `co_await`:
```cpp
seastar::future<> Engine::init() {
    co_await createDirectoryStructure();
    co_await index.open();
    co_await tsmFileManager.init();
}
```

### Gate-Based Shutdown
Use `seastar::gate` to track in-flight operations and drain them during shutdown:
```cpp
// In the operation:
auto holder = _insertGate.hold();  // RAII -- released when holder goes out of scope
// ... do work ...

// In stop():
co_await _insertGate.close();  // Waits for all holders to release
```

Multiple gates per class are normal (`_insertGate`, `_streamingGate`, `_retentionGate` in `Engine`).

### Shard-Per-Core
- No atomics, no mutexes. Each shard owns its data exclusively.
- Cross-shard communication uses `invoke_on()` / `invoke_on_all()` / `submit_to()`.
- Schema metadata is broadcast via `invoke_on_all` for local cache reads.
- Data is routed to the owning shard via `timestar::routeToCore()`.

### Scheduling Groups
Three I/O priority classes: `_queryGroup`, `_writeGroup`, `_compactionGroup`. Created once in `main()`, distributed via `invoke_on_all`. Use `seastar::with_scheduling_group()` to prioritize query I/O over background work.

### Blocking Future Resolution in Tests
Tests run inside the Seastar reactor. Async test helpers return `seastar::future<>` and are resolved with `.get()` in Google Test:
```cpp
TEST_F(TSMSeastarTest, ReadFloatData) {
    testTSMReadFloat(getTestFilePath("0_1.tsm")).get();
}
```

## Error Handling

- **`std::optional`** for "not found" lookups (`getSeriesId`, `getFieldStats`, `query`).
- **`std::expected<T, E>`** for bounded operations that may exceed limits (`findSeries` with `SeriesLimitExceeded`).
- **Exceptions** (`std::runtime_error`) for unrecoverable errors (file I/O failures, corrupt data). Caught at HTTP handler boundaries and converted to JSON error responses.
- **`seastar::gate_closed_exception`** is expected during shutdown; catch and ignore it.

## SIMD (Google Highway)

SIMD files follow Highway's multi-compilation pattern. Each `.cpp` is compiled once per ISA target:

```cpp
// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "index/key_encoding_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "hwy/highway.h"

HWY_BEFORE_NAMESPACE();
namespace timestar::index::simd {
namespace HWY_NAMESPACE {
namespace hn = hwy::HWY_NAMESPACE;

// SIMD kernel (compiled once per target: SSE4, AVX2, AVX-512, etc.)
size_t FindFirstEscapeCharKernel(const char* HWY_RESTRICT data, size_t len) { ... }

}  // namespace HWY_NAMESPACE
}  // namespace timestar::index::simd
HWY_AFTER_NAMESPACE();

// Dispatch (compiled once)
#if HWY_ONCE
namespace timestar::index::simd {
HWY_EXPORT(FindFirstEscapeCharKernel);
size_t findFirstEscapeChar(const char* data, size_t len) {
    return HWY_DYNAMIC_DISPATCH(FindFirstEscapeCharKernel)(data, len);
}
}  // namespace timestar::index::simd
#endif
```

SIMD headers declare the public dispatch function. The kernel name is `PascalCase`, the dispatch wrapper is `camelCase`.

Encoder pattern: `FloatEncoderBasic` (scalar), `FloatEncoderSIMD` (Highway-dispatched), `FloatEncoderAVX512` (explicit intrinsics). All expose a static `encode()` / `decode()` interface.

## Logging

### Logger Hierarchy
Defined in `lib/utils/logger.hpp` as `inline seastar::logger` instances:
```
timestar.engine, timestar.tsm, timestar.wal, timestar.memory,
timestar.index, timestar.metadata, timestar.compactor, timestar.http, timestar.query
```

### Compile-Time Path Logging
`lib/utils/logging_config.hpp` defines `LOG_INSERT_PATH` and `LOG_QUERY_PATH` macros that compile to no-ops when disabled (default). Enable via `-DTIMESTAR_LOG_INSERT_PATH=1`.

### Slow Query Detection
Always compiled in, controlled by `http.slow_query_threshold_ms` config (default 500ms). Logged as:
```
[SLOW_QUERY] 1234.5ms (threshold 500ms) | measurement=cpu series=42 points=100000
```

## Testing

### Framework
Google Test. No custom Seastar test macros -- tests use `::testing::Test` base class with `SetUp`/`TearDown` for temp directory management.

### File Naming
`test/unit/{component}/{descriptive_name}_test.cpp` -- e.g., `test/unit/storage/tsm_seastar_test.cpp`, `test/unit/query/aggregator_test.cpp`.

### Async Test Pattern
Write a free function returning `seastar::future<>`, call `.get()` from the `TEST_F`:
```cpp
seastar::future<> testTSMReadFloat(std::string filename) {
    // ... co_await operations, EXPECT_* assertions ...
}
TEST_F(TSMSeastarTest, ReadFloatData) {
    testTSMReadFloat(getTestFilePath("0_1.tsm")).get();
}
```

### Temporary Files
Tests create directories in `SetUp()` and `fs::remove_all()` in `TearDown()`. Use relative paths from the build directory.

## Configuration

### TOML Config (`lib/config/timestar_config.hpp`)
- Config structs are plain aggregates with sensible defaults.
- Glaze metadata (`glz::meta<T>`) for TOML serialization is declared alongside the struct.
- Nested config: `TimestarConfig` contains `ServerConfig`, `StorageConfig`, `HttpConfig`, `IndexConfig`, `EngineConfig`, `StreamingConfig`.
- Global singleton: `setGlobalConfig()` called once before reactor start; `timestar::config()` reads from any shard (lock-free).

### Environment Overrides
`TIMESTAR_` prefixed env vars override TOML values. Pattern: `TIMESTAR_{SECTION}_{FIELD}` in upper snake case (e.g., `TIMESTAR_HTTP_MAX_SERIES_COUNT`).

### Validation
`TimestarConfig::validate()` returns a `vector<string>` of errors (empty = valid). Called after load + env overrides.

## Data Structures

- **`tsl::robin_map`** for hot-path hash maps (bitmap caches, sparse index). Faster than `std::unordered_map`.
- **`std::map`** for ordered data (tags are always sorted).
- **`roaring::Roaring`** for set membership (posting lists, day bitmaps).
- **`std::unordered_set` / `std::unordered_map`** for general-purpose non-critical-path maps.

## Encoder Pattern

Encoders are stateless classes with static methods:
```cpp
class BoolEncoder {
public:
    static AlignedBuffer encode(const std::vector<bool>& values);
    static void decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out);
};
```

No instances are created. The class is just a namespace with access control.

## HTTP Handler Pattern

Each handler is a class that takes `seastar::sharded<Engine>*` in its constructor:
```cpp
class HttpQueryHandler {
    seastar::sharded<Engine>* engineSharded;
public:
    explicit HttpQueryHandler(seastar::sharded<Engine>* engine, ...);
    seastar::future<std::unique_ptr<seastar::http::reply>> handleQuery(std::unique_ptr<seastar::http::request> req);
    void registerRoutes(seastar::httpd::routes& r);
};
```

Config-derived limits are accessed via static methods reading from `timestar::config()`.

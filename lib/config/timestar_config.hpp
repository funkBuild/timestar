#pragma once

#include <glaze/json.hpp>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace timestar {

struct ServerConfig {
    uint16_t port = 8086;
    std::string log_level = "info";
    std::string data_dir = ".";
    uint32_t shutdown_timeout_seconds = 30;  // 0 = wait forever
    bool auth_enabled = false;
    std::string auth_token;  // Empty = auto-generate on startup when auth_enabled
};

struct CompactionConfig {
    uint32_t max_concurrent = 2;
    uint64_t max_memory = 256 * 1024 * 1024;
    uint32_t batch_size = 10000;
    // Files merged per compaction, uniform across ALL tiers (replaces the old
    // tier0/1/2_min_files trio, of which only tier0's was ever read). Both the
    // merge TRIGGER (a tier is eligible once it holds this many files) and the
    // merge INPUT SIZE (exactly this many oldest files, never more): with 6
    // files in a tier, the oldest 4 merge and the newest 2 wait for 2 more.
    // Merging "whatever has accumulated" made tier-file sizes wander by 2-4x;
    // fixed-count merges keep them geometrically consistent (a tier-N file is
    // ~files_per_merge tier-(N-1) files), which is what makes tier depth a
    // meaningful signal for block sizing and reclaim-rate scheduling.
    uint32_t files_per_merge = 4;
    // Tier-0 file count at which compaction stops yielding to pending WAL->TSM
    // conversion. WAL drain normally wins (it frees disk and keeps ingest
    // flowing), but an unbounded tier 0 degrades every query on the shard, so
    // this caps how far read amplification can drift during a long burst.
    uint32_t tier0_starvation_ceiling = 32;
    // Tier-0 file count at which INGEST is shed (503 + Retry-After). At high
    // cardinality every tier-0 file carries a multi-MB sparse index, so an
    // unbounded backlog is an unbounded memory commitment: a soak at 3x the
    // target rate grew tier 0 to 268 files, whose indexes exhausted the memory
    // pool and turned merges and conversions themselves into bad_alloc
    // failures -- at which point the backlog could never drain. Shedding at
    // twice the starvation ceiling gives merges a full-priority window
    // [starvation_ceiling .. this] to catch up before writes are refused; a
    // client that honours Retry-After converges on the rate the WHOLE
    // pipeline (parse + WAL + convert + merge) sustains, not just its front.
    uint32_t tier0_shed_ceiling = 64;
    // Ceiling for the per-tier output block size: compaction writes tier-T
    // outputs with blocks of up to min(max_points_per_block << T, this).
    // High-cardinality workloads flush files whose per-series blocks hold only
    // a few dozen points (128k series at a 64MB store = ~50 points/series);
    // merges are the only chance to consolidate them, and deeper tiers hold
    // data that is rewritten rarely but scanned a lot -- bigger blocks there
    // buy compression ratio and fewer index entries. The ceiling bounds the
    // decode cost of touching one block on the query path.
    uint32_t deep_block_points_cap = 24000;
};

struct StorageConfig {
    // Per-store WAL segment size, which is also the store rollover trigger
    // (with the resident-bytes ceiling at 4x this). 64MB, up from 16MB: at
    // high cardinality the WAL estimate is dominated by ~120B/series-insert
    // framing, so 16MB rolled a 128k-series store every couple of batches --
    // constant conversion churn, ~2-point flush blocks, and 10x the rollover
    // rate for no durability gain. 64MB is the value every validated
    // endurance run used (3B pts / 128k series / 6GB: 21.1M pts/s import,
    // zero errors, RSS ~1.1GB). Memory cost: retained stores are bounded by
    // admission at 16 stores/shard worst case.
    uint64_t wal_size_threshold = 64 * 1024 * 1024;
    uint32_t max_points_per_block = 3000;
    double tsm_bloom_fpr = 0.001;
    uint32_t tsm_cache_entries = 4096;
    uint32_t wal_max_concurrent_encoders = 4;  // Max concurrent WAL encoding coroutines per shard
    // Concurrent WAL->TSM conversions per shard. Was hard-coded to 1, which made
    // conversion the ingest ceiling: conversion is the only thing that frees
    // retained-store RAM, so ITS throughput -- not tier-merge throughput -- sets
    // the sustainable ingest rate.
    //
    // Measured, 1.3B points / 2 shards / 8 connections on NVMe, sweeping this
    // value (13000 batches, accepted vs shed, latency over the whole run):
    //
    //    conv   pts/s   accepted   shed    p99      max
    //      1    2.66M    3110/13000  76%   250ms    726ms
    //      3    7.09M    8995/13000  31%   281ms    748ms
    //      6    9.69M   11322/13000  13%    67ms    124ms   <-- optimum
    //     12    8.45M   10796/13000  17%   325ms   1105ms
    //
    // A clear inverted U. Below the knee conversions serialise behind each
    // other's I/O waits and the backlog forces writes to be shed; above it they
    // contend for disk and CPU, and both latency and throughput regress. 6 wins
    // on every axis simultaneously -- 3.6x the throughput of the old hard-coded
    // 1, with p99 cut from 250ms to 67ms.
    //
    // This is disk-dependent: measured on tmpfs the sweep is flat, because
    // RAM-backed I/O never blocks, so a single fiber keeps up. Re-measure on the
    // target storage before changing it.
    uint32_t conversion_concurrency = 6;
    // Free-memory floor for accepting writes: below this per-shard free
    // memory, ingest is shed with 503 + Retry-After. The last line of defence
    // against bad_alloc storms of ANY origin -- once allocation fails inside
    // the write path (or worse, inside a conversion or merge), the failure
    // mode is data loss and a backlog that can no longer drain; a shed
    // request is retryable and costs nothing. 256MB leaves room for the
    // in-flight parse transients the parse budget already bounds.
    uint64_t ingest_min_free_bytes = 256ull * 1024 * 1024;
    CompactionConfig compaction;
};

struct HttpConfig {
    uint64_t max_write_body_size = 64 * 1024 * 1024;
    uint64_t max_query_body_size = 1 * 1024 * 1024;
    uint32_t max_series_count = 10000;
    uint64_t max_total_points = 10000000;
    uint32_t query_timeout_seconds = 30;
    uint32_t slow_query_threshold_ms = 500;  // Log queries slower than this (0 = disabled)
};

struct IndexConfig {
    uint32_t bloom_filter_bits = 15;
    uint32_t block_size = 16384;
    uint64_t write_buffer_size = 16 * 1024 * 1024;
    uint32_t max_open_files = 1000;
    uint64_t max_file_size = 64 * 1024 * 1024;
    uint64_t series_cache_size = 1000000;
    uint64_t metadata_cache_bytes = 48 * 1024 * 1024;   // 48MB for series metadata LRU cache
    uint64_t discovery_cache_bytes = 16 * 1024 * 1024;  // 16MB for discovery result LRU cache
    uint64_t block_cache_bytes = 8 * 1024 * 1024;       // 8MB per shard for SSTable block cache
    uint32_t compaction_rate_limit_mbps = 0;            // Max compaction write MB/s (0 = unlimited)
};

struct StreamingConfig {
    uint32_t max_subscriptions_per_shard = 100;
    uint32_t output_queue_size = 1024;
    uint32_t heartbeat_interval_seconds = 15;
};

// I/O scheduling shares for Seastar's fair queue. Higher shares = more disk
// bandwidth under contention. When only one class has pending I/O, it gets
// full bandwidth regardless of share count.
struct IOPriorityConfig {
    float query_shares = 100.0f;  // TSM/index reads during queries
    float write_shares = 50.0f;   // WAL writes, memtable flushes
    // Background TSM/SSTable compaction. Shares are a PROPORTION under
    // contention, not a cap: an idle reactor still gives compaction 100%.
    // 333 holds ~25% against a saturated `main` (333/1333), ~22% with
    // ts_flush also runnable (333/1533). The old value of 10 rounded to
    // 0.8% under sustained foreground load -- effectively starvation: a
    // parse-saturated shard completed 9 merges while conversions added ~25
    // files/min, and the resulting tier-0 backlog's sparse indexes exhausted
    // the shard's memory pool. A merge burst can still never take more than
    // its proportional slice from a busy foreground, so the write-latency
    // cost only appears when compaction genuinely has work during saturation
    // -- exactly when falling behind is the worse outcome.
    float compaction_shares = 333.0f;
    // WAL->TSM conversion. Deliberately ABOVE compaction_shares: conversion
    // frees retained-store RAM and WAL disk, so when merges and conversions
    // genuinely contend (the tier-0 starvation-ceiling window, where the
    // WAL-first policy deferral is bypassed) drain must still win. 500 holds
    // ~33% against a saturated main. In normal operation the policy layer
    // (compactionYieldReason) keeps merges out of conversion's way entirely;
    // the share gap only matters when that policy is deliberately overridden.
    float flush_shares = 500.0f;
};

struct EngineConfig {
    uint32_t metadata_retry_interval_seconds = 5;
    uint32_t max_metadata_retry_ops = 10000;
    uint32_t retention_sweep_interval_minutes = 15;
    double tombstone_dead_fraction_threshold = 0.10;
    uint32_t max_tombstone_rewrites_per_sweep = 2;
    IOPriorityConfig io_priority;
};

// Seastar settings parsed from [seastar] TOML section.
// Stored as string key-value pairs; only keys present in the file are set.
// Keys use underscore naming (matching TOML), and are converted to Seastar's
// hyphenated CLI names during injection.
struct SeastarConfig {
    std::map<std::string, std::string> settings;

    bool has(const std::string& key) const { return settings.count(key) > 0; }
    const std::string& get(const std::string& key) const { return settings.at(key); }
};

// Internal struct for Glaze TOML parsing (excludes SeastarConfig)
struct TimestarConfigParseable {
    ServerConfig server;
    StorageConfig storage;
    HttpConfig http;
    IndexConfig index;
    EngineConfig engine;
    StreamingConfig streaming;
};

struct TimestarConfig {
    ServerConfig server;
    StorageConfig storage;
    HttpConfig http;
    IndexConfig index;
    EngineConfig engine;
    StreamingConfig streaming;
    SeastarConfig seastar;

    // Validate config values. Returns a list of error strings (empty = valid).
    std::vector<std::string> validate() const;
};

// Load config from a TOML file. Throws std::runtime_error on parse/validation failure.
TimestarConfig loadConfigFile(const std::string& path);

// Return the default config as a TOML string.
std::string dumpDefaultConfig();

// Apply TIMESTAR_* environment variable overrides onto an existing config.
// Called after loading config file but before setGlobalConfig().
void applyEnvironmentOverrides(TimestarConfig& cfg);

// Set the global config (call once in main before app.run()).
void setGlobalConfig(const TimestarConfig& cfg);

// Read the global config from any shard (lock-free, set once before reactor starts).
const TimestarConfig& config();

// Normalized data root directory derived from config().server.data_dir.
// Trailing slashes are stripped (a bare "/" is preserved); an empty value
// normalizes to "." (the process working directory). Never empty.
std::string dataRootPath();

// Directory holding one shard's data: "<data_dir>/shard_<id>".
// With the default data_dir of "." this returns the legacy CWD-relative
// "shard_<id>" so existing deployments and tests are unaffected.
std::string shardDataPath(unsigned shardId);

}  // namespace timestar

// Glaze metadata for TOML serialization

template <>
struct glz::meta<timestar::ServerConfig> {
    using T = timestar::ServerConfig;
    static constexpr auto value =
        object("port", &T::port, "log_level", &T::log_level, "data_dir", &T::data_dir, "shutdown_timeout_seconds",
               &T::shutdown_timeout_seconds, "auth_enabled", &T::auth_enabled, "auth_token", &T::auth_token);
};

template <>
struct glz::meta<timestar::CompactionConfig> {
    using T = timestar::CompactionConfig;
    static constexpr auto value =
        object("max_concurrent", &T::max_concurrent, "max_memory", &T::max_memory, "batch_size", &T::batch_size,
               "files_per_merge", &T::files_per_merge, "tier0_starvation_ceiling", &T::tier0_starvation_ceiling,
               "tier0_shed_ceiling", &T::tier0_shed_ceiling, "deep_block_points_cap", &T::deep_block_points_cap);
};

template <>
struct glz::meta<timestar::StorageConfig> {
    using T = timestar::StorageConfig;
    static constexpr auto value = object(
        "wal_size_threshold", &T::wal_size_threshold, "max_points_per_block", &T::max_points_per_block, "tsm_bloom_fpr",
        &T::tsm_bloom_fpr, "tsm_cache_entries", &T::tsm_cache_entries, "wal_max_concurrent_encoders",
        &T::wal_max_concurrent_encoders, "conversion_concurrency", &T::conversion_concurrency, "ingest_min_free_bytes",
        &T::ingest_min_free_bytes, "compaction", &T::compaction);
};

template <>
struct glz::meta<timestar::HttpConfig> {
    using T = timestar::HttpConfig;
    static constexpr auto value = object(
        "max_write_body_size", &T::max_write_body_size, "max_query_body_size", &T::max_query_body_size,
        "max_series_count", &T::max_series_count, "max_total_points", &T::max_total_points, "query_timeout_seconds",
        &T::query_timeout_seconds, "slow_query_threshold_ms", &T::slow_query_threshold_ms);
};

template <>
struct glz::meta<timestar::IndexConfig> {
    using T = timestar::IndexConfig;
    static constexpr auto value =
        object("bloom_filter_bits", &T::bloom_filter_bits, "block_size", &T::block_size, "write_buffer_size",
               &T::write_buffer_size, "max_open_files", &T::max_open_files, "max_file_size", &T::max_file_size,
               "series_cache_size", &T::series_cache_size, "metadata_cache_bytes", &T::metadata_cache_bytes,
               "discovery_cache_bytes", &T::discovery_cache_bytes, "block_cache_bytes", &T::block_cache_bytes,
               "compaction_rate_limit_mbps", &T::compaction_rate_limit_mbps);
};

template <>
struct glz::meta<timestar::IOPriorityConfig> {
    using T = timestar::IOPriorityConfig;
    static constexpr auto value = object("query_shares", &T::query_shares, "write_shares", &T::write_shares,
                                         "compaction_shares", &T::compaction_shares, "flush_shares", &T::flush_shares);
};

template <>
struct glz::meta<timestar::EngineConfig> {
    using T = timestar::EngineConfig;
    static constexpr auto value = object(
        "metadata_retry_interval_seconds", &T::metadata_retry_interval_seconds, "max_metadata_retry_ops",
        &T::max_metadata_retry_ops, "retention_sweep_interval_minutes", &T::retention_sweep_interval_minutes,
        "tombstone_dead_fraction_threshold", &T::tombstone_dead_fraction_threshold, "max_tombstone_rewrites_per_sweep",
        &T::max_tombstone_rewrites_per_sweep, "io_priority", &T::io_priority);
};

template <>
struct glz::meta<timestar::StreamingConfig> {
    using T = timestar::StreamingConfig;
    static constexpr auto value =
        object("max_subscriptions_per_shard", &T::max_subscriptions_per_shard, "output_queue_size",
               &T::output_queue_size, "heartbeat_interval_seconds", &T::heartbeat_interval_seconds);
};

template <>
struct glz::meta<timestar::TimestarConfigParseable> {
    using T = timestar::TimestarConfigParseable;
    static constexpr auto value = object("server", &T::server, "storage", &T::storage, "http", &T::http, "index",
                                         &T::index, "engine", &T::engine, "streaming", &T::streaming);
};

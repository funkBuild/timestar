#pragma once

#include <glaze/glaze.hpp>

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
};

struct CompactionConfig {
    uint32_t max_concurrent = 2;
    uint64_t max_memory = 256 * 1024 * 1024;
    uint32_t batch_size = 10000;
    uint32_t tier0_min_files = 4;
    uint32_t tier1_min_files = 4;
    uint32_t tier2_min_files = 4;
};

struct StorageConfig {
    uint64_t wal_size_threshold = 16 * 1024 * 1024;
    uint32_t max_points_per_block = 3000;
    double tsm_bloom_fpr = 0.001;
    uint32_t tsm_cache_entries = 4096;
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
    uint32_t compaction_rate_limit_mbps = 0;             // Max compaction write MB/s (0 = unlimited)
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
    float query_shares = 100.0f;       // TSM/index reads during queries
    float write_shares = 50.0f;        // WAL writes, memtable flushes
    float compaction_shares = 10.0f;   // Background TSM/SSTable compaction
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

}  // namespace timestar

// Glaze metadata for TOML serialization

template <>
struct glz::meta<timestar::ServerConfig> {
    using T = timestar::ServerConfig;
    static constexpr auto value = object("port", &T::port, "log_level", &T::log_level, "data_dir", &T::data_dir,
                                         "shutdown_timeout_seconds", &T::shutdown_timeout_seconds);
};

template <>
struct glz::meta<timestar::CompactionConfig> {
    using T = timestar::CompactionConfig;
    static constexpr auto value =
        object("max_concurrent", &T::max_concurrent, "max_memory", &T::max_memory, "batch_size", &T::batch_size,
               "tier0_min_files", &T::tier0_min_files, "tier1_min_files", &T::tier1_min_files, "tier2_min_files",
               &T::tier2_min_files);
};

template <>
struct glz::meta<timestar::StorageConfig> {
    using T = timestar::StorageConfig;
    static constexpr auto value = object("wal_size_threshold", &T::wal_size_threshold, "max_points_per_block",
                                         &T::max_points_per_block, "tsm_bloom_fpr", &T::tsm_bloom_fpr,
                                         "tsm_cache_entries", &T::tsm_cache_entries, "compaction", &T::compaction);
};

template <>
struct glz::meta<timestar::HttpConfig> {
    using T = timestar::HttpConfig;
    static constexpr auto value =
        object("max_write_body_size", &T::max_write_body_size, "max_query_body_size", &T::max_query_body_size,
               "max_series_count", &T::max_series_count, "max_total_points", &T::max_total_points,
               "query_timeout_seconds", &T::query_timeout_seconds,
               "slow_query_threshold_ms", &T::slow_query_threshold_ms);
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
    static constexpr auto value =
        object("query_shares", &T::query_shares, "write_shares", &T::write_shares,
               "compaction_shares", &T::compaction_shares);
};

template <>
struct glz::meta<timestar::EngineConfig> {
    using T = timestar::EngineConfig;
    static constexpr auto value =
        object("metadata_retry_interval_seconds", &T::metadata_retry_interval_seconds, "max_metadata_retry_ops",
               &T::max_metadata_retry_ops, "retention_sweep_interval_minutes", &T::retention_sweep_interval_minutes,
               "tombstone_dead_fraction_threshold", &T::tombstone_dead_fraction_threshold,
               "max_tombstone_rewrites_per_sweep", &T::max_tombstone_rewrites_per_sweep,
               "io_priority", &T::io_priority);
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

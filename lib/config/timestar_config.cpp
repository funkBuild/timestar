#include "timestar_config.hpp"

#include <glaze/toml.hpp>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace timestar {

// Global config pointer — set once in main() before app.run(), read from any shard.
static const TimestarConfig* g_config = nullptr;
static TimestarConfig g_defaultConfig;

void setGlobalConfig(const TimestarConfig& cfg) {
    static TimestarConfig stored;
    stored = cfg;
    g_config = &stored;
}

const TimestarConfig& config() {
    return g_config ? *g_config : g_defaultConfig;
}

std::vector<std::string> TimestarConfig::validate() const {
    std::vector<std::string> errors;

    if (server.port == 0) {
        errors.emplace_back("server.port must be > 0");
    }

    if (storage.wal_size_threshold == 0) {
        errors.emplace_back("storage.wal_size_threshold must be > 0");
    }
    if (storage.max_points_per_block == 0) {
        errors.emplace_back("storage.max_points_per_block must be > 0");
    }
    if (std::isnan(storage.tsm_bloom_fpr) || std::isinf(storage.tsm_bloom_fpr) ||
        storage.tsm_bloom_fpr <= 0.0 || storage.tsm_bloom_fpr >= 1.0) {
        errors.emplace_back("storage.tsm_bloom_fpr must be in (0, 1)");
    }

    if (storage.compaction.max_concurrent == 0) {
        errors.emplace_back("storage.compaction.max_concurrent must be > 0");
    }
    if (storage.compaction.batch_size == 0) {
        errors.emplace_back("storage.compaction.batch_size must be > 0");
    }

    if (http.max_write_body_size == 0) {
        errors.emplace_back("http.max_write_body_size must be > 0");
    }
    if (http.max_query_body_size == 0) {
        errors.emplace_back("http.max_query_body_size must be > 0");
    }
    if (http.query_timeout_seconds == 0) {
        errors.emplace_back("http.query_timeout_seconds must be > 0");
    }

    if (index.bloom_filter_bits == 0) {
        errors.emplace_back("index.bloom_filter_bits must be > 0");
    }
    if (index.block_size == 0) {
        errors.emplace_back("index.block_size must be > 0");
    }
    if (index.write_buffer_size == 0) {
        errors.emplace_back("index.write_buffer_size must be > 0");
    }

    if (std::isnan(engine.tombstone_dead_fraction_threshold) || std::isinf(engine.tombstone_dead_fraction_threshold) ||
        engine.tombstone_dead_fraction_threshold <= 0.0 || engine.tombstone_dead_fraction_threshold >= 1.0) {
        errors.emplace_back("engine.tombstone_dead_fraction_threshold must be in (0, 1)");
    }
    if (engine.retention_sweep_interval_minutes == 0) {
        errors.emplace_back("engine.retention_sweep_interval_minutes must be > 0");
    }
    if (streaming.output_queue_size == 0) {
        errors.emplace_back("streaming.output_queue_size must be > 0");
    }
    if (streaming.heartbeat_interval_seconds == 0) {
        errors.emplace_back("streaming.heartbeat_interval_seconds must be > 0");
    }

    // Validate Seastar-specific settings
    if (seastar.has("task_quota_ms")) {
        const std::string& raw = seastar.get("task_quota_ms");
        bool parseOk = false;
        double val = 0.0;
        try {
            std::size_t pos = 0;
            val = std::stod(raw, &pos);
            // Reject trailing garbage (e.g. "1.0abc")
            if (pos != raw.size()) {
                errors.emplace_back("seastar.task_quota_ms must be a number, got: \"" + raw + "\"");
            } else if (val <= 0.0) {
                errors.emplace_back("seastar.task_quota_ms must be > 0");
            } else {
                parseOk = true;
            }
        } catch (const std::exception&) {
            errors.emplace_back("seastar.task_quota_ms must be a number, got: \"" + raw + "\"");
        }
        (void)parseOk;
    }
    if (seastar.has("reactor_backend")) {
        const auto& rb = seastar.get("reactor_backend");
        if (rb != "linux-aio" && rb != "epoll" && rb != "io_uring") {
            errors.emplace_back("seastar.reactor_backend must be one of: linux-aio, epoll, io_uring");
        }
    }

    return errors;
}

// Parse the [seastar] section from raw TOML text.
// Glaze TOML doesn't support std::optional, so we parse this section manually.
static SeastarConfig parseSeastarSection(const std::string& tomlContent) {
    SeastarConfig cfg;

    // Find [seastar] section
    auto pos = tomlContent.find("[seastar]");
    if (pos == std::string::npos) {
        return cfg;
    }

    // Parse key = value lines until next section or EOF
    std::istringstream stream(tomlContent.substr(pos + 9));  // skip "[seastar]"
    std::string line;
    while (std::getline(stream, line)) {
        // Trim leading whitespace
        auto start = line.find_first_not_of(" \t");
        if (start == std::string::npos)
            continue;
        line = line.substr(start);

        // Skip comments and empty lines
        if (line.empty() || line[0] == '#')
            continue;

        // Stop at next section header
        if (line[0] == '[')
            break;

        // Parse key = value
        auto eq = line.find('=');
        if (eq == std::string::npos)
            continue;

        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);

        // Trim whitespace from key and value
        auto trimWs = [](std::string& s) {
            auto b = s.find_first_not_of(" \t");
            auto e = s.find_last_not_of(" \t\r\n");
            if (b == std::string::npos) {
                s.clear();
                return;
            }
            s = s.substr(b, e - b + 1);
        };
        trimWs(key);
        trimWs(value);

        // Remove quotes from string values
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        // Strip inline comments (after the value)
        auto commentPos = value.find('#');
        if (commentPos != std::string::npos) {
            value = value.substr(0, commentPos);
            trimWs(value);
        }

        if (!key.empty() && !value.empty()) {
            cfg.settings[key] = value;
        }
    }

    return cfg;
}

TimestarConfig loadConfigFile(const std::string& path) {
    // Read the entire file
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + path);
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // Parse TimeStar sections via Glaze (skip unknown keys like [seastar])
    TimestarConfigParseable parsed{};
    static constexpr glz::toml::toml_opts tomlOpts{.error_on_unknown_keys = false};
    glz::context ctx{};
    auto ec = glz::read<tomlOpts>(parsed, content, ctx);
    if (ec) {
        throw std::runtime_error("Failed to parse config file '" + path + "': " + glz::format_error(ec, content));
    }

    // Build the full config
    TimestarConfig cfg;
    cfg.server = parsed.server;
    cfg.storage = parsed.storage;
    cfg.http = parsed.http;
    cfg.index = parsed.index;
    cfg.engine = parsed.engine;
    cfg.streaming = parsed.streaming;

    // Parse [seastar] section manually
    cfg.seastar = parseSeastarSection(content);

    auto errors = cfg.validate();
    if (!errors.empty()) {
        std::string msg = "Config validation errors in '" + path + "':";
        for (const auto& e : errors) {
            msg += "\n  - " + e;
        }
        throw std::runtime_error(msg);
    }

    return cfg;
}

std::string dumpDefaultConfig() {
    TimestarConfigParseable defaults{};
    auto result = glz::write_toml(defaults);
    std::string out;
    if (result.has_value()) {
        out = result.value();
    } else {
        out = "# Error generating default config\n";
    }

    // Append the [seastar] section as commented-out defaults
    out += "\n[seastar]\n";
    out += "# All optional — omit to use Seastar defaults\n";
    out += "# smp = 4\n";
    out += "# memory = \"8G\"\n";
    out += "# reserve_memory = \"512M\"\n";
    out += "# poll_mode = false\n";
    out += "# task_quota_ms = 0.5\n";
    out += "# overprovisioned = false\n";
    out += "# thread_affinity = true\n";
    out += "# reactor_backend = \"linux-aio\"\n";
    out += "# blocked_reactor_notify_ms = 25\n";
    out += "# max_networking_io_control_blocks = 10000\n";
    out += "# unsafe_bypass_fsync = false\n";
    out += "# kernel_page_cache = false\n";
    out += "# max_task_backlog = 1000\n";
    out += "# io_properties_file = \"\"\n";

    return out;
}

void applyEnvironmentOverrides(TimestarConfig& cfg) {
    // Helper lambdas for type-safe env var reading
    auto envStr = [](const char* name) -> const char* { return std::getenv(name); };

    auto envU16 = [&](const char* name, uint16_t& field) {
        if (auto v = envStr(name)) {
            try { field = static_cast<uint16_t>(std::stoul(v)); }
            catch (const std::exception&) {
                throw std::runtime_error(std::string("Invalid value for ") + name + ": \"" + v + "\"");
            }
        }
    };
    auto envU32 = [&](const char* name, uint32_t& field) {
        if (auto v = envStr(name)) {
            try { field = static_cast<uint32_t>(std::stoul(v)); }
            catch (const std::exception&) {
                throw std::runtime_error(std::string("Invalid value for ") + name + ": \"" + v + "\"");
            }
        }
    };
    auto envU64 = [&](const char* name, uint64_t& field) {
        if (auto v = envStr(name)) {
            try { field = std::stoull(v); }
            catch (const std::exception&) {
                throw std::runtime_error(std::string("Invalid value for ") + name + ": \"" + v + "\"");
            }
        }
    };
    auto envDbl = [&](const char* name, double& field) {
        if (auto v = envStr(name)) {
            try { field = std::stod(v); }
            catch (const std::exception&) {
                throw std::runtime_error(std::string("Invalid value for ") + name + ": \"" + v + "\"");
            }
        }
    };
    auto envString = [&](const char* name, std::string& field) {
        if (auto v = envStr(name))
            field = v;
    };

    // Server
    envU16("TIMESTAR_PORT", cfg.server.port);
    envString("TIMESTAR_LOG_LEVEL", cfg.server.log_level);
    envString("TIMESTAR_DATA_DIR", cfg.server.data_dir);
    envU32("TIMESTAR_SHUTDOWN_TIMEOUT_SECONDS", cfg.server.shutdown_timeout_seconds);

    // Storage
    envU64("TIMESTAR_WAL_SIZE_THRESHOLD", cfg.storage.wal_size_threshold);
    envU32("TIMESTAR_MAX_POINTS_PER_BLOCK", cfg.storage.max_points_per_block);
    envDbl("TIMESTAR_TSM_BLOOM_FPR", cfg.storage.tsm_bloom_fpr);
    envU32("TIMESTAR_TSM_CACHE_ENTRIES", cfg.storage.tsm_cache_entries);

    // Compaction
    envU32("TIMESTAR_COMPACTION_MAX_CONCURRENT", cfg.storage.compaction.max_concurrent);
    envU64("TIMESTAR_COMPACTION_MAX_MEMORY", cfg.storage.compaction.max_memory);
    envU32("TIMESTAR_COMPACTION_BATCH_SIZE", cfg.storage.compaction.batch_size);

    // HTTP
    envU64("TIMESTAR_HTTP_MAX_WRITE_BODY_SIZE", cfg.http.max_write_body_size);
    envU64("TIMESTAR_HTTP_MAX_QUERY_BODY_SIZE", cfg.http.max_query_body_size);
    envU32("TIMESTAR_HTTP_MAX_SERIES_COUNT", cfg.http.max_series_count);
    envU64("TIMESTAR_HTTP_MAX_TOTAL_POINTS", cfg.http.max_total_points);
    envU32("TIMESTAR_HTTP_QUERY_TIMEOUT_SECONDS", cfg.http.query_timeout_seconds);
    envU32("TIMESTAR_HTTP_SLOW_QUERY_THRESHOLD_MS", cfg.http.slow_query_threshold_ms);

    // Index
    envU32("TIMESTAR_INDEX_BLOOM_FILTER_BITS", cfg.index.bloom_filter_bits);
    envU32("TIMESTAR_INDEX_BLOCK_SIZE", cfg.index.block_size);
    envU64("TIMESTAR_INDEX_WRITE_BUFFER_SIZE", cfg.index.write_buffer_size);
    envU32("TIMESTAR_INDEX_MAX_OPEN_FILES", cfg.index.max_open_files);
    envU64("TIMESTAR_INDEX_MAX_FILE_SIZE", cfg.index.max_file_size);
    envU64("TIMESTAR_INDEX_SERIES_CACHE_SIZE", cfg.index.series_cache_size);

    // Engine
    envU32("TIMESTAR_RETENTION_SWEEP_INTERVAL_MINUTES", cfg.engine.retention_sweep_interval_minutes);
    envU32("TIMESTAR_MAX_METADATA_RETRY_OPS", cfg.engine.max_metadata_retry_ops);
    envDbl("TIMESTAR_TOMBSTONE_DEAD_FRACTION_THRESHOLD", cfg.engine.tombstone_dead_fraction_threshold);
    envU32("TIMESTAR_MAX_TOMBSTONE_REWRITES_PER_SWEEP", cfg.engine.max_tombstone_rewrites_per_sweep);

    // Streaming
    envU32("TIMESTAR_STREAMING_MAX_SUBSCRIPTIONS", cfg.streaming.max_subscriptions_per_shard);
    envU32("TIMESTAR_STREAMING_OUTPUT_QUEUE_SIZE", cfg.streaming.output_queue_size);
    envU32("TIMESTAR_STREAMING_HEARTBEAT_INTERVAL_SECONDS", cfg.streaming.heartbeat_interval_seconds);

    // Seastar — these go into the string map
    auto envSeastar = [&](const char* envName, const char* key) {
        if (auto v = envStr(envName))
            cfg.seastar.settings[key] = v;
    };
    envSeastar("TIMESTAR_SMP", "smp");
    envSeastar("TIMESTAR_MEMORY", "memory");
    envSeastar("TIMESTAR_RESERVE_MEMORY", "reserve_memory");
    envSeastar("TIMESTAR_OVERPROVISIONED", "overprovisioned");
    envSeastar("TIMESTAR_THREAD_AFFINITY", "thread_affinity");
    envSeastar("TIMESTAR_REACTOR_BACKEND", "reactor_backend");
    envSeastar("TIMESTAR_POLL_MODE", "poll_mode");
    envSeastar("TIMESTAR_TASK_QUOTA_MS", "task_quota_ms");
    envSeastar("TIMESTAR_IO_PROPERTIES_FILE", "io_properties_file");
}

}  // namespace timestar

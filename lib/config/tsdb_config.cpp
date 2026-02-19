#include "tsdb_config.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include <glaze/toml.hpp>

namespace tsdb {

// Global config pointer — set once in main() before app.run(), read from any shard.
static const TsdbConfig* g_config = nullptr;
static TsdbConfig g_defaultConfig;

void setGlobalConfig(const TsdbConfig& cfg) {
    static TsdbConfig stored = cfg;
    g_config = &stored;
}

const TsdbConfig& config() {
    return g_config ? *g_config : g_defaultConfig;
}

std::vector<std::string> TsdbConfig::validate() const {
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
    if (storage.tsm_bloom_fpr <= 0.0 || storage.tsm_bloom_fpr >= 1.0) {
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

    if (engine.tombstone_dead_fraction_threshold <= 0.0 || engine.tombstone_dead_fraction_threshold >= 1.0) {
        errors.emplace_back("engine.tombstone_dead_fraction_threshold must be in (0, 1)");
    }

    // Validate Seastar-specific settings
    if (seastar.has("task_quota_ms")) {
        double val = std::stod(seastar.get("task_quota_ms"));
        if (val <= 0.0) {
            errors.emplace_back("seastar.task_quota_ms must be > 0");
        }
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
        if (start == std::string::npos) continue;
        line = line.substr(start);

        // Skip comments and empty lines
        if (line.empty() || line[0] == '#') continue;

        // Stop at next section header
        if (line[0] == '[') break;

        // Parse key = value
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;

        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);

        // Trim whitespace from key and value
        auto trimWs = [](std::string& s) {
            auto b = s.find_first_not_of(" \t");
            auto e = s.find_last_not_of(" \t\r\n");
            if (b == std::string::npos) { s.clear(); return; }
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

TsdbConfig loadConfigFile(const std::string& path) {
    // Read the entire file
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + path);
    }
    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

    // Parse TSDB sections via Glaze (skip unknown keys like [seastar])
    TsdbConfigParseable parsed{};
    static constexpr glz::toml::toml_opts tomlOpts{.error_on_unknown_keys = false};
    glz::context ctx{};
    auto ec = glz::read<tomlOpts>(parsed, content, ctx);
    if (ec) {
        throw std::runtime_error("Failed to parse config file '" + path + "': " +
                                 glz::format_error(ec, content));
    }

    // Build the full config
    TsdbConfig cfg;
    cfg.server = parsed.server;
    cfg.storage = parsed.storage;
    cfg.http = parsed.http;
    cfg.index = parsed.index;
    cfg.engine = parsed.engine;

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
    TsdbConfigParseable defaults{};
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

} // namespace tsdb

#include "config/timestar_config.hpp"
#include "core/engine.hpp"
#include "core/placement_table.hpp"
#include "http/http_delete_handler.hpp"
#include "http/http_derived_query_handler.hpp"
#include "http/http_metadata_handler.hpp"
#include "http/http_query_handler.hpp"
#include "http/http_retention_handler.hpp"
#include "http/http_stream_handler.hpp"
#include "http/http_write_handler.hpp"
#include "storage/shard_rebalancer.hpp"
#include "timestar/version.hpp"
#include "utils/logger.hpp"
#include "utils/stop_signal.hpp"

#include <atomic>
#include <cstring>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/util/backtrace.hh>
#include <sstream>
#include <vector>

using namespace seastar;
using namespace httpd;

// HttpServer class removed - using direct implementation in main()

// Global sharded engine - declared here so set_routes() can reference it.
// (Defined before set_routes, initialized in main.)
seastar::sharded<Engine> g_engine;

// Per-shard stream handler pointer, used to call stop() during shutdown.
static thread_local timestar::HttpStreamHandler* g_streamHandler = nullptr;

// Readiness flag — set true after all engines are initialized.
// Used by /health for Kubernetes readiness probes.
static std::atomic<bool> g_ready{false};

// TODO(H-H1): Add authentication/authorization middleware.  Currently all
// endpoints are unauthenticated.  When deploying to a network accessible to
// untrusted clients, place a reverse proxy (nginx, envoy) in front or
// implement bearer-token / mTLS auth here.

void set_routes(routes& r) {
    // Per-shard handler instances. Each handler's registerRoutes() captures
    // `this` in route lambdas, so the handler must outlive set_routes().
    // We store them in a thread_local vector so they are freed on process exit
    // (avoids ASAN leak reports while keeping shard-local memory ownership).
    static thread_local std::vector<std::unique_ptr<void, void (*)(void*)>> handlers;

    auto emplaceHandler = [&]<typename T>(T* ptr) {
        handlers.emplace_back(ptr, [](void* p) { delete static_cast<T*>(p); });
        return ptr;
    };

    // Simple test endpoints
    auto* test = emplaceHandler(new function_handler([](const_req req) { return "Hello from TimeStar HTTP Server!"; }));
    r.add(operation_type::GET, url("/test"), test);

    auto* health = emplaceHandler(new function_handler([](const_req req) {
        if (g_ready.load(std::memory_order_relaxed)) {
            return sstring("{\"status\":\"healthy\"}");
        }
        return sstring("{\"status\":\"starting\"}");
    }));
    r.add(operation_type::GET, url("/health"), health);

    auto* version = emplaceHandler(new function_handler([](const_req req) {
        return sstring(fmt::format(
            R"({{"version":"{}","git_commit":"{}","build_time":"{}","compiler":"{}"}})",
            timestar::VERSION, timestar::GIT_COMMIT, timestar::BUILD_TIME, timestar::COMPILER));
    }));
    r.add(operation_type::GET, url("/version"), version);

    auto* writeHandler = emplaceHandler(new HttpWriteHandler(&g_engine));
    writeHandler->registerRoutes(r);

    auto* queryHandler = emplaceHandler(new timestar::HttpQueryHandler(&g_engine, nullptr));
    queryHandler->registerRoutes(r);

    auto* deleteHandler = emplaceHandler(new HttpDeleteHandler(&g_engine));
    deleteHandler->registerRoutes(r);

    auto* metadataHandler = emplaceHandler(new HttpMetadataHandler(&g_engine));
    metadataHandler->registerRoutes(r);

    // HttpRetentionHandler uses enable_shared_from_this, so it must be
    // constructed as a shared_ptr. Store the shared_ptr in the handlers
    // vector to ensure it outlives the route lambdas.
    auto retentionHandlerPtr = std::make_shared<HttpRetentionHandler>(&g_engine);
    retentionHandlerPtr->registerRoutes(r);
    handlers.emplace_back(new std::shared_ptr<HttpRetentionHandler>(retentionHandlerPtr),
                          [](void* p) { delete static_cast<std::shared_ptr<HttpRetentionHandler>*>(p); });

    auto* streamHandler = emplaceHandler(new timestar::HttpStreamHandler(&g_engine));
    streamHandler->registerRoutes(r);
    g_streamHandler = streamHandler;  // Save for shutdown

    auto* derivedQueryHandler = emplaceHandler(new timestar::HttpDerivedQueryHandler(&g_engine));
    derivedQueryHandler->registerRoutes(r);

    auto* root = emplaceHandler(new function_handler([](const_req req) {
        return "{\"message\":\"TimeStar HTTP "
               "Server\",\"endpoints\":[\"/test\",\"/health\",\"/write\",\"/query\",\"/delete\",\"/measurements\",\"/"
               "tags\",\"/fields\",\"/retention\",\"/subscribe\",\"/subscriptions\"]}";
    }));
    r.add(operation_type::GET, url("/"), root);
}

int main(int argc, char** argv) {
    // Pre-scan argv for --version, --dump-config, --config before Seastar touches args.
    // This avoids Seastar complaining about unknown options.
    std::string configPath;
    bool dumpConfig = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--version") == 0) {
            std::cout << "TimeStar " << timestar::VERSION
                      << " (" << timestar::GIT_COMMIT << ")"
                      << "\nBuilt: " << timestar::BUILD_TIME
                      << "\nCompiler: " << timestar::COMPILER << std::endl;
            return 0;
        } else if (std::strcmp(argv[i], "--dump-config") == 0) {
            dumpConfig = true;
        } else if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            configPath = argv[i + 1];
        }
    }

    if (dumpConfig) {
        std::cout << timestar::dumpDefaultConfig();
        return 0;
    }

    // Support TIMESTAR_CONFIG_FILE env var as alternative to --config
    if (configPath.empty()) {
        if (auto envCfg = std::getenv("TIMESTAR_CONFIG_FILE")) {
            configPath = envCfg;
        }
    }

    // Load config file if specified, otherwise use defaults.
    timestar::TimestarConfig timestarConfig{};
    if (!configPath.empty()) {
        try {
            timestarConfig = timestar::loadConfigFile(configPath);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
    }

    // Apply TIMESTAR_* environment variable overrides (env vars > config file > defaults)
    timestar::applyEnvironmentOverrides(timestarConfig);

    // Re-validate after env overrides (env vars may set invalid values)
    auto envErrors = timestarConfig.validate();
    if (!envErrors.empty()) {
        std::cerr << "Config validation errors after environment overrides:" << std::endl;
        for (const auto& e : envErrors) {
            std::cerr << "  - " << e << std::endl;
        }
        return 1;
    }

    timestar::setGlobalConfig(timestarConfig);

    seastar::app_template app;

    namespace bpo = boost::program_options;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(timestarConfig.server.port), "HTTP server port")(
        "log-level", bpo::value<seastar::log_level>()->default_value(seastar::log_level::info),
        "Log level (error, warn, info, debug, trace)")("config", bpo::value<std::string>(), "Path to TOML config file")(
        "dump-config", "Print default config to stdout and exit");

    // Inject Seastar settings from TOML [seastar] section.
    // CLI args are already stored first, so bpo::store won't overwrite them.
    app.set_configuration_reader([&timestarConfig, &app](bpo::variables_map& vm) {
        const auto& ss = timestarConfig.seastar;
        if (ss.settings.empty())
            return;

        // Map TOML underscore keys to Seastar's hyphenated CLI option names.
        static const std::map<std::string, std::string> keyMap = {
            {"smp", "smp"},
            {"memory", "memory"},
            {"reserve_memory", "reserve-memory"},
            {"poll_mode", "poll-mode"},
            {"task_quota_ms", "task-quota-ms"},
            {"overprovisioned", "overprovisioned"},
            {"thread_affinity", "thread-affinity"},
            {"reactor_backend", "reactor-backend"},
            {"blocked_reactor_notify_ms", "blocked-reactor-notify-ms"},
            {"max_networking_io_control_blocks", "max-networking-io-control-blocks"},
            {"unsafe_bypass_fsync", "unsafe-bypass-fsync"},
            {"kernel_page_cache", "kernel-page-cache"},
            {"max_task_backlog", "max-task-backlog"},
            {"io_properties_file", "io-properties-file"},
        };

        std::ostringstream ini;
        for (const auto& [tomlKey, value] : ss.settings) {
            auto it = keyMap.find(tomlKey);
            if (it != keyMap.end()) {
                ini << it->second << "=" << value << "\n";
            }
        }

        std::string iniStr = ini.str();
        if (!iniStr.empty()) {
            std::istringstream iss(iniStr);
            bpo::store(bpo::parse_config_file(iss, app.get_conf_file_options_description()), vm);
        }
    });

    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            auto& config = app.configuration();
            uint16_t port = config["port"].as<uint16_t>();
            auto log_level = config["log-level"].as<seastar::log_level>();

            // Initialize logging
            timestar::init_logging(log_level);
            timestar::http_log.info("Starting TimeStar {} ({}) built {} with {}",
                                   timestar::VERSION, timestar::GIT_COMMIT,
                                   timestar::BUILD_TIME, timestar::COMPILER);

            // STEP 0: Check for shard rebalancing (CPU count change)
            {
                timestar::ShardRebalancer rebalancer(".");
                // Recover from any previously interrupted rebalance first
                rebalancer.recoverIfNeeded(seastar::smp::count).get();

                if (rebalancer.isRebalanceNeeded(seastar::smp::count)) {
                    timestar::http_log.info("Shard count changed from {} to {}, starting rebalance...",
                                            rebalancer.previousShardCount(), seastar::smp::count);
                    rebalancer.execute(seastar::smp::count).get();
                    timestar::http_log.info("Shard rebalance complete");
                } else {
                    // Persist current shard count for next startup
                    timestar::ShardRebalancer::writeShardCountMeta(".", seastar::smp::count);
                }
            }

            // Initialize virtual shard placement table (Phase 5)
            auto pt = timestar::PlacementTable::buildLocal(seastar::smp::count);
            timestar::setGlobalPlacement(std::move(pt));
            timestar::savePlacement("placement.json");

            // STEP 1: Initialize the Engine on all shards
            timestar::http_log.info("Initializing Engine on all shards...");
            g_engine.start().get();

            try {
                g_engine.invoke_on_all([](Engine& engine) { return engine.init(); }).get();

                // Create I/O scheduling groups (global operation, called once from shard 0).
                // Query I/O gets highest priority; compaction gets lowest to protect
                // query tail latency during compaction storms.
                const auto& ioCfg = timestar::config().engine.io_priority;
                auto queryGrp = seastar::create_scheduling_group("ts_query", ioCfg.query_shares).get();
                auto writeGrp = seastar::create_scheduling_group("ts_write", ioCfg.write_shares).get();
                auto compactGrp = seastar::create_scheduling_group("ts_compact", ioCfg.compaction_shares).get();

                g_engine
                    .invoke_on_all([queryGrp, writeGrp, compactGrp](Engine& engine) {
                        engine.setIOSchedulingGroups(queryGrp, writeGrp, compactGrp);
                        return seastar::make_ready_future<>();
                    })
                    .get();

                // Set back-reference so Engine can do cross-shard operations.
                g_engine
                    .invoke_on_all([](Engine& engine) {
                        engine.setShardedRef(&g_engine);
                        return seastar::make_ready_future<>();
                    })
                    .get();

                // Load retention policies from NativeIndex and broadcast to all shards
                g_engine.invoke_on(0, [](Engine& engine) { return engine.loadAndBroadcastRetentionPolicies(); }).get();

                // Start the retention sweep timer on shard 0 (15-minute interval)
                g_engine
                    .invoke_on(0,
                               [](Engine& engine) {
                                   engine.startRetentionSweepTimer();
                                   return seastar::make_ready_future<>();
                               })
                    .get();

                // Persist shard count after successful init
                timestar::ShardRebalancer::writeShardCountMeta(".", seastar::smp::count);
                timestar::http_log.info("Engine init completed on all shards");
            } catch (const std::bad_alloc& e) {
                timestar::http_log.error("bad_alloc during Engine init: {}", e.what());
                // Print backtrace for debugging according to Seastar docs
                timestar::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            } catch (const std::exception& e) {
                timestar::http_log.error("Exception during Engine init: {}", e.what());
                // Print backtrace for debugging
                timestar::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            }

            // Start background tasks on all shards for WAL->TSM conversion
            timestar::http_log.info("Starting background tasks on all shards...");
            g_engine.invoke_on_all([](Engine& engine) { return engine.startBackgroundTasks(); }).get();

            timestar::http_log.info("Engine initialized successfully with background tasks");
            g_ready.store(true, std::memory_order_relaxed);

            // STEP 2: Create stop signal handler
            // Note: HTTP handlers are created per-shard inside set_routes() to avoid
            // cross-shard memory access. Each shard gets its own handler instance
            // allocated on its own heap.
            seastar_apps_lib::stop_signal stop_signal;

            auto server = std::make_unique<http_server_control>();

            // Start the HTTP server
            try {
                server->start().get();

                // Limit request body size to 64MB to prevent memory exhaustion
                // from oversized payloads. Seastar enforces this at the connection
                // layer before buffering the full body.
                static constexpr size_t MAX_CONTENT_LENGTH = 64 * 1024 * 1024;
                server->server().invoke_on_all([](httpd::http_server& s) {
                    s.set_content_length_limit(MAX_CONTENT_LENGTH);
                }).get();

                server->set_routes(set_routes).get();

                // Register Prometheus metrics endpoint at /metrics.
                // Exposes all per-shard TimeStar counters/gauges plus Seastar
                // built-in metrics (reactor, I/O, scheduler, memory).
                seastar::prometheus::config promConfig;
                promConfig.metric_help = "TimeStar TSDB metrics";
                promConfig.prefix = "timestar";
                seastar::prometheus::add_prometheus_routes(server->server(), promConfig).get();

                server->listen(port).get();
            } catch (const std::exception& e) {
                timestar::http_log.error("Failed to start HTTP server: {}", e.what());
                // Clean up engine before exiting
                g_engine.invoke_on_all([](Engine& engine) { return engine.stop(); }).get();
                g_engine.stop().get();
                throw;
            }

            timestar::http_log.info("TimeStar HTTP Server listening on port {} ...", port);
            timestar::http_log.info("Available endpoints:");
            timestar::http_log.info("  GET  /         - Root endpoint");
            timestar::http_log.info("  GET  /test     - Test message");
            timestar::http_log.info("  GET  /health   - Health check");
            timestar::http_log.info("  POST /write    - Write time series data");
            timestar::http_log.info("  POST /query    - Query time series data");
            timestar::http_log.info("  POST /delete   - Delete time series data");
            timestar::http_log.info("  POST /subscribe - Subscribe to streaming data (SSE)");
            timestar::http_log.info("  GET  /metrics  - Prometheus metrics");
            timestar::http_log.info("  GET  /version  - Build version info");

            // Wait for stop signal
            stop_signal.wait().get();

            // Graceful shutdown with configurable timeout.
            // If any gate drain, WAL flush, or index close hangs beyond the
            // deadline, we log an error and exit — the OS reclaims resources
            // and WAL recovery handles unflushed data on restart.
            const auto timeoutSec = timestar::config().server.shutdown_timeout_seconds;
            timestar::http_log.info("Shutting down (timeout: {}s)...", timeoutSec);

            auto doShutdown = [&]() -> seastar::future<> {
                co_await server->stop();
                co_await seastar::smp::invoke_on_all([] {
                    if (g_streamHandler) return g_streamHandler->stop();
                    return seastar::make_ready_future<>();
                });
                co_await g_engine.invoke_on_all([](Engine& engine) { return engine.stop(); });
                co_await g_engine.stop();
            };

            try {
                if (timeoutSec > 0) {
                    auto deadline = seastar::lowres_clock::now() + std::chrono::seconds(timeoutSec);
                    seastar::with_timeout(deadline, doShutdown()).get();
                } else {
                    doShutdown().get();
                }
                timestar::http_log.info("Shutdown complete");
            } catch (const seastar::timed_out_error&) {
                timestar::http_log.error("Shutdown timed out after {}s — forcing exit", timeoutSec);
            }

            return 0;
        });
    });
}
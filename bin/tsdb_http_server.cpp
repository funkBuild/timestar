#include <vector>
#include <chrono>
#include <cstring>
#include <sstream>

#include "core/engine.hpp"
#include "http/http_write_handler.hpp"
#include "http/http_query_handler.hpp"
#include "http/http_delete_handler.hpp"
#include "http/http_metadata_handler.hpp"
#include "http/http_retention_handler.hpp"
#include "http/http_stream_handler.hpp"
#include "http/http_derived_query_handler.hpp"
#include "config/tsdb_config.hpp"
#include "utils/stop_signal.hpp"
#include "utils/logger.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/net/api.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/util/backtrace.hh>

using namespace seastar;
using namespace httpd;

// HttpServer class removed - using direct implementation in main()

// Global sharded engine - declared here so set_routes() can reference it.
// (Defined before set_routes, initialized in main.)
seastar::sharded<Engine> g_engine;

void set_routes(routes& r) {
    // Simple test endpoints
    auto* test = new function_handler([](const_req req) {
        return "Hello from TSDB HTTP Server!";
    });
    r.add(operation_type::GET, url("/test"), test);

    auto* health = new function_handler([](const_req req) {
        return "{\"status\":\"healthy\"}";
    });
    r.add(operation_type::GET, url("/health"), health);

    // Create per-shard handler instances on the calling shard's heap.
    // Each handler's registerRoutes() captures `this` in route lambdas,
    // so the handler must outlive set_routes(). Heap allocation ensures
    // each shard owns its handler in shard-local memory (no cross-shard access).
    // These are intentionally never freed -- they live for the process lifetime,
    // matching the pattern used by function_handler above.
    auto* writeHandler = new HttpWriteHandler(&g_engine);
    writeHandler->registerRoutes(r);

    auto* queryHandler = new tsdb::HttpQueryHandler(&g_engine, nullptr);
    queryHandler->registerRoutes(r);

    auto* deleteHandler = new HttpDeleteHandler(&g_engine);
    deleteHandler->registerRoutes(r);

    auto* metadataHandler = new HttpMetadataHandler(&g_engine);
    metadataHandler->registerRoutes(r);

    // HttpRetentionHandler uses enable_shared_from_this so that its
    // registerRoutes() can capture shared_from_this() in the route lambdas.
    // The shared_ptr is kept alive by the function_handler objects owned by
    // routes; when routes is destroyed the handler is freed cleanly.
    auto retentionHandler = std::make_shared<HttpRetentionHandler>(&g_engine);
    retentionHandler->registerRoutes(r);

    auto* streamHandler = new tsdb::HttpStreamHandler(&g_engine);
    streamHandler->registerRoutes(r);

    auto* derivedQueryHandler = new tsdb::HttpDerivedQueryHandler(&g_engine);
    derivedQueryHandler->registerRoutes(r);

    auto* root = new function_handler([](const_req req) {
        return "{\"message\":\"TSDB HTTP Server\",\"endpoints\":[\"/test\",\"/health\",\"/write\",\"/query\",\"/delete\",\"/measurements\",\"/tags\",\"/fields\",\"/retention\",\"/subscribe\",\"/subscriptions\"]}";
    });
    r.add(operation_type::GET, url("/"), root);
}

int main(int argc, char** argv) {
    // Pre-scan argv for --dump-config and --config before Seastar touches args.
    // This avoids Seastar complaining about unknown options.
    std::string configPath;
    bool dumpConfig = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--dump-config") == 0) {
            dumpConfig = true;
        } else if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            configPath = argv[i + 1];
        }
    }

    if (dumpConfig) {
        std::cout << tsdb::dumpDefaultConfig();
        return 0;
    }

    // Load config file if specified, otherwise use defaults.
    tsdb::TsdbConfig tsdbConfig{};
    if (!configPath.empty()) {
        try {
            tsdbConfig = tsdb::loadConfigFile(configPath);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
    }
    tsdb::setGlobalConfig(tsdbConfig);

    seastar::app_template app;

    namespace bpo = boost::program_options;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(tsdbConfig.server.port),
         "HTTP server port")
        ("log-level", bpo::value<seastar::log_level>()->default_value(seastar::log_level::info),
         "Log level (error, warn, info, debug, trace)")
        ("config", bpo::value<std::string>(),
         "Path to TOML config file")
        ("dump-config", "Print default config to stdout and exit");

    // Inject Seastar settings from TOML [seastar] section.
    // CLI args are already stored first, so bpo::store won't overwrite them.
    app.set_configuration_reader([&tsdbConfig, &app](bpo::variables_map& vm) {
        const auto& ss = tsdbConfig.seastar;
        if (ss.settings.empty()) return;

        // Map TOML underscore keys to Seastar's hyphenated CLI option names.
        static const std::map<std::string, std::string> keyMap = {
            {"smp", "smp"}, {"memory", "memory"},
            {"reserve_memory", "reserve-memory"}, {"poll_mode", "poll-mode"},
            {"task_quota_ms", "task-quota-ms"}, {"overprovisioned", "overprovisioned"},
            {"thread_affinity", "thread-affinity"}, {"reactor_backend", "reactor-backend"},
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
            tsdb::init_logging(log_level);
            tsdb::http_log.info("Starting TSDB HTTP Server with log level: {}", log_level);
            
            // STEP 1: Initialize the Engine on all shards
            tsdb::http_log.info("Initializing Engine on all shards...");
            g_engine.start().get();
            
            try {
                g_engine.invoke_on_all([](Engine& engine) {
                    return engine.init();
                }).get();

                // Set back-reference so Engine::insert() on non-zero shards can
                // forward metadata indexing to shard 0.
                g_engine.invoke_on_all([](Engine& engine) {
                    engine.setShardedRef(&g_engine);
                    return seastar::make_ready_future<>();
                }).get();

                // Load retention policies from LevelDB and broadcast to all shards
                g_engine.invoke_on(0, [](Engine& engine) {
                    return engine.loadAndBroadcastRetentionPolicies();
                }).get();

                // Start the retention sweep timer on shard 0 (15-minute interval)
                g_engine.invoke_on(0, [](Engine& engine) {
                    engine.startRetentionSweepTimer();
                    return seastar::make_ready_future<>();
                }).get();

                tsdb::http_log.info("Engine init completed on all shards");
            } catch (const std::bad_alloc& e) {
                tsdb::http_log.error("bad_alloc during Engine init: {}", e.what());
                // Print backtrace for debugging according to Seastar docs
                tsdb::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            } catch (const std::exception& e) {
                tsdb::http_log.error("Exception during Engine init: {}", e.what());
                // Print backtrace for debugging
                tsdb::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            }
            
            // Start background tasks on all shards for WAL->TSM conversion
            tsdb::http_log.info("Starting background tasks on all shards...");
            g_engine.invoke_on_all([](Engine& engine) {
                return engine.startBackgroundTasks();
            }).get();
            
            tsdb::http_log.info("Engine initialized successfully with background tasks");

            // STEP 2: Create stop signal handler
            // Note: HTTP handlers are created per-shard inside set_routes() to avoid
            // cross-shard memory access. Each shard gets its own handler instance
            // allocated on its own heap.
            seastar_apps_lib::stop_signal stop_signal;
            
            auto server = std::make_unique<http_server_control>();
            
            // Start the HTTP server
            try {
                server->start().get();
                server->set_routes(set_routes).get();
                server->listen(port).get();
            } catch (const std::exception& e) {
                tsdb::http_log.error("Failed to start HTTP server: {}", e.what());
                // Clean up engine before exiting
                g_engine.invoke_on_all([](Engine& engine) {
                    return engine.stop();
                }).get();
                g_engine.stop().get();
                throw;
            }
            
            tsdb::http_log.info("TSDB HTTP Server listening on port {} ...", port);
            tsdb::http_log.info("Available endpoints:");
            tsdb::http_log.info("  GET  /         - Root endpoint");
            tsdb::http_log.info("  GET  /test     - Test message");
            tsdb::http_log.info("  GET  /health   - Health check");
            tsdb::http_log.info("  POST /write    - Write time series data");
            tsdb::http_log.info("  POST /query    - Query time series data");
            tsdb::http_log.info("  POST /delete   - Delete time series data");
            tsdb::http_log.info("  POST /subscribe - Subscribe to streaming data (SSE)");
            
            // Wait for stop signal
            stop_signal.wait().get();
            
            // Clean shutdown
            tsdb::http_log.info("Shutting down HTTP server...");
            server->stop().get();

            // Flush in-memory data to TSM and stop engine
            tsdb::http_log.info("Flushing in-memory data to TSM files...");
            g_engine.invoke_on_all([](Engine& engine) {
                return engine.stop();
            }).get();

            // Then stop the engine instances
            tsdb::http_log.info("Stopping Engine instances...");
            g_engine.stop().get();

            tsdb::http_log.info("Shutdown complete");
            return 0;
        });
    });
}
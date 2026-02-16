#include <vector>
#include <chrono>

#include "core/engine.hpp"
#include "http/http_write_handler.hpp"
#include "http/http_query_handler.hpp"
#include "http/http_delete_handler.hpp"
#include "http/http_metadata_handler.hpp"
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

    auto* root = new function_handler([](const_req req) {
        return "{\"message\":\"TSDB HTTP Server\",\"endpoints\":[\"/test\",\"/health\",\"/write\",\"/query\",\"/delete\",\"/measurements\",\"/tags\",\"/fields\"]}";
    });
    r.add(operation_type::GET, url("/"), root);
}

int main(int argc, char** argv) {
    seastar::app_template app;
    
    namespace bpo = boost::program_options;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(8086), 
         "HTTP server port (default: 8086)")
        ("log-level", bpo::value<seastar::log_level>()->default_value(seastar::log_level::info),
         "Log level (error, warn, info, debug, trace)");
    
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
            
            // Wait for stop signal
            stop_signal.wait().get();
            
            // Clean shutdown
            tsdb::http_log.info("Shutting down HTTP server...");
            server->stop().get();
            
            // Stop background tasks first
            tsdb::http_log.info("Stopping background tasks...");
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
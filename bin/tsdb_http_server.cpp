#include <vector>
#include <chrono>

#include "core/engine.hpp"
#include "http/http_write_handler.hpp"
#include "http/http_query_handler.hpp"
#include "http/http_delete_handler.hpp"
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

using namespace seastar;
using namespace httpd;

// HttpServer class removed - using direct implementation in main()

// Create handlers as globals so they can be used in set_routes
std::unique_ptr<HttpWriteHandler> g_writeHandler;
std::unique_ptr<tsdb::HttpQueryHandler> g_queryHandler;
std::unique_ptr<HttpDeleteHandler> g_deleteHandler;

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
    
    // Register write endpoint
    if (g_writeHandler) {
        g_writeHandler->registerRoutes(r);
    }
    
    // Register query endpoint
    if (g_queryHandler) {
        g_queryHandler->registerRoutes(r);
    }
    
    // Register delete endpoint
    if (g_deleteHandler) {
        g_deleteHandler->registerRoutes(r);
    }
    
    auto* root = new function_handler([](const_req req) {
        return "{\"message\":\"TSDB HTTP Server\",\"endpoints\":[\"/test\",\"/health\",\"/write\",\"/query\",\"/delete\"]}";
    });
    r.add(operation_type::GET, url("/"), root);
}

// Global sharded engine for use in handlers
seastar::sharded<Engine> g_engine;

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
                
                tsdb::http_log.info("Engine init completed on all shards");
            } catch (const std::bad_alloc& e) {
                tsdb::http_log.error("bad_alloc during Engine init: {}", e.what());
                throw;
            } catch (const std::exception& e) {
                tsdb::http_log.error("Exception during Engine init: {}", e.what());
                throw;
            }
            
            // Start background tasks on all shards for WAL->TSM conversion
            tsdb::http_log.info("Starting background tasks on all shards...");
            g_engine.invoke_on_all([](Engine& engine) {
                return engine.startBackgroundTasks();
            }).get();
            
            tsdb::http_log.info("Engine initialized successfully with background tasks");
            
            // STEP 2: Create handlers
            g_writeHandler = std::make_unique<HttpWriteHandler>(&g_engine);
            tsdb::http_log.info("Write handler created");
            
            // Note: Pass nullptr for index since each Engine already has its own index
            // The query handler will use the Engine's index through the Engine interface
            try {
                g_queryHandler = std::make_unique<tsdb::HttpQueryHandler>(&g_engine, nullptr);
                tsdb::http_log.info("Query handler created");
            } catch (const std::exception& e) {
                tsdb::http_log.error("Failed to create query handler: {}", e.what());
                // Continue without query handler for now
                g_queryHandler = nullptr;
            }
            
            // Create delete handler
            g_deleteHandler = std::make_unique<HttpDeleteHandler>(&g_engine);
            tsdb::http_log.info("Delete handler created");
            
            // Create stop signal handler
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
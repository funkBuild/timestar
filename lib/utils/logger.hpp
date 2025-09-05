#pragma once

#include <seastar/util/log.hh>

namespace tsdb {

// TSDB module loggers - each component gets its own logger for filtering
inline seastar::logger tsdb_log("tsdb");
inline seastar::logger engine_log("tsdb.engine");
inline seastar::logger tsm_log("tsdb.tsm");
inline seastar::logger wal_log("tsdb.wal");
inline seastar::logger memory_log("tsdb.memory");
inline seastar::logger index_log("tsdb.index");
inline seastar::logger metadata_log("tsdb.metadata");
inline seastar::logger compactor_log("tsdb.compactor");
inline seastar::logger http_log("tsdb.http");
inline seastar::logger query_log("tsdb.query");

// Convenience macros for logging
#define TSDB_LOG_ERROR(logger, ...) logger.error(__VA_ARGS__)
#define TSDB_LOG_WARN(logger, ...) logger.warn(__VA_ARGS__)
#define TSDB_LOG_INFO(logger, ...) logger.info(__VA_ARGS__)
#define TSDB_LOG_DEBUG(logger, ...) logger.debug(__VA_ARGS__)
#define TSDB_LOG_TRACE(logger, ...) logger.trace(__VA_ARGS__)

// Initialize logging with default or custom level
inline void init_logging(seastar::log_level level = seastar::log_level::info) {
    seastar::global_logger_registry().set_all_loggers_level(level);
}

// Set log level for specific logger
inline void set_log_level(seastar::logger& logger, seastar::log_level level) {
    logger.set_level(level);
}

} // namespace tsdb
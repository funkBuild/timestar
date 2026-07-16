#pragma once

#include <seastar/util/log.hh>

namespace timestar {

// TimeStar module loggers - each component gets its own logger for filtering
inline seastar::logger timestar_log("timestar");
inline seastar::logger engine_log("timestar.engine");
inline seastar::logger tsm_log("timestar.tsm");
inline seastar::logger wal_log("timestar.wal");
inline seastar::logger memory_log("timestar.memory");
inline seastar::logger index_log("timestar.index");
inline seastar::logger metadata_log("timestar.metadata");
inline seastar::logger compactor_log("timestar.compactor");
inline seastar::logger http_log("timestar.http");
inline seastar::logger query_log("timestar.query");

// Initialize logging with default or custom level
inline void init_logging(seastar::log_level level = seastar::log_level::info) {
    seastar::global_logger_registry().set_all_loggers_level(level);
}

}  // namespace timestar
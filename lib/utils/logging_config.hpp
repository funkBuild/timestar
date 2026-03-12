#ifndef LOGGING_CONFIG_HPP
#define LOGGING_CONFIG_HPP

// Compile-time logging configuration for TimeStar
// These defines control verbose logging in performance-critical paths
// Set to 1 to enable, 0 to disable

// Enable/disable logging in the insert (write) path
// This includes WAL writes, memory store inserts, and TSM writes
#ifndef TIMESTAR_LOG_INSERT_PATH
    #define TIMESTAR_LOG_INSERT_PATH 0
#endif

// Enable/disable logging in the query (read) path
// This includes query parsing, planning, execution, and result merging
#ifndef TIMESTAR_LOG_QUERY_PATH
    #define TIMESTAR_LOG_QUERY_PATH 0
#endif

// Convenience macros for conditional logging
#if TIMESTAR_LOG_INSERT_PATH
    #define LOG_INSERT_PATH(logger, level, ...) logger.level(__VA_ARGS__)
#else
    #define LOG_INSERT_PATH(logger, level, ...) ((void)0)
#endif

#if TIMESTAR_LOG_QUERY_PATH
    #define LOG_QUERY_PATH(logger, level, ...) logger.level(__VA_ARGS__)
#else
    #define LOG_QUERY_PATH(logger, level, ...) ((void)0)
#endif

#endif  // LOGGING_CONFIG_HPP
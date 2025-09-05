#ifndef __LOGGING_CONFIG_HPP__
#define __LOGGING_CONFIG_HPP__

// Compile-time logging configuration for TSDB
// These defines control verbose logging in performance-critical paths
// Set to 1 to enable, 0 to disable

// Enable/disable logging in the insert (write) path
// This includes WAL writes, memory store inserts, and TSM writes
#ifndef TSDB_LOG_INSERT_PATH
#define TSDB_LOG_INSERT_PATH 1
#endif

// Enable/disable logging in the query (read) path
// This includes query parsing, planning, execution, and result merging
#ifndef TSDB_LOG_QUERY_PATH
#define TSDB_LOG_QUERY_PATH 1
#endif

// Convenience macros for conditional logging
#if TSDB_LOG_INSERT_PATH
#define LOG_INSERT_PATH(logger, level, ...) logger.level(__VA_ARGS__)
#else
#define LOG_INSERT_PATH(logger, level, ...) ((void)0)
#endif

#if TSDB_LOG_QUERY_PATH
#define LOG_QUERY_PATH(logger, level, ...) logger.level(__VA_ARGS__)
#else
#define LOG_QUERY_PATH(logger, level, ...) ((void)0)
#endif

#endif // __LOGGING_CONFIG_HPP__
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../../lib/config/timestar_config.hpp"

// ---------------------------------------------------------------------------
// Helper: build a minimal SeastarConfig with one setting.
// ---------------------------------------------------------------------------
static timestar::SeastarConfig makeSeastarConfig(const std::string& key, const std::string& value) {
    timestar::SeastarConfig cfg;
    cfg.settings[key] = value;
    return cfg;
}

// ---------------------------------------------------------------------------
// Helper: build a TimestarConfig that is fully valid except for the provided
// SeastarConfig so that any error in the returned list comes specifically
// from the seastar section.
// ---------------------------------------------------------------------------
static timestar::TimestarConfig makeValidConfigWith(timestar::SeastarConfig seastar) {
    timestar::TimestarConfig cfg;
    cfg.seastar = std::move(seastar);
    return cfg;
}

// ===========================================================================
// Tests for task_quota_ms validation (bug: raw std::stod throw on non-numeric)
// ===========================================================================

TEST(TimestarConfigValidateTest, TaskQuotaMsValidNumericPasses) {
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "0.5"));
    auto errors = cfg.validate();
    for (const auto& e : errors) {
        EXPECT_EQ(e.find("task_quota_ms"), std::string::npos)
            << "Unexpected task_quota_ms error: " << e;
    }
}

TEST(TimestarConfigValidateTest, TaskQuotaMsInvalidStringReturnsError) {
    // Before the fix this propagated std::invalid_argument from std::stod,
    // crashing the server on startup.  After the fix it must return an error.
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "abc"));
    std::vector<std::string> errors;
    ASSERT_NO_THROW(errors = cfg.validate())
        << "validate() must not propagate std::invalid_argument from std::stod";
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found)
        << "Expected a validation error mentioning 'task_quota_ms' for value \"abc\"";
}

TEST(TimestarConfigValidateTest, TaskQuotaMsEmptyStringReturnsError) {
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", ""));
    std::vector<std::string> errors;
    ASSERT_NO_THROW(errors = cfg.validate());
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found)
        << "Expected a validation error mentioning 'task_quota_ms' for empty string";
}

TEST(TimestarConfigValidateTest, TaskQuotaMsNegativeValueReturnsError) {
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "-1.0"));
    auto errors = cfg.validate();
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "Expected a validation error for negative task_quota_ms";
}

TEST(TimestarConfigValidateTest, TaskQuotaMsZeroReturnsError) {
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "0"));
    auto errors = cfg.validate();
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "Expected a validation error for task_quota_ms = 0";
}

TEST(TimestarConfigValidateTest, TaskQuotaMsWhitespaceStringReturnsError) {
    // A string of only spaces is not a valid number.
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "   "));
    std::vector<std::string> errors;
    ASSERT_NO_THROW(errors = cfg.validate());
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found)
        << "Expected a validation error for whitespace task_quota_ms";
}

TEST(TimestarConfigValidateTest, TaskQuotaMsTrailingGarbageReturnsError) {
    // "1.0abc": std::stod parses the leading "1.0" but trailing garbage remains.
    // The fix must reject this as an invalid number value.
    auto cfg = makeValidConfigWith(makeSeastarConfig("task_quota_ms", "1.0abc"));
    std::vector<std::string> errors;
    ASSERT_NO_THROW(errors = cfg.validate());
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("task_quota_ms") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found)
        << "Expected a validation error for task_quota_ms = \"1.0abc\" (trailing garbage)";
}

// ===========================================================================
// Tests for reactor_backend validation (pre-existing behaviour, no bug)
// ===========================================================================

TEST(TimestarConfigValidateTest, ReactorBackendValidValuePasses) {
    for (const auto* val : {"linux-aio", "epoll", "io_uring"}) {
        auto cfg = makeValidConfigWith(makeSeastarConfig("reactor_backend", val));
        auto errors = cfg.validate();
        for (const auto& e : errors) {
            EXPECT_EQ(e.find("reactor_backend"), std::string::npos)
                << "Unexpected reactor_backend error for value '" << val << "': " << e;
        }
    }
}

TEST(TimestarConfigValidateTest, ReactorBackendInvalidValueReturnsError) {
    auto cfg = makeValidConfigWith(makeSeastarConfig("reactor_backend", "kqueue"));
    auto errors = cfg.validate();
    bool found = false;
    for (const auto& e : errors) {
        if (e.find("reactor_backend") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found)
        << "Expected a validation error for reactor_backend = \"kqueue\"";
}

// ===========================================================================
// Baseline: an empty seastar section produces no seastar-specific errors.
// ===========================================================================

TEST(TimestarConfigValidateTest, EmptySeastarSectionProducesNoSeastarErrors) {
    timestar::TimestarConfig cfg;  // seastar.settings is empty
    auto errors = cfg.validate();
    for (const auto& e : errors) {
        EXPECT_EQ(e.find("task_quota_ms"), std::string::npos);
        EXPECT_EQ(e.find("reactor_backend"), std::string::npos);
    }
}

// ===========================================================================
// Helper: return only the errors whose text contains the given substring.
// ===========================================================================
static std::vector<std::string> errorsMatching(const std::vector<std::string>& errors,
                                                const std::string& needle) {
    std::vector<std::string> out;
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) out.push_back(e);
    }
    return out;
}

// ===========================================================================
// server.port
// ===========================================================================

TEST(TimestarConfigValidateTest, ServerPortZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.server.port = 0;
    auto errs = errorsMatching(cfg.validate(), "server.port");
    EXPECT_FALSE(errs.empty()) << "Expected error for server.port = 0";
}

TEST(TimestarConfigValidateTest, ServerPortNonZeroPasses) {
    timestar::TimestarConfig cfg;
    cfg.server.port = 8086;
    auto errs = errorsMatching(cfg.validate(), "server.port");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for server.port = 8086";
}

TEST(TimestarConfigValidateTest, ServerPortMaxValuePasses) {
    timestar::TimestarConfig cfg;
    cfg.server.port = 65535;
    auto errs = errorsMatching(cfg.validate(), "server.port");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for server.port = 65535";
}

// ===========================================================================
// storage.wal_size_threshold
// ===========================================================================

TEST(TimestarConfigValidateTest, WalSizeThresholdZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.wal_size_threshold = 0;
    auto errs = errorsMatching(cfg.validate(), "storage.wal_size_threshold");
    EXPECT_FALSE(errs.empty()) << "Expected error for wal_size_threshold = 0";
}

TEST(TimestarConfigValidateTest, WalSizeThresholdOneBytePassess) {
    timestar::TimestarConfig cfg;
    cfg.storage.wal_size_threshold = 1;
    auto errs = errorsMatching(cfg.validate(), "storage.wal_size_threshold");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for wal_size_threshold = 1";
}

TEST(TimestarConfigValidateTest, WalSizeThresholdVeryLargePasses) {
    timestar::TimestarConfig cfg;
    cfg.storage.wal_size_threshold = UINT64_MAX;
    auto errs = errorsMatching(cfg.validate(), "storage.wal_size_threshold");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for wal_size_threshold = UINT64_MAX";
}

// ===========================================================================
// storage.max_points_per_block
// ===========================================================================

TEST(TimestarConfigValidateTest, MaxPointsPerBlockZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.max_points_per_block = 0;
    auto errs = errorsMatching(cfg.validate(), "storage.max_points_per_block");
    EXPECT_FALSE(errs.empty()) << "Expected error for max_points_per_block = 0";
}

TEST(TimestarConfigValidateTest, MaxPointsPerBlockOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.storage.max_points_per_block = 1;
    auto errs = errorsMatching(cfg.validate(), "storage.max_points_per_block");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for max_points_per_block = 1";
}

// ===========================================================================
// storage.tsm_bloom_fpr  — open interval (0, 1)
// ===========================================================================

TEST(TimestarConfigValidateTest, TsmBloomFprZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 0.0;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_FALSE(errs.empty()) << "Expected error for tsm_bloom_fpr = 0.0 (boundary)";
}

TEST(TimestarConfigValidateTest, TsmBloomFprOneReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 1.0;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_FALSE(errs.empty()) << "Expected error for tsm_bloom_fpr = 1.0 (boundary)";
}

TEST(TimestarConfigValidateTest, TsmBloomFprNegativeReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = -0.5;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_FALSE(errs.empty()) << "Expected error for tsm_bloom_fpr = -0.5";
}

TEST(TimestarConfigValidateTest, TsmBloomFprGreaterThanOneReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 1.5;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_FALSE(errs.empty()) << "Expected error for tsm_bloom_fpr = 1.5";
}

TEST(TimestarConfigValidateTest, TsmBloomFprValidSmallValuePasses) {
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 0.001;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tsm_bloom_fpr = 0.001 (default)";
}

TEST(TimestarConfigValidateTest, TsmBloomFprNearBoundaryLowPasses) {
    // Just above 0.0 — should pass the (0, 1) check.
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 1e-10;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tsm_bloom_fpr = 1e-10";
}

TEST(TimestarConfigValidateTest, TsmBloomFprNearBoundaryHighPasses) {
    // Just below 1.0 — should pass the (0, 1) check.
    timestar::TimestarConfig cfg;
    cfg.storage.tsm_bloom_fpr = 0.9999999;
    auto errs = errorsMatching(cfg.validate(), "storage.tsm_bloom_fpr");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tsm_bloom_fpr = 0.9999999";
}

// ===========================================================================
// storage.compaction.max_concurrent
// ===========================================================================

TEST(TimestarConfigValidateTest, CompactionMaxConcurrentZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.compaction.max_concurrent = 0;
    auto errs = errorsMatching(cfg.validate(), "storage.compaction.max_concurrent");
    EXPECT_FALSE(errs.empty()) << "Expected error for compaction.max_concurrent = 0";
}

TEST(TimestarConfigValidateTest, CompactionMaxConcurrentOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.storage.compaction.max_concurrent = 1;
    auto errs = errorsMatching(cfg.validate(), "storage.compaction.max_concurrent");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for compaction.max_concurrent = 1";
}

// ===========================================================================
// storage.compaction.batch_size
// ===========================================================================

TEST(TimestarConfigValidateTest, CompactionBatchSizeZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.storage.compaction.batch_size = 0;
    auto errs = errorsMatching(cfg.validate(), "storage.compaction.batch_size");
    EXPECT_FALSE(errs.empty()) << "Expected error for compaction.batch_size = 0";
}

TEST(TimestarConfigValidateTest, CompactionBatchSizeOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.storage.compaction.batch_size = 1;
    auto errs = errorsMatching(cfg.validate(), "storage.compaction.batch_size");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for compaction.batch_size = 1";
}

// ===========================================================================
// http.max_write_body_size
// ===========================================================================

TEST(TimestarConfigValidateTest, MaxWriteBodySizeZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.http.max_write_body_size = 0;
    auto errs = errorsMatching(cfg.validate(), "http.max_write_body_size");
    EXPECT_FALSE(errs.empty()) << "Expected error for max_write_body_size = 0";
}

TEST(TimestarConfigValidateTest, MaxWriteBodySizeOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.http.max_write_body_size = 1;
    auto errs = errorsMatching(cfg.validate(), "http.max_write_body_size");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for max_write_body_size = 1";
}

// ===========================================================================
// http.max_query_body_size
// ===========================================================================

TEST(TimestarConfigValidateTest, MaxQueryBodySizeZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.http.max_query_body_size = 0;
    auto errs = errorsMatching(cfg.validate(), "http.max_query_body_size");
    EXPECT_FALSE(errs.empty()) << "Expected error for max_query_body_size = 0";
}

TEST(TimestarConfigValidateTest, MaxQueryBodySizeOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.http.max_query_body_size = 1;
    auto errs = errorsMatching(cfg.validate(), "http.max_query_body_size");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for max_query_body_size = 1";
}

// ===========================================================================
// http.query_timeout_seconds
// ===========================================================================

TEST(TimestarConfigValidateTest, QueryTimeoutSecondsZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.http.query_timeout_seconds = 0;
    auto errs = errorsMatching(cfg.validate(), "http.query_timeout_seconds");
    EXPECT_FALSE(errs.empty()) << "Expected error for query_timeout_seconds = 0";
}

TEST(TimestarConfigValidateTest, QueryTimeoutSecondsOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.http.query_timeout_seconds = 1;
    auto errs = errorsMatching(cfg.validate(), "http.query_timeout_seconds");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for query_timeout_seconds = 1";
}

TEST(TimestarConfigValidateTest, QueryTimeoutSecondsVeryLargePasses) {
    timestar::TimestarConfig cfg;
    cfg.http.query_timeout_seconds = UINT32_MAX;
    auto errs = errorsMatching(cfg.validate(), "http.query_timeout_seconds");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for query_timeout_seconds = UINT32_MAX";
}

// ===========================================================================
// index.bloom_filter_bits
// ===========================================================================

TEST(TimestarConfigValidateTest, BloomFilterBitsZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.index.bloom_filter_bits = 0;
    auto errs = errorsMatching(cfg.validate(), "index.bloom_filter_bits");
    EXPECT_FALSE(errs.empty()) << "Expected error for bloom_filter_bits = 0";
}

TEST(TimestarConfigValidateTest, BloomFilterBitsOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.index.bloom_filter_bits = 1;
    auto errs = errorsMatching(cfg.validate(), "index.bloom_filter_bits");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for bloom_filter_bits = 1";
}

// ===========================================================================
// index.block_size
// ===========================================================================

TEST(TimestarConfigValidateTest, IndexBlockSizeZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.index.block_size = 0;
    auto errs = errorsMatching(cfg.validate(), "index.block_size");
    EXPECT_FALSE(errs.empty()) << "Expected error for index.block_size = 0";
}

TEST(TimestarConfigValidateTest, IndexBlockSizeOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.index.block_size = 1;
    auto errs = errorsMatching(cfg.validate(), "index.block_size");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for index.block_size = 1";
}

// ===========================================================================
// index.write_buffer_size
// ===========================================================================

TEST(TimestarConfigValidateTest, IndexWriteBufferSizeZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.index.write_buffer_size = 0;
    auto errs = errorsMatching(cfg.validate(), "index.write_buffer_size");
    EXPECT_FALSE(errs.empty()) << "Expected error for index.write_buffer_size = 0";
}

TEST(TimestarConfigValidateTest, IndexWriteBufferSizeOnePasses) {
    timestar::TimestarConfig cfg;
    cfg.index.write_buffer_size = 1;
    auto errs = errorsMatching(cfg.validate(), "index.write_buffer_size");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for index.write_buffer_size = 1";
}

// ===========================================================================
// engine.tombstone_dead_fraction_threshold  — open interval (0, 1)
// ===========================================================================

TEST(TimestarConfigValidateTest, TombstoneFractionZeroReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = 0.0;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_FALSE(errs.empty()) << "Expected error for tombstone_dead_fraction_threshold = 0.0 (boundary)";
}

TEST(TimestarConfigValidateTest, TombstoneFractionOneReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = 1.0;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_FALSE(errs.empty()) << "Expected error for tombstone_dead_fraction_threshold = 1.0 (boundary)";
}

TEST(TimestarConfigValidateTest, TombstoneFractionNegativeReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = -0.1;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_FALSE(errs.empty()) << "Expected error for tombstone_dead_fraction_threshold = -0.1";
}

TEST(TimestarConfigValidateTest, TombstoneFractionGreaterThanOneReturnsError) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = 1.1;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_FALSE(errs.empty()) << "Expected error for tombstone_dead_fraction_threshold = 1.1";
}

TEST(TimestarConfigValidateTest, TombstoneFractionDefaultValuePasses) {
    timestar::TimestarConfig cfg;
    // Default is 0.10, which is in (0, 1).
    cfg.engine.tombstone_dead_fraction_threshold = 0.10;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tombstone_dead_fraction_threshold = 0.10 (default)";
}

TEST(TimestarConfigValidateTest, TombstoneFractionNearBoundaryLowPasses) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = 1e-9;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tombstone_dead_fraction_threshold = 1e-9";
}

TEST(TimestarConfigValidateTest, TombstoneFractionNearBoundaryHighPasses) {
    timestar::TimestarConfig cfg;
    cfg.engine.tombstone_dead_fraction_threshold = 0.9999999;
    auto errs = errorsMatching(cfg.validate(), "engine.tombstone_dead_fraction_threshold");
    EXPECT_TRUE(errs.empty()) << "Unexpected error for tombstone_dead_fraction_threshold = 0.9999999";
}

// ===========================================================================
// Multiple simultaneous errors — validate() collects all of them.
// ===========================================================================

TEST(TimestarConfigValidateTest, MultipleInvalidFieldsReturnsAllErrors) {
    timestar::TimestarConfig cfg;
    cfg.server.port = 0;
    cfg.storage.wal_size_threshold = 0;
    cfg.storage.max_points_per_block = 0;
    cfg.storage.tsm_bloom_fpr = 0.0;
    cfg.storage.compaction.max_concurrent = 0;
    cfg.storage.compaction.batch_size = 0;
    cfg.http.max_write_body_size = 0;
    cfg.http.max_query_body_size = 0;
    cfg.http.query_timeout_seconds = 0;
    cfg.index.bloom_filter_bits = 0;
    cfg.index.block_size = 0;
    cfg.index.write_buffer_size = 0;
    cfg.engine.tombstone_dead_fraction_threshold = 0.0;

    auto errors = cfg.validate();
    EXPECT_GE(errors.size(), static_cast<std::size_t>(13))
        << "Expected at least 13 errors when all validated fields are invalid";
}

// ===========================================================================
// Default-constructed TimestarConfig must pass validation with no errors.
// ===========================================================================

TEST(TimestarConfigValidateTest, DefaultConfigIsValid) {
    timestar::TimestarConfig cfg;
    auto errors = cfg.validate();
    EXPECT_TRUE(errors.empty())
        << "Default-constructed TimestarConfig should be valid; got errors:";
    for (const auto& e : errors) {
        ADD_FAILURE() << "  " << e;
    }
}

// ===========================================================================
// applyEnvironmentOverrides() tests
// ===========================================================================

// RAII helper to set an env var for the duration of a scope and restore on exit.
class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        auto* prev = std::getenv(name);
        had_prev_ = (prev != nullptr);
        if (had_prev_) prev_value_ = prev;
        setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (had_prev_) {
            setenv(name_.c_str(), prev_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;
private:
    std::string name_;
    std::string prev_value_;
    bool had_prev_ = false;
};

TEST(TimestarConfigEnvTest, PortOverride) {
    ScopedEnv e("TIMESTAR_PORT", "9090");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.server.port, 9090);
}

TEST(TimestarConfigEnvTest, LogLevelOverride) {
    ScopedEnv e("TIMESTAR_LOG_LEVEL", "debug");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.server.log_level, "debug");
}

TEST(TimestarConfigEnvTest, DataDirOverride) {
    ScopedEnv e("TIMESTAR_DATA_DIR", "/var/lib/timestar");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.server.data_dir, "/var/lib/timestar");
}

TEST(TimestarConfigEnvTest, StorageOverrides) {
    ScopedEnv e1("TIMESTAR_WAL_SIZE_THRESHOLD", "33554432");
    ScopedEnv e2("TIMESTAR_MAX_POINTS_PER_BLOCK", "5000");
    ScopedEnv e3("TIMESTAR_TSM_BLOOM_FPR", "0.01");
    ScopedEnv e4("TIMESTAR_TSM_CACHE_ENTRIES", "8192");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.storage.wal_size_threshold, 33554432u);
    EXPECT_EQ(cfg.storage.max_points_per_block, 5000u);
    EXPECT_DOUBLE_EQ(cfg.storage.tsm_bloom_fpr, 0.01);
    EXPECT_EQ(cfg.storage.tsm_cache_entries, 8192u);
}

TEST(TimestarConfigEnvTest, CompactionOverrides) {
    ScopedEnv e1("TIMESTAR_COMPACTION_MAX_CONCURRENT", "4");
    ScopedEnv e2("TIMESTAR_COMPACTION_MAX_MEMORY", "536870912");
    ScopedEnv e3("TIMESTAR_COMPACTION_BATCH_SIZE", "20000");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.storage.compaction.max_concurrent, 4u);
    EXPECT_EQ(cfg.storage.compaction.max_memory, 536870912u);
    EXPECT_EQ(cfg.storage.compaction.batch_size, 20000u);
}

TEST(TimestarConfigEnvTest, HttpOverrides) {
    ScopedEnv e1("TIMESTAR_HTTP_MAX_WRITE_BODY_SIZE", "134217728");
    ScopedEnv e2("TIMESTAR_HTTP_QUERY_TIMEOUT_SECONDS", "60");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.http.max_write_body_size, 134217728u);
    EXPECT_EQ(cfg.http.query_timeout_seconds, 60u);
}

TEST(TimestarConfigEnvTest, IndexOverrides) {
    ScopedEnv e1("TIMESTAR_INDEX_BLOOM_FILTER_BITS", "20");
    ScopedEnv e2("TIMESTAR_INDEX_SERIES_CACHE_SIZE", "2000000");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.index.bloom_filter_bits, 20u);
    EXPECT_EQ(cfg.index.series_cache_size, 2000000u);
}

TEST(TimestarConfigEnvTest, SeastarOverrides) {
    ScopedEnv e1("TIMESTAR_SMP", "4");
    ScopedEnv e2("TIMESTAR_MEMORY", "8G");
    ScopedEnv e3("TIMESTAR_OVERPROVISIONED", "true");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_TRUE(cfg.seastar.has("smp"));
    EXPECT_EQ(cfg.seastar.get("smp"), "4");
    EXPECT_TRUE(cfg.seastar.has("memory"));
    EXPECT_EQ(cfg.seastar.get("memory"), "8G");
    EXPECT_TRUE(cfg.seastar.has("overprovisioned"));
    EXPECT_EQ(cfg.seastar.get("overprovisioned"), "true");
}

TEST(TimestarConfigEnvTest, UnsetEnvVarsDoNotOverride) {
    // Ensure no TIMESTAR_ env vars are set for this test
    unsetenv("TIMESTAR_PORT");
    unsetenv("TIMESTAR_LOG_LEVEL");
    unsetenv("TIMESTAR_DATA_DIR");
    timestar::TimestarConfig cfg;
    timestar::applyEnvironmentOverrides(cfg);
    // Should remain at defaults
    EXPECT_EQ(cfg.server.port, 8086);
    EXPECT_EQ(cfg.server.log_level, "info");
    EXPECT_EQ(cfg.server.data_dir, ".");
}

TEST(TimestarConfigEnvTest, EnvOverridesConfigFileValues) {
    // Simulate a config file having set port to 9000, then env overrides to 7070
    timestar::TimestarConfig cfg;
    cfg.server.port = 9000;
    ScopedEnv e("TIMESTAR_PORT", "7070");
    timestar::applyEnvironmentOverrides(cfg);
    EXPECT_EQ(cfg.server.port, 7070);
}

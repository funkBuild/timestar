#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection regression tests for 11 high-severity bugfixes.
// Each test reads the fixed source file and verifies the fix is in place,
// preventing regressions without requiring Seastar runtime.
// =============================================================================

static std::string readFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

class BugfixSourceInspectionTest : public ::testing::Test {};

// ---------------------------------------------------------------------------
// Bug 1: SIMD ComputeHistogram bins[] must not be hardcoded to 8
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug1_HistogramBufferNotHardcoded8) {
    std::string src = readFile(SIMD_AGGREGATOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read simd_aggregator.cpp";

    // The old bug had: "double bins[8]"
    EXPECT_EQ(src.find("double bins[8]"), std::string::npos)
        << "bins[] must not be hardcoded to 8 lanes (ARM SVE overflow risk)";

    // Verify the fix uses HWY_MAX_LANES_D or kMaxLanes
    EXPECT_NE(src.find("HWY_MAX_LANES_D"), std::string::npos)
        << "bins[] should use HWY_MAX_LANES_D for safe sizing";
}

// ---------------------------------------------------------------------------
// Bug 2: lastCompactStats member removed, compact() returns stats directly
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug2_CompactReturnsStats) {
    std::string hppSrc = readFile(TSM_COMPACTOR_HPP_SOURCE_PATH);
    ASSERT_FALSE(hppSrc.empty()) << "Could not read tsm_compactor.hpp";

    // The shared member should be removed
    EXPECT_EQ(hppSrc.find("CompactionStats lastCompactStats"), std::string::npos)
        << "lastCompactStats member must be removed to prevent data races";

    // compact() should return CompactionResult
    EXPECT_NE(hppSrc.find("CompactionResult"), std::string::npos)
        << "compact() should return CompactionResult (path + stats)";

    std::string cppSrc = readFile(TSM_COMPACTOR_SOURCE_PATH);
    ASSERT_FALSE(cppSrc.empty()) << "Could not read tsm_compactor.cpp";

    // No assignment to lastCompactStats
    EXPECT_EQ(cppSrc.find("lastCompactStats = stats"), std::string::npos)
        << "compact() must not assign to shared lastCompactStats member";
}

// ---------------------------------------------------------------------------
// Bug 3: String encoder must copy thread-local buffer, not move it
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug3_StringEncoderNoCopyThreadLocal) {
    std::string src = readFile(STRING_ENCODER_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read string_encoder.cpp";

    // Find the compressStrings function region
    auto fnStart = src.find("compressStrings");
    ASSERT_NE(fnStart, std::string::npos);

    // Check that no std::move(compressed) or std::move(tlCompBuf) exists
    // in the return statement of compressStrings
    auto fnEnd = src.find("\nAlignedBuffer", fnStart);  // Next function after compressStrings
    std::string fnBody = src.substr(fnStart, fnEnd != std::string::npos ? fnEnd - fnStart : std::string::npos);

    EXPECT_EQ(fnBody.find("std::move(compressed)"), std::string::npos)
        << "compressStrings must not std::move the thread-local compressed buffer";
    EXPECT_EQ(fnBody.find("std::move(tlCompBuf)"), std::string::npos)
        << "compressStrings must not std::move the thread-local tlCompBuf";
}

// ---------------------------------------------------------------------------
// Bug 4: Compaction must use atomicReplaceFiles for single-fsync manifest update
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug4_AtomicManifestUpdate) {
    std::string compSrc = readFile(COMPACTION_CPP_SOURCE_PATH);
    ASSERT_FALSE(compSrc.empty()) << "Could not read compaction.cpp";

    EXPECT_NE(compSrc.find("atomicReplaceFiles"), std::string::npos)
        << "NativeIndex compaction must use atomicReplaceFiles for crash safety";

    std::string hppSrc = readFile(MANIFEST_HPP_SOURCE_PATH);
    ASSERT_FALSE(hppSrc.empty()) << "Could not read manifest.hpp";

    EXPECT_NE(hppSrc.find("atomicReplaceFiles"), std::string::npos)
        << "Manifest must declare atomicReplaceFiles method";
}

// ---------------------------------------------------------------------------
// Bug 5: insertBatch must increment metrics counters
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug5_InsertBatchMetrics) {
    std::string src = readFile(ENGINE_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read engine.cpp";

    // Find the insertBatch function
    auto fnStart = src.find("Engine::insertBatch");
    ASSERT_NE(fnStart, std::string::npos);

    // Check for metrics increment within the function body (up to the next Engine:: function)
    auto fnEnd = src.find("\nEngine::", fnStart + 20);
    std::string fnBody = src.substr(fnStart, fnEnd != std::string::npos ? fnEnd - fnStart : 500);

    EXPECT_NE(fnBody.find("_metrics.inserts_total"), std::string::npos)
        << "insertBatch must increment _metrics.inserts_total";
    EXPECT_NE(fnBody.find("insert_points_total"), std::string::npos)
        << "insertBatch must increment _metrics.insert_points_total";
}

// ---------------------------------------------------------------------------
// Bug 6: query() 4-arg overload must use seriesId parameter (not ignore it)
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug6_QueryUsesSeriesId) {
    std::string src = readFile(ENGINE_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read engine.cpp";

    // The parameter must NOT be commented out
    EXPECT_EQ(src.find("const SeriesId128& /* seriesId */"), std::string::npos)
        << "query() 4-arg overload must not comment out seriesId parameter";

    // The runner.runQuery call should pass seriesId
    auto queryFn = src.find("Engine::query(std::string series, const SeriesId128& seriesId,");
    ASSERT_NE(queryFn, std::string::npos) << "4-arg query overload must exist with named seriesId";
}

// ---------------------------------------------------------------------------
// Bug 7: WAL init() must not double-remove WAL files
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug7_NoDoubleWALRemoval) {
    std::string src = readFile(WAL_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read wal_file_manager.cpp";

    // The fix should guard the removal with file_exists
    auto initFn = src.find("WALFileManager::init(");
    ASSERT_NE(initFn, std::string::npos) << "WALFileManager::init not found in source";

    auto initEnd = src.find("\nWALFileManager::", initFn + 25);
    std::string initBody = src.substr(initFn, initEnd != std::string::npos ? initEnd - initFn : std::string::npos);

    // Should have file_exists guard before remove_file
    EXPECT_NE(initBody.find("file_exists"), std::string::npos)
        << "WAL init() must guard remove_file with file_exists to prevent double removal";
}

// ---------------------------------------------------------------------------
// Bug 8: Compactor must use when_all_succeed instead of when_all
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug8_WhenAllSucceedUsed) {
    std::string src = readFile(TSM_COMPACTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read tsm_compactor.cpp";

    // Find all "when_all(" occurrences that are NOT "when_all_succeed("
    size_t pos = 0;
    while ((pos = src.find("when_all(", pos)) != std::string::npos) {
        // Check if it's actually when_all_succeed
        if (pos >= 9 && src.substr(pos - 9, 18) == "when_all_succeed(r") {
            pos += 9;
            continue;
        }
        // Check the surrounding context (allow when_all_succeed but not bare when_all)
        size_t lineStart = src.rfind('\n', pos);
        std::string line = src.substr(lineStart != std::string::npos ? lineStart : 0,
                                       pos + 30 - (lineStart != std::string::npos ? lineStart : 0));
        if (line.find("when_all_succeed") == std::string::npos) {
            FAIL() << "Found bare 'when_all(' (without _succeed) in tsm_compactor.cpp near: "
                   << line;
        }
        pos += 9;
    }
}

// ---------------------------------------------------------------------------
// Bug 9: Compactor must have defer/cleanup guard for temp file
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug9_TempFileCleanupGuard) {
    std::string src = readFile(TSM_COMPACTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read tsm_compactor.cpp";

    // The compact() function must clean up temp files on failure via catch block
    EXPECT_NE(src.find("std::filesystem::remove(tempPath"), std::string::npos)
        << "compact() must clean up temp files on failure in the catch block";
}

// ---------------------------------------------------------------------------
// Bug 10: writeMemstore must use .tmp intermediate file
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug10_WriteMemstoreUseTmpFile) {
    std::string src = readFile(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read tsm_file_manager.cpp";

    // Find writeMemstore function
    auto fnStart = src.find("TSMFileManager::writeMemstore");
    ASSERT_NE(fnStart, std::string::npos);

    auto fnEnd = src.find("\nTSMFileManager::", fnStart + 30);
    std::string fnBody = src.substr(fnStart, fnEnd != std::string::npos ? fnEnd - fnStart : std::string::npos);

    EXPECT_NE(fnBody.find(".tmp"), std::string::npos)
        << "writeMemstore must write to a .tmp file first";
    EXPECT_NE(fnBody.find("rename_file"), std::string::npos)
        << "writeMemstore must rename .tmp to final after successful write";
}

// ---------------------------------------------------------------------------
// Bug 11: line_parser tags must use std::string, not std::string_view
// ---------------------------------------------------------------------------
TEST_F(BugfixSourceInspectionTest, Bug11_LineParserOwnsTags) {
    std::string src = readFile(LINE_PARSER_HPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read line_parser.hpp";

    // Must NOT have string_view tags
    EXPECT_EQ(src.find("map<std::string_view, std::string_view> tags"), std::string::npos)
        << "tags must not use string_view (dangling references)";

    // Must have owning std::string tags
    EXPECT_NE(src.find("map<std::string, std::string> tags"), std::string::npos)
        << "tags must use std::string to own tag data";
}

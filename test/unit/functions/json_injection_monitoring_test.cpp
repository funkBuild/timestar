#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #17: JSON injection in function monitoring and HTTP handler
//
// Function names and alert messages were interpolated into JSON strings
// without escaping, allowing injection attacks. The fix wraps all
// user-controlled strings with jsonEscape() before JSON interpolation.
// =============================================================================

// ---------------------------------------------------------------------------
// Source-inspection tests for function_monitoring.cpp
// ---------------------------------------------------------------------------
class FunctionMonitoringJsonInjectionTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef FUNCTION_MONITORING_SOURCE_PATH
        std::ifstream file(FUNCTION_MONITORING_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/functions/function_monitoring.cpp",
            "../../lib/functions/function_monitoring.cpp",
        };
        for (const auto& path : paths) {
            std::ifstream f(path);
            if (f.is_open()) {
                std::stringstream ss;
                ss << f.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }

    // Extract function body by name
    std::string extractFunction(const std::string& signature) const {
        auto pos = sourceCode.find(signature);
        if (pos == std::string::npos) return "";
        auto braceStart = sourceCode.find('{', pos);
        if (braceStart == std::string::npos) return "";
        int depth = 1;
        size_t i = braceStart + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }
        return sourceCode.substr(pos, i - pos);
    }
};

TEST_F(FunctionMonitoringJsonInjectionTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load function_monitoring.cpp";
}

TEST_F(FunctionMonitoringJsonInjectionTest, IncludesJsonEscapeHeader) {
    EXPECT_NE(sourceCode.find("json_escape.hpp"), std::string::npos)
        << "function_monitoring.cpp must include json_escape.hpp";
}

TEST_F(FunctionMonitoringJsonInjectionTest, ExportMetricsEscapesFunctionName) {
    std::string body = extractFunction("exportMetricsAsJson");
    ASSERT_FALSE(body.empty()) << "Could not find exportMetricsAsJson function";

    // The function name used as JSON key must be escaped
    EXPECT_NE(body.find("jsonEscape(name)"), std::string::npos)
        << "exportMetricsAsJson must escape function name with jsonEscape()";
}

TEST_F(FunctionMonitoringJsonInjectionTest, ExportAlertsEscapesFunctionName) {
    std::string body = extractFunction("exportAlertsAsJson");
    ASSERT_FALSE(body.empty()) << "Could not find exportAlertsAsJson function";

    EXPECT_NE(body.find("jsonEscape(alert.function_name)"), std::string::npos)
        << "exportAlertsAsJson must escape alert.function_name with jsonEscape()";
}

TEST_F(FunctionMonitoringJsonInjectionTest, ExportAlertsEscapesMessage) {
    std::string body = extractFunction("exportAlertsAsJson");
    ASSERT_FALSE(body.empty()) << "Could not find exportAlertsAsJson function";

    EXPECT_NE(body.find("jsonEscape(alert.message)"), std::string::npos)
        << "exportAlertsAsJson must escape alert.message with jsonEscape()";
}

// ---------------------------------------------------------------------------
// Source-inspection tests for function_http_handler.cpp
// ---------------------------------------------------------------------------
class FunctionHttpHandlerJsonInjectionTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef FUNCTION_HTTP_HANDLER_SOURCE_PATH
        std::ifstream file(FUNCTION_HTTP_HANDLER_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/functions/function_http_handler.cpp",
            "../../lib/functions/function_http_handler.cpp",
        };
        for (const auto& path : paths) {
            std::ifstream f(path);
            if (f.is_open()) {
                std::stringstream ss;
                ss << f.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }

    std::string extractFunction(const std::string& signature) const {
        auto pos = sourceCode.find(signature);
        if (pos == std::string::npos) return "";
        auto braceStart = sourceCode.find('{', pos);
        if (braceStart == std::string::npos) return "";
        int depth = 1;
        size_t i = braceStart + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }
        return sourceCode.substr(pos, i - pos);
    }
};

TEST_F(FunctionHttpHandlerJsonInjectionTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load function_http_handler.cpp";
}

TEST_F(FunctionHttpHandlerJsonInjectionTest, GetPerformanceStatsEscapesFunctionNames) {
    std::string body = extractFunction("getPerformanceStats");
    ASSERT_FALSE(body.empty()) << "Could not find getPerformanceStats function";

    // pair.first (function name) used as JSON key must be escaped
    EXPECT_NE(body.find("jsonEscape(pair.first)"), std::string::npos)
        << "getPerformanceStats must escape function names with jsonEscape()";
}

TEST_F(FunctionHttpHandlerJsonInjectionTest, CreateErrorReplyEscapesError) {
    std::string body = extractFunction("createErrorReply");
    ASSERT_FALSE(body.empty()) << "Could not find createErrorReply function";

    // The error message in the JSON must be escaped
    EXPECT_NE(body.find("jsonEscape(error)"), std::string::npos)
        << "createErrorReply must escape error message with jsonEscape()";
}

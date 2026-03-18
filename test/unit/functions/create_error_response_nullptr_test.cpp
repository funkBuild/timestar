#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #18: createErrorResponse returns nullptr
//
// FunctionHttpHandler::createErrorResponse returned nullptr instead of a
// valid reply object. Any caller that dereferenced the result would crash.
// The fix constructs a proper reply with status, Content-Type, and JSON body.
// =============================================================================

class CreateErrorResponseNullptrTest : public ::testing::Test {
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

TEST_F(CreateErrorResponseNullptrTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load function_http_handler.cpp";
}

TEST_F(CreateErrorResponseNullptrTest, DoesNotReturnNullptr) {
    std::string body = extractFunction("createErrorResponse");
    ASSERT_FALSE(body.empty()) << "Could not find createErrorResponse function";

    EXPECT_EQ(body.find("return nullptr"), std::string::npos)
        << "createErrorResponse must NOT return nullptr — it must return a valid reply";
}

TEST_F(CreateErrorResponseNullptrTest, ReturnsMakeUnique) {
    std::string body = extractFunction("createErrorResponse");
    ASSERT_FALSE(body.empty()) << "Could not find createErrorResponse function";

    EXPECT_NE(body.find("make_unique"), std::string::npos)
        << "createErrorResponse should construct a reply via std::make_unique";
}

TEST_F(CreateErrorResponseNullptrTest, SetsContentType) {
    std::string body = extractFunction("createErrorResponse");
    ASSERT_FALSE(body.empty()) << "Could not find createErrorResponse function";

    EXPECT_NE(body.find("Content-Type"), std::string::npos)
        << "createErrorResponse should set Content-Type header";
}

TEST_F(CreateErrorResponseNullptrTest, EscapesErrorMessage) {
    std::string body = extractFunction("createErrorResponse");
    ASSERT_FALSE(body.empty()) << "Could not find createErrorResponse function";

    EXPECT_NE(body.find("jsonEscape"), std::string::npos)
        << "createErrorResponse must escape the error message with jsonEscape()";
}

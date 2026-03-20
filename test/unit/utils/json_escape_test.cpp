#include "../../lib/utils/json_escape.hpp"
#include <gtest/gtest.h>

using namespace timestar;

TEST(JsonEscapeTest, EmptyString) {
    EXPECT_EQ(jsonEscape(""), "");
}

TEST(JsonEscapeTest, NoEscapingNeeded) {
    EXPECT_EQ(jsonEscape("hello world"), "hello world");
    EXPECT_EQ(jsonEscape("abc123"), "abc123");
}

TEST(JsonEscapeTest, DoubleQuote) {
    EXPECT_EQ(jsonEscape("say \"hello\""), "say \\\"hello\\\"");
}

TEST(JsonEscapeTest, Backslash) {
    EXPECT_EQ(jsonEscape("path\\to\\file"), "path\\\\to\\\\file");
}

TEST(JsonEscapeTest, Newline) {
    EXPECT_EQ(jsonEscape("line1\nline2"), "line1\\nline2");
}

TEST(JsonEscapeTest, CarriageReturn) {
    EXPECT_EQ(jsonEscape("line1\rline2"), "line1\\rline2");
}

TEST(JsonEscapeTest, Tab) {
    EXPECT_EQ(jsonEscape("col1\tcol2"), "col1\\tcol2");
}

TEST(JsonEscapeTest, NullByte) {
    std::string input("null\0byte", 9);
    std::string result = jsonEscape(input);
    EXPECT_NE(result.find("\\u0000"), std::string::npos);
}

TEST(JsonEscapeTest, ControlCharacters) {
    // \x01 through \x1F (except \n \r \t which have named escapes)
    std::string input(1, '\x01');
    EXPECT_EQ(jsonEscape(input), "\\u0001");

    input = std::string(1, '\x1F');
    EXPECT_EQ(jsonEscape(input), "\\u001f");

    // Bell character
    input = std::string(1, '\x07');
    EXPECT_EQ(jsonEscape(input), "\\u0007");
}

TEST(JsonEscapeTest, DEL_0x7F) {
    std::string input(1, '\x7F');
    EXPECT_EQ(jsonEscape(input), "\\u007f");
}

TEST(JsonEscapeTest, UTF8Passthrough) {
    // UTF-8 multi-byte characters should pass through unescaped
    EXPECT_EQ(jsonEscape("café"), "café");
    EXPECT_EQ(jsonEscape("日本語"), "日本語");
    EXPECT_EQ(jsonEscape("🎉"), "🎉");
}

TEST(JsonEscapeTest, MixedContent) {
    EXPECT_EQ(jsonEscape("key=\"value\"\nnext"), "key=\\\"value\\\"\\nnext");
}

TEST(JsonEscapeTest, AllSpecialCharsAtOnce) {
    std::string input = "\"\\\n\r\t";
    EXPECT_EQ(jsonEscape(input), "\\\"\\\\\\n\\r\\t");
}

TEST(JsonEscapeTest, AppendToExistingString) {
    std::string out = "prefix:";
    jsonEscapeAppend("a\"b", out);
    EXPECT_EQ(out, "prefix:a\\\"b");
}

TEST(JsonEscapeTest, LongStringPerformance) {
    // Verify no crash or corruption on large inputs
    std::string input(10000, 'x');
    input[5000] = '"';
    input[9000] = '\\';
    std::string result = jsonEscape(input);
    EXPECT_GT(result.size(), input.size());
    EXPECT_NE(result.find("\\\""), std::string::npos);
    EXPECT_NE(result.find("\\\\"), std::string::npos);
}

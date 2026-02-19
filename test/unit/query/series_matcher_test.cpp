#include <gtest/gtest.h>
#include "../../../lib/query/series_matcher.hpp"
#include <map>
#include <string>

using namespace tsdb;

class SeriesMatcherTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test exact matching
TEST_F(SeriesMatcherTest, ExactMatch) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "server-01"},
        {"datacenter", "dc1"},
        {"region", "us-west"}
    };
    
    // Exact match - should match
    std::map<std::string, std::string> scopes1 = {
        {"host", "server-01"},
        {"datacenter", "dc1"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes1));
    
    // Exact match with all tags - should match
    std::map<std::string, std::string> scopes2 = {
        {"host", "server-01"},
        {"datacenter", "dc1"},
        {"region", "us-west"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes2));
    
    // Different value - should not match
    std::map<std::string, std::string> scopes3 = {
        {"host", "server-02"}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes3));
    
    // Missing tag in series - should not match
    std::map<std::string, std::string> scopes4 = {
        {"nonexistent", "value"}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes4));
}

// Test empty scopes (match all)
TEST_F(SeriesMatcherTest, EmptyScopes) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "server-01"}
    };
    
    std::map<std::string, std::string> emptyScopes;
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, emptyScopes));
}

// Test wildcard matching
TEST_F(SeriesMatcherTest, WildcardMatching) {
    // Test star wildcard
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("server-01", "server-*"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("server-123", "server-*"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("production", "*duction"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("production", "prod*"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("production", "*"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("server-01", "client-*"));
    
    // Test question mark wildcard
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("server-01", "server-0?"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("server-09", "server-0?"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("server-123", "server-0?"));
    
    // Test combined wildcards
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("server-01-prod", "server-?""?-*"));
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("host-a-test", "host-?-*"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("host-abc-test", "host-?-*"));
}

// Test regex matching
TEST_F(SeriesMatcherTest, RegexMatching) {
    // Simple regex
    EXPECT_TRUE(SeriesMatcher::matchesRegex("server-01", "server-[0-9]+"));
    EXPECT_TRUE(SeriesMatcher::matchesRegex("server-123", "server-[0-9]+"));
    EXPECT_FALSE(SeriesMatcher::matchesRegex("server-abc", "server-[0-9]+"));
    
    // Complex regex
    EXPECT_TRUE(SeriesMatcher::matchesRegex("prod-us-west-2", "prod-us-(west|east)-[0-9]"));
    EXPECT_TRUE(SeriesMatcher::matchesRegex("prod-us-east-1", "prod-us-(west|east)-[0-9]"));
    EXPECT_FALSE(SeriesMatcher::matchesRegex("prod-us-north-1", "prod-us-(west|east)-[0-9]"));
    
    // Anchored regex
    EXPECT_TRUE(SeriesMatcher::matchesRegex("test123", "test[0-9]+"));
    EXPECT_FALSE(SeriesMatcher::matchesRegex("mytest123", "^test[0-9]+"));
    
    // Invalid regex should return false
    EXPECT_FALSE(SeriesMatcher::matchesRegex("test", "[invalid"));
}

// Test tag matching with wildcards
TEST_F(SeriesMatcherTest, TagMatchingWithWildcards) {
    // Wildcard in tag value
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-01", "server-*"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-01", "server-0?"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("server-01", "client-*"));
    
    // Exact match
    EXPECT_TRUE(SeriesMatcher::matchesTag("production", "production"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("production", "staging"));
}

// Test tag matching with regex
TEST_F(SeriesMatcherTest, TagMatchingWithRegex) {
    // Regex pattern (starts with /)
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-01", "/server-[0-9]+/"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-123", "/server-[0-9]+/"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("server-abc", "/server-[0-9]+/"));
    
    // Invalid regex format (no closing /)
    EXPECT_FALSE(SeriesMatcher::matchesTag("server-01", "/server-[0-9]+"));
}

// Test complete series matching with wildcards
TEST_F(SeriesMatcherTest, SeriesMatchingWithWildcards) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "server-01"},
        {"env", "production"},
        {"region", "us-west-2"}
    };
    
    // Wildcard in scopes
    std::map<std::string, std::string> scopes1 = {
        {"host", "server-*"},
        {"env", "production"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes1));
    
    // Multiple wildcards
    std::map<std::string, std::string> scopes2 = {
        {"host", "server-??"},
        {"region", "us-*"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes2));
    
    // Mixed exact and wildcard
    std::map<std::string, std::string> scopes3 = {
        {"host", "server-*"},
        {"env", "staging"}  // Exact match fails
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes3));
}

// Test complete series matching with regex
TEST_F(SeriesMatcherTest, SeriesMatchingWithRegex) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "server-42"},
        {"datacenter", "dc1"}
    };
    
    // Regex pattern in scopes
    std::map<std::string, std::string> scopes1 = {
        {"host", "/server-[0-9]+/"},
        {"datacenter", "dc1"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes1));
    
    // Regex that doesn't match
    std::map<std::string, std::string> scopes2 = {
        {"host", "/client-[0-9]+/"}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes2));
}

// Test wildcardToRegex conversion
TEST_F(SeriesMatcherTest, WildcardToRegexConversion) {
    // The wildcardToRegex function is private, so we test it indirectly
    // through matchesWildcard
    
    // Test that special regex characters are escaped
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("file.txt", "file.txt"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("fileatxt", "file.txt"));
    
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("value+plus", "value+plus"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("valueplus", "value+plus"));
    
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("test[1]", "test[1]"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("test1", "test[1]"));
}

// Test edge cases
TEST_F(SeriesMatcherTest, EdgeCases) {
    std::map<std::string, std::string> seriesTags = {
        {"key", "value"}
    };
    
    // Empty string values
    std::map<std::string, std::string> emptyValueScope = {
        {"key", ""}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, emptyValueScope));
    
    // Special characters in exact match
    std::map<std::string, std::string> specialTags = {
        {"key", "value-with.special+chars[]"}
    };
    std::map<std::string, std::string> specialScope = {
        {"key", "value-with.special+chars[]"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(specialTags, specialScope));
}

// Test case sensitivity
TEST_F(SeriesMatcherTest, CaseSensitivity) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "Server-01"}
    };
    
    // Case sensitive exact match
    std::map<std::string, std::string> scopes1 = {
        {"host", "Server-01"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes1));
    
    std::map<std::string, std::string> scopes2 = {
        {"host", "server-01"}  // Different case
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes2));
    
    // Case sensitive wildcard
    EXPECT_TRUE(SeriesMatcher::matchesWildcard("Server-01", "Server-*"));
    EXPECT_FALSE(SeriesMatcher::matchesWildcard("Server-01", "server-*"));
}

// Test ~regex matching via matchesTag
TEST_F(SeriesMatcherTest, TildeRegexMatching) {
    // Basic ~regex
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-01", "~server-[0-9]+"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-123", "~server-[0-9]+"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("server-abc", "~server-[0-9]+"));

    // ~regex with alternation
    EXPECT_TRUE(SeriesMatcher::matchesTag("us-west", "~us-(west|east)"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("us-east", "~us-(west|east)"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("us-north", "~us-(west|east)"));

    // ~regex with character class range
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-01", "~server-0[1-3]"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-02", "~server-0[1-3]"));
    EXPECT_TRUE(SeriesMatcher::matchesTag("server-03", "~server-0[1-3]"));
    EXPECT_FALSE(SeriesMatcher::matchesTag("server-04", "~server-0[1-3]"));

    // Invalid ~regex
    EXPECT_FALSE(SeriesMatcher::matchesTag("test", "~[invalid"));
}

// Test classifyScope
TEST_F(SeriesMatcherTest, ClassifyScope) {
    // Exact
    EXPECT_EQ(SeriesMatcher::classifyScope("server-01"), ScopeMatchType::EXACT);
    EXPECT_EQ(SeriesMatcher::classifyScope("production"), ScopeMatchType::EXACT);
    EXPECT_EQ(SeriesMatcher::classifyScope(""), ScopeMatchType::EXACT);

    // Wildcard
    EXPECT_EQ(SeriesMatcher::classifyScope("server-*"), ScopeMatchType::WILDCARD);
    EXPECT_EQ(SeriesMatcher::classifyScope("server-0?"), ScopeMatchType::WILDCARD);
    EXPECT_EQ(SeriesMatcher::classifyScope("*"), ScopeMatchType::WILDCARD);

    // Regex
    EXPECT_EQ(SeriesMatcher::classifyScope("~server-[0-9]+"), ScopeMatchType::REGEX);
    EXPECT_EQ(SeriesMatcher::classifyScope("~.*"), ScopeMatchType::REGEX);
    EXPECT_EQ(SeriesMatcher::classifyScope("/server-[0-9]+/"), ScopeMatchType::REGEX);
}

// Test extractLiteralPrefix
TEST_F(SeriesMatcherTest, ExtractLiteralPrefix) {
    // Exact match returns full value
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("server-01"), "server-01");

    // Wildcard — up to first metachar
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("server-*"), "server-");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("server-0?"), "server-0");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("*"), "");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("?foo"), "");

    // ~regex — strip ~ then scan for metachar
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("~server-[0-9]+"), "server-");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("~[a-z]+"), "");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("~prod-us-(west|east)"), "prod-us-");

    // /regex/ — strip slashes then scan
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("/server-[0-9]+/"), "server-");
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix("/[a-z]+/"), "");

    // Empty
    EXPECT_EQ(SeriesMatcher::extractLiteralPrefix(""), "");
}

// Test series matching with ~regex in scopes
TEST_F(SeriesMatcherTest, SeriesMatchingWithTildeRegex) {
    std::map<std::string, std::string> seriesTags = {
        {"host", "server-42"},
        {"datacenter", "dc1"}
    };

    std::map<std::string, std::string> scopes1 = {
        {"host", "~server-[0-9]+"},
        {"datacenter", "dc1"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, scopes1));

    std::map<std::string, std::string> scopes2 = {
        {"host", "~client-[0-9]+"}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, scopes2));
}
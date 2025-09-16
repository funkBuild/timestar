#ifndef __FUNCTION_SECURITY_H_INCLUDED__
#define __FUNCTION_SECURITY_H_INCLUDED__

#include <string>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <cctype>
#include <regex>

namespace tsdb::functions {

/**
 * Security validation utility for TSDB function system.
 * Provides comprehensive input validation and sanitization to protect against:
 * - Parameter injection attacks
 * - Path traversal attempts
 * - Command injection
 * - Code execution attempts
 * - Buffer overflow attacks
 * - SQL injection-style attacks
 * - XSS payload injection
 */
class FunctionSecurity {
public:
    struct ValidationResult {
        bool isValid = false;
        std::string errorCode;
        std::string errorMessage;
        std::string sanitizedInput;
    };

    /**
     * Validate a function name for security issues
     */
    static ValidationResult validateFunctionName(const std::string& functionName);

    /**
     * Validate function parameters for security issues
     */
    static ValidationResult validateParameters(const std::string& parameters);

    /**
     * Validate a complete function query for security issues
     */
    static ValidationResult validateFunctionQuery(const std::string& query);

    /**
     * Sanitize input string by removing/replacing dangerous characters
     */
    static std::string sanitizeInput(const std::string& input);

    /**
     * Check if input contains dangerous patterns
     */
    static bool containsDangerousPatterns(const std::string& input);

    /**
     * Validate JSON input for structure and security
     */
    static ValidationResult validateJsonInput(const std::string& json);

private:
    // Known dangerous function names
    static const std::unordered_set<std::string> dangerousFunctionNames_;
    
    // Dangerous patterns to detect
    static const std::vector<std::regex> dangerousPatterns_;
    
    // Maximum allowed input sizes
    static const size_t MAX_FUNCTION_NAME_LENGTH = 64;
    static const size_t MAX_PARAMETER_LENGTH = 1000;
    static const size_t MAX_QUERY_LENGTH = 2000;
    static const size_t MAX_JSON_LENGTH = 5000;
    
    // Helper methods
    static bool isValidFunctionNameChar(char c);
    static bool containsPathTraversal(const std::string& input);
    static bool containsNullBytes(const std::string& input);
    static bool containsControlCharacters(const std::string& input);
    static std::string removeControlCharacters(const std::string& input);
    static bool exceedsMaxLength(const std::string& input, size_t maxLength);
};

} // namespace tsdb::functions

#endif // __FUNCTION_SECURITY_H_INCLUDED__
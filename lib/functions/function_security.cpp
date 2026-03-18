#include "function_security.hpp"

#include <iomanip>
#include <sstream>

namespace timestar::functions {

// Static member definitions
const std::unordered_set<std::string> FunctionSecurity::dangerousFunctionNames_ = {
    // System commands
    "system", "exec", "eval", "shell", "bash", "sh", "cmd", "powershell", "shell_exec", "passthru", "popen",
    "proc_open",

    // File operations
    "file", "fopen", "fread", "fwrite", "readfile", "file_get_contents", "include", "include_once", "require",
    "require_once",

    // Network operations
    "curl", "wget", "http", "ftp", "socket", "fsockopen",

    // Code execution
    "__import__", "__builtins__", "getattr", "setattr", "delattr", "compile", "execfile", "reload", "input",
    "raw_input",

    // Administrative functions
    "admin", "root", "sudo", "su", "chmod", "chown", "kill",

    // Database operations
    "drop", "delete", "truncate", "alter", "create", "insert", "update", "union", "select", "where", "from", "join",
    "having", "order",

    // Special characters and protocols
    "javascript", "vbscript", "data", "file", "ftp", "http", "https",

    // Path-related
    "cd", "chdir", "pwd", "ls", "dir", "find", "locate",

    // Process control
    "fork", "spawn", "thread", "process", "exit", "abort"};

// Lazily initialized on first call. Function-local static avoids global
// constructor ordering issues and safely contains any regex_error.
// NOTE: ~30 regex patterns are evaluated per request. For production use, consider
// replacing simple string patterns with std::string::find() and reserving regex
// for genuinely complex patterns. The ProductionMonitor's std::mutex should be
// replaced with lock-free atomics or Seastar timers to avoid blocking the reactor.
const std::vector<std::regex>& FunctionSecurity::getDangerousPatterns() {
    static const std::vector<std::regex> patterns = [] {
        std::vector<std::regex> p;
        // Pre-allocate to avoid repeated reallocation
        p.reserve(30);
        auto add = [&](const char* pattern) { p.emplace_back(pattern, std::regex_constants::icase); };

        // Code injection patterns
        add(R"(__import__\s*\()");
        add(R"(exec\s*\()");
        add(R"(eval\s*\()");
        add(R"(system\s*\()");
        add(R"(shell_exec\s*\()");

        // Command injection patterns — only match shell metacharacters.
        // Parentheses (), braces {}, and brackets [] are legitimate query
        // language syntax and must NOT be blocked here.
        add(R"([;&|`$])");
        add(R"(\\x[0-9a-f]{2})");

        // Command-like patterns (spaces between command arguments)
        add(R"(rm\s+)");
        add(R"(sudo\s+)");
        add(R"(chmod\s+)");
        add(R"(kill\s+)");
        add(R"(ps\s+)");
        add(R"(ls\s+)");
        add(R"(cat\s+)");
        add(R"(curl\s+)");
        add(R"(wget\s+)");
        add(R"(nc\s+)");

        // Path traversal patterns
        add(R"(\.\./|\.\.\\)");
        add(R"(%2e%2e%2f|%2e%2e%5c)");
        add(R"(\\\.\\\.\\|\\\.\\\./)");

        // SQL injection patterns
        add(R"('\s*or\s*')");
        add(R"('\s*union\s*select)");
        add(R"(drop\s+table)");
        add(R"(delete\s+from)");
        add(R"(insert\s+into)");

        // XSS patterns
        add(R"(<script[^>]*>)");
        add(R"(javascript\s*:)");
        // Bound \w to {1,30} to prevent ReDoS (longest HTML event attr is 23 chars)
        add(R"(on\w{1,30}\s*=)");

        // Format string attacks — match %n (dangerous write primitive) and multi-digit
        // width specifiers like %10s, %08x, but NOT bare %Xd or %Xs which collide with
        // URL-encoded values (%2d = '-', %3d = '=', etc.)
        add(R"(%[0-9]{2,}[sdxn]|%n)");

        // LDAP injection
        add(R"(\*\)\([\w=*]+\)\(\|)");

        // Protocol injection
        add(R"(file:\/\/|ftp:\/\/|ldap:\/\/)");

        return p;
    }();
    return patterns;
}

FunctionSecurity::ValidationResult FunctionSecurity::validateFunctionName(const std::string& functionName) {
    ValidationResult result;

    // Check for empty name
    if (functionName.empty()) {
        result.errorCode = "EMPTY_FUNCTION_NAME";
        result.errorMessage = "Function name cannot be empty";
        return result;
    }

    // Check maximum length
    if (exceedsMaxLength(functionName, MAX_FUNCTION_NAME_LENGTH)) {
        result.errorCode = "FUNCTION_NAME_TOO_LONG";
        result.errorMessage =
            "Function name exceeds maximum length of " + std::to_string(MAX_FUNCTION_NAME_LENGTH) + " characters";
        return result;
    }

    // Check for null bytes
    if (containsNullBytes(functionName)) {
        result.errorCode = "NULL_BYTES_DETECTED";
        result.errorMessage = "Function name contains null bytes";
        return result;
    }

    // Check for control characters
    if (containsControlCharacters(functionName)) {
        result.errorCode = "CONTROL_CHARACTERS_DETECTED";
        result.errorMessage = "Function name contains control characters";
        return result;
    }

    // Check for path traversal
    if (containsPathTraversal(functionName)) {
        result.errorCode = "PATH_TRAVERSAL_DETECTED";
        result.errorMessage = "Function name contains path traversal sequences";
        return result;
    }

    // Convert to lowercase for checking dangerous names
    std::string lowerName = functionName;
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);

    // Check against dangerous function names
    if (dangerousFunctionNames_.find(lowerName) != dangerousFunctionNames_.end()) {
        result.errorCode = "DANGEROUS_FUNCTION_NAME";
        result.errorMessage = "Function name '" + functionName + "' is not allowed for security reasons";
        return result;
    }

    // Check for dangerous patterns
    if (containsDangerousPatterns(functionName)) {
        result.errorCode = "DANGEROUS_PATTERN_DETECTED";
        result.errorMessage = "Function name contains dangerous patterns";
        return result;
    }

    // Validate character set (only allow alphanumeric, underscore, hyphen)
    for (char c : functionName) {
        if (!isValidFunctionNameChar(c)) {
            result.errorCode = "INVALID_CHARACTER";
            result.errorMessage = "Function name contains invalid character: '" + std::string(1, c) + "'";
            return result;
        }
    }

    // All checks passed
    result.isValid = true;
    result.sanitizedInput = sanitizeInput(functionName);
    return result;
}

FunctionSecurity::ValidationResult FunctionSecurity::validateParameters(const std::string& parameters) {
    ValidationResult result;

    // Check maximum length
    if (exceedsMaxLength(parameters, MAX_PARAMETER_LENGTH)) {
        result.errorCode = "PARAMETERS_TOO_LONG";
        result.errorMessage =
            "Parameters exceed maximum length of " + std::to_string(MAX_PARAMETER_LENGTH) + " characters";
        return result;
    }

    // Check for null bytes
    if (containsNullBytes(parameters)) {
        result.errorCode = "NULL_BYTES_DETECTED";
        result.errorMessage = "Parameters contain null bytes";
        return result;
    }

    // Check for dangerous patterns
    if (containsDangerousPatterns(parameters)) {
        result.errorCode = "DANGEROUS_PATTERN_DETECTED";
        result.errorMessage = "Parameters contain dangerous patterns";
        return result;
    }

    // All checks passed
    result.isValid = true;
    result.sanitizedInput = sanitizeInput(parameters);
    return result;
}

FunctionSecurity::ValidationResult FunctionSecurity::validateFunctionQuery(const std::string& query) {
    ValidationResult result;

    // Check maximum length
    if (exceedsMaxLength(query, MAX_QUERY_LENGTH)) {
        result.errorCode = "QUERY_TOO_LONG";
        result.errorMessage = "Query exceeds maximum length of " + std::to_string(MAX_QUERY_LENGTH) + " characters";
        return result;
    }

    // Check for null bytes
    if (containsNullBytes(query)) {
        result.errorCode = "NULL_BYTES_DETECTED";
        result.errorMessage = "Query contains null bytes";
        return result;
    }

    // Check for dangerous patterns
    if (containsDangerousPatterns(query)) {
        result.errorCode = "DANGEROUS_PATTERN_DETECTED";
        result.errorMessage = "Query contains dangerous patterns";
        return result;
    }

    // All checks passed
    result.isValid = true;
    result.sanitizedInput = sanitizeInput(query);
    return result;
}

FunctionSecurity::ValidationResult FunctionSecurity::validateJsonInput(const std::string& json) {
    ValidationResult result;

    // Check maximum length
    if (exceedsMaxLength(json, MAX_JSON_LENGTH)) {
        result.errorCode = "JSON_TOO_LONG";
        result.errorMessage = "JSON input exceeds maximum length of " + std::to_string(MAX_JSON_LENGTH) + " characters";
        return result;
    }

    // Check for null bytes
    if (containsNullBytes(json)) {
        result.errorCode = "NULL_BYTES_DETECTED";
        result.errorMessage = "JSON input contains null bytes";
        return result;
    }

    // Basic JSON structure validation
    size_t openBraces = 0, closeBraces = 0;
    size_t openBrackets = 0, closeBrackets = 0;
    bool inString = false;
    bool escaped = false;

    for (size_t i = 0; i < json.length(); ++i) {
        char c = json[i];

        if (escaped) {
            escaped = false;
            continue;
        }

        if (c == '\\') {
            escaped = true;
            continue;
        }

        if (c == '"') {
            inString = !inString;
            continue;
        }

        if (!inString) {
            switch (c) {
                case '{':
                    openBraces++;
                    break;
                case '}':
                    closeBraces++;
                    break;
                case '[':
                    openBrackets++;
                    break;
                case ']':
                    closeBrackets++;
                    break;
            }
        }
    }

    if (openBraces != closeBraces) {
        result.errorCode = "MALFORMED_JSON";
        result.errorMessage = "JSON has mismatched braces";
        return result;
    }

    if (openBrackets != closeBrackets) {
        result.errorCode = "MALFORMED_JSON";
        result.errorMessage = "JSON has mismatched brackets";
        return result;
    }

    // Check for dangerous patterns
    if (containsDangerousPatterns(json)) {
        result.errorCode = "DANGEROUS_PATTERN_DETECTED";
        result.errorMessage = "JSON input contains dangerous patterns";
        return result;
    }

    // All checks passed
    result.isValid = true;
    result.sanitizedInput = sanitizeInput(json);
    return result;
}

std::string FunctionSecurity::sanitizeInput(const std::string& input) {
    std::string sanitized = removeControlCharacters(input);

    // Remove null bytes
    sanitized.erase(std::remove(sanitized.begin(), sanitized.end(), '\0'), sanitized.end());

    // Truncate if too long (safety measure)
    if (sanitized.length() > MAX_QUERY_LENGTH) {
        sanitized = sanitized.substr(0, MAX_QUERY_LENGTH);
    }

    return sanitized;
}

bool FunctionSecurity::containsDangerousPatterns(const std::string& input) {
    for (const auto& pattern : getDangerousPatterns()) {
        if (std::regex_search(input, pattern)) {
            return true;
        }
    }
    return false;
}

bool FunctionSecurity::isValidFunctionNameChar(char c) {
    // Only allow alphanumeric characters, underscore, and hyphen
    // Explicitly reject spaces and other special characters
    return std::isalnum(c) || c == '_' || c == '-';
}

bool FunctionSecurity::containsPathTraversal(const std::string& input) {
    if (input.find("../") != std::string::npos || input.find("..\\") != std::string::npos) {
        return true;
    }
    // URL-encoded checks must be case-insensitive since hex digits
    // can be upper or lowercase (e.g., %2e vs %2E).
    std::string lower = input;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    return lower.find("%2e%2e%2f") != std::string::npos || lower.find("%2e%2e%5c") != std::string::npos;
}

bool FunctionSecurity::containsNullBytes(const std::string& input) {
    return input.find('\0') != std::string::npos;
}

bool FunctionSecurity::containsControlCharacters(const std::string& input) {
    for (unsigned char c : input) {
        if (c < 32 && c != '\t' && c != '\n' && c != '\r') {
            return true;
        }
    }
    return false;
}

std::string FunctionSecurity::removeControlCharacters(const std::string& input) {
    std::string result;
    result.reserve(input.length());

    for (unsigned char c : input) {
        if (c >= 32 || c == '\t' || c == '\n' || c == '\r') {
            result += c;
        } else {
            result += ' ';  // Replace control characters with spaces
        }
    }

    return result;
}

bool FunctionSecurity::exceedsMaxLength(const std::string& input, size_t maxLength) {
    return input.length() > maxLength;
}

}  // namespace timestar::functions
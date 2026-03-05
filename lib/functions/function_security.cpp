#include "function_security.hpp"
#include <sstream>
#include <iomanip>

namespace tsdb::functions {

// Static member definitions
const std::unordered_set<std::string> FunctionSecurity::dangerousFunctionNames_ = {
    // System commands
    "system", "exec", "eval", "shell", "bash", "sh", "cmd", "powershell",
    "shell_exec", "passthru", "popen", "proc_open",
    
    // File operations
    "file", "fopen", "fread", "fwrite", "readfile", "file_get_contents",
    "include", "include_once", "require", "require_once",
    
    // Network operations
    "curl", "wget", "http", "ftp", "socket", "fsockopen",
    
    // Code execution
    "__import__", "__builtins__", "getattr", "setattr", "delattr",
    "compile", "execfile", "reload", "input", "raw_input",
    
    // Administrative functions
    "admin", "root", "sudo", "su", "chmod", "chown", "kill",
    
    // Database operations
    "drop", "delete", "truncate", "alter", "create", "insert", "update",
    "union", "select", "where", "from", "join", "having", "order",
    
    // Special characters and protocols
    "javascript", "vbscript", "data", "file", "ftp", "http", "https",
    
    // Path-related
    "cd", "chdir", "pwd", "ls", "dir", "find", "locate",
    
    // Process control
    "fork", "spawn", "thread", "process", "exit", "abort"
};

const std::vector<std::regex> FunctionSecurity::dangerousPatterns_ = {
    // Code injection patterns
    std::regex(R"(__import__\s*\()", std::regex_constants::icase),
    std::regex(R"(exec\s*\()", std::regex_constants::icase),
    std::regex(R"(eval\s*\()", std::regex_constants::icase),
    std::regex(R"(system\s*\()", std::regex_constants::icase),
    std::regex(R"(shell_exec\s*\()", std::regex_constants::icase),
    
    // Command injection patterns
    std::regex(R"([;&|`$(){}[\]])", std::regex_constants::icase),
    std::regex(R"(\\x[0-9a-f]{2})", std::regex_constants::icase),
    
    // Command-like patterns (spaces between command arguments)
    std::regex(R"(rm\s+)", std::regex_constants::icase),
    std::regex(R"(sudo\s+)", std::regex_constants::icase),
    std::regex(R"(chmod\s+)", std::regex_constants::icase),
    std::regex(R"(kill\s+)", std::regex_constants::icase),
    std::regex(R"(ps\s+)", std::regex_constants::icase),
    std::regex(R"(ls\s+)", std::regex_constants::icase),
    std::regex(R"(cat\s+)", std::regex_constants::icase),
    std::regex(R"(curl\s+)", std::regex_constants::icase),
    std::regex(R"(wget\s+)", std::regex_constants::icase),
    std::regex(R"(nc\s+)", std::regex_constants::icase),
    
    // Path traversal patterns
    std::regex(R"(\.\./|\.\.\\)", std::regex_constants::icase),
    std::regex(R"(%2e%2e%2f|%2e%2e%5c)", std::regex_constants::icase),
    std::regex(R"(\\\.\\\.\\|\\\.\\\.\/)", std::regex_constants::icase),
    
    // SQL injection patterns
    std::regex(R"('\s*or\s*')", std::regex_constants::icase),
    std::regex(R"('\s*union\s*select)", std::regex_constants::icase),
    std::regex(R"(drop\s+table)", std::regex_constants::icase),
    std::regex(R"(delete\s+from)", std::regex_constants::icase),
    std::regex(R"(insert\s+into)", std::regex_constants::icase),
    
    // XSS patterns
    // NOTE: <script[^>]*> uses [^>]* (negated class) which cannot backtrack catastrophically.
    std::regex(R"(<script[^>]*>)", std::regex_constants::icase),
    std::regex(R"(javascript\s*:)", std::regex_constants::icase),
    // NOTE: on\w+\s*= was vulnerable to ReDoS: with input "onaonaonaona..." (n chars),
    // the unbounded \w+ caused O(n^2) backtracking per position, totalling O(n^3).
    // Fix: bound \w to {1,30} — covers all real HTML event attributes (longest is
    // onsecuritypolicyviolation at 23 chars) while eliminating catastrophic backtracking.
    std::regex(R"(on\w{1,30}\s*=)", std::regex_constants::icase),
    
    // Format string attacks
    std::regex(R"(%[0-9]*[sdxn])", std::regex_constants::icase),
    
    // LDAP injection
    std::regex(R"(\*\)\([\w=*]+\)\(\|)", std::regex_constants::icase),
    
    // Protocol injection
    std::regex(R"(file:\/\/|ftp:\/\/|ldap:\/\/)", std::regex_constants::icase),
};

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
        result.errorMessage = "Function name exceeds maximum length of " + std::to_string(MAX_FUNCTION_NAME_LENGTH) + " characters";
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
        result.errorMessage = "Parameters exceed maximum length of " + std::to_string(MAX_PARAMETER_LENGTH) + " characters";
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
                case '{': openBraces++; break;
                case '}': closeBraces++; break;
                case '[': openBrackets++; break;
                case ']': closeBrackets++; break;
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
    for (const auto& pattern : dangerousPatterns_) {
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
    return input.find("../") != std::string::npos ||
           input.find("..\\") != std::string::npos ||
           input.find("%2e%2e%2f") != std::string::npos ||
           input.find("%2e%2e%5c") != std::string::npos;
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
            result += ' '; // Replace control characters with spaces
        }
    }
    
    return result;
}

bool FunctionSecurity::exceedsMaxLength(const std::string& input, size_t maxLength) {
    return input.length() > maxLength;
}

} // namespace tsdb::functions
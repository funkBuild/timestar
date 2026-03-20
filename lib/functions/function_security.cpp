#include "function_security.hpp"

#include <iomanip>
#include <sstream>
#include <string_view>

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
    "javascript", "vbscript", "data", "https",

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
    std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(),
                   [](unsigned char c) { return std::tolower(c); });

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
    result.sanitizedInput = sanitizeInput(json, MAX_JSON_LENGTH);
    return result;
}

std::string FunctionSecurity::sanitizeInput(const std::string& input, size_t maxLength) {
    std::string sanitized = removeControlCharacters(input);

    // Remove null bytes
    sanitized.erase(std::remove(sanitized.begin(), sanitized.end(), '\0'), sanitized.end());

    // Truncate if too long (safety measure)
    if (sanitized.length() > maxLength) {
        sanitized = sanitized.substr(0, maxLength);
    }

    return sanitized;
}

bool FunctionSecurity::containsDangerousPatterns(const std::string& input) {
    // Pre-lowercase once (all patterns are case-insensitive)
    std::string lower;
    lower.resize(input.size());
    for (size_t i = 0; i < input.size(); ++i) {
        lower[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(input[i])));
    }

    // Shell metacharacters
    if (lower.find_first_of(";&|`$") != std::string::npos)
        return true;

    // Code injection: keyword at word boundary + optional whitespace + '('
    // Require word boundary to avoid matching "execute(", "evaluate(", "subsystem("
    static constexpr std::string_view parenKw[] = {"__import__", "exec", "eval", "system", "shell_exec"};
    for (auto kw : parenKw) {
        size_t pos = 0;
        while ((pos = lower.find(kw, pos)) != std::string::npos) {
            bool atWordBoundary = (pos == 0 || !std::isalnum(static_cast<unsigned char>(lower[pos - 1])));
            if (atWordBoundary) {
                size_t p = pos + kw.size();
                while (p < lower.size() &&
                       (lower[p] == ' ' || lower[p] == '\t' || lower[p] == '\n' || lower[p] == '\r'))
                    ++p;
                if (p < lower.size() && lower[p] == '(')
                    return true;
            }
            ++pos;
        }
    }

    // Command injection: keyword at word boundary + at least one whitespace.
    // Require word boundary before the keyword to avoid matching "firmware" for "rm",
    // "impulse" for "ls", "once" for "nc", etc.
    static constexpr std::string_view cmdKw[] = {"rm", "sudo", "chmod", "kill", "cat", "curl", "wget"};
    for (auto kw : cmdKw) {
        size_t pos = 0;
        while ((pos = lower.find(kw, pos)) != std::string::npos) {
            // Check word boundary before keyword (start of string or non-alnum)
            bool atWordBoundary = (pos == 0 || !std::isalnum(static_cast<unsigned char>(lower[pos - 1])));
            size_t p = pos + kw.size();
            if (atWordBoundary && p < lower.size() &&
                (lower[p] == ' ' || lower[p] == '\t' || lower[p] == '\n' || lower[p] == '\r'))
                return true;
            ++pos;
        }
    }

    // Path traversal
    if (lower.find("../") != std::string::npos || lower.find("..\\") != std::string::npos)
        return true;
    if (lower.find("%2e%2e%2f") != std::string::npos || lower.find("%2e%2e%5c") != std::string::npos)
        return true;
    if (lower.find("\\.\\.\\") != std::string::npos || lower.find("\\.\\./") != std::string::npos)
        return true;

    // SQL injection: ' or '
    {
        size_t pos = 0;
        while ((pos = lower.find('\'', pos)) != std::string::npos) {
            size_t p = pos + 1;
            while (p < lower.size() && (lower[p] == ' ' || lower[p] == '\t'))
                ++p;
            if (p + 2 <= lower.size() && lower[p] == 'o' && lower[p + 1] == 'r') {
                size_t q = p + 2;
                while (q < lower.size() && (lower[q] == ' ' || lower[q] == '\t'))
                    ++q;
                if (q < lower.size() && lower[q] == '\'')
                    return true;
            }
            ++pos;
        }
    }
    // ' union select
    {
        size_t pos = 0;
        while ((pos = lower.find('\'', pos)) != std::string::npos) {
            size_t p = pos + 1;
            while (p < lower.size() && (lower[p] == ' ' || lower[p] == '\t'))
                ++p;
            if (lower.compare(p, 5, "union") == 0) {
                size_t q = p + 5;
                while (q < lower.size() && (lower[q] == ' ' || lower[q] == '\t'))
                    ++q;
                if (lower.compare(q, 6, "select") == 0)
                    return true;
            }
            ++pos;
        }
    }
    // Two-word SQL: drop table, delete from, insert into
    static constexpr struct {
        std::string_view kw1;
        std::string_view kw2;
    } sqlPairs[] = {{"drop", "table"}, {"delete", "from"}, {"insert", "into"}};
    for (const auto& [kw1, kw2] : sqlPairs) {
        size_t pos = 0;
        while ((pos = lower.find(kw1, pos)) != std::string::npos) {
            bool atWordBoundary = (pos == 0 || !std::isalnum(static_cast<unsigned char>(lower[pos - 1])));
            size_t p = pos + kw1.size();
            if (atWordBoundary && p < lower.size() &&
                (lower[p] == ' ' || lower[p] == '\t' || lower[p] == '\n' || lower[p] == '\r')) {
                while (p < lower.size() &&
                       (lower[p] == ' ' || lower[p] == '\t' || lower[p] == '\n' || lower[p] == '\r'))
                    ++p;
                if (lower.compare(p, kw2.size(), kw2) == 0)
                    return true;
            }
            ++pos;
        }
    }

    // XSS: <script...>
    {
        size_t pos = 0;
        while ((pos = lower.find("<script", pos)) != std::string::npos) {
            size_t p = pos + 7;
            while (p < lower.size() && lower[p] != '>')
                ++p;
            if (p < lower.size())
                return true;
            ++pos;
        }
    }
    // javascript:
    {
        size_t pos = 0;
        while ((pos = lower.find("javascript", pos)) != std::string::npos) {
            size_t p = pos + 10;
            while (p < lower.size() && (lower[p] == ' ' || lower[p] == '\t'))
                ++p;
            if (p < lower.size() && lower[p] == ':')
                return true;
            ++pos;
        }
    }
    // on\w{1,30}\s*= (HTML event handler attributes like onclick=, onload=)
    // Require word boundary before "on" to avoid matching "condition=", "conversion=", etc.
    {
        size_t pos = 0;
        while ((pos = lower.find("on", pos)) != std::string::npos) {
            bool atWordBoundary = (pos == 0 || !std::isalnum(static_cast<unsigned char>(lower[pos - 1])));
            if (atWordBoundary) {
                size_t p = pos + 2;
                size_t wc = 0;
                while (p < lower.size() && (std::isalnum(static_cast<unsigned char>(lower[p])) || lower[p] == '_')) {
                    ++p;
                    ++wc;
                    if (wc > 30)
                        break;
                }
                if (wc >= 1 && wc <= 30) {
                    while (p < lower.size() && (lower[p] == ' ' || lower[p] == '\t'))
                        ++p;
                    if (p < lower.size() && lower[p] == '=')
                        return true;
                }
            }
            ++pos;
        }
    }

    // Hex escape: \x followed by 2 hex digits
    {
        size_t pos = 0;
        while ((pos = lower.find("\\x", pos)) != std::string::npos) {
            if (pos + 3 < lower.size() && std::isxdigit(static_cast<unsigned char>(lower[pos + 2])) &&
                std::isxdigit(static_cast<unsigned char>(lower[pos + 3])))
                return true;
            ++pos;
        }
    }

    // Format string: %n (dangerous write primitive) or %<2+ digits><s|d|x|n>
    // Skip percent-encoded values like %0A, %2F which are legitimate URL encoding.
    {
        size_t pos = 0;
        while ((pos = lower.find('%', pos)) != std::string::npos) {
            // %n is dangerous only when not followed by a hex digit (to skip %0a-style URL encoding)
            if (pos + 1 < lower.size() && lower[pos + 1] == 'n' &&
                (pos + 2 >= lower.size() || !std::isalnum(static_cast<unsigned char>(lower[pos + 2]))))
                return true;
            size_t p = pos + 1;
            size_t dc = 0;
            while (p < lower.size() && lower[p] >= '0' && lower[p] <= '9') {
                ++p;
                ++dc;
            }
            if (dc >= 2 && p < lower.size() &&
                (lower[p] == 's' || lower[p] == 'd' || lower[p] == 'x' || lower[p] == 'n'))
                return true;
            ++pos;
        }
    }

    // LDAP injection: *)([\w=*]+)(|
    {
        size_t pos = 0;
        while ((pos = lower.find("*)(", pos)) != std::string::npos) {
            size_t p = pos + 3;
            size_t cc = 0;
            while (p < lower.size() && (std::isalnum(static_cast<unsigned char>(lower[p])) || lower[p] == '_' ||
                                        lower[p] == '=' || lower[p] == '*')) {
                ++p;
                ++cc;
            }
            if (cc > 0 && p + 2 < lower.size() && lower[p] == ')' && lower[p + 1] == '(' && lower[p + 2] == '|')
                return true;
            ++pos;
        }
    }

    // Protocol injection
    if (lower.find("file://") != std::string::npos || lower.find("ftp://") != std::string::npos ||
        lower.find("ldap://") != std::string::npos)
        return true;

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
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
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
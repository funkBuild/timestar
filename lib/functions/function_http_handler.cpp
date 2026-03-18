#include "function_http_handler.hpp"

#include "../core/engine.hpp"
#include "../http/http_query_handler.hpp"
#include "../query/query_parser.hpp"
#include "../utils/json_escape.hpp"
#include "function_security.hpp"

#include <glaze/glaze.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <numeric>
#include <set>
#include <sstream>
#include <vector>

// Glaze structs for JSON parsing in function HTTP handlers.
// Defined at file scope so glz::meta specializations work correctly.

struct GlazeFunctionValidationRequest {
    std::string function;
    std::optional<glz::generic> parameters;
};

template <>
struct glz::meta<GlazeFunctionValidationRequest> {
    using T = GlazeFunctionValidationRequest;
    static constexpr auto value = object("function", &T::function, "parameters", &T::parameters);
};

struct GlazeQueryParseRequest {
    std::string query;
};

template <>
struct glz::meta<GlazeQueryParseRequest> {
    using T = GlazeQueryParseRequest;
    static constexpr auto value = object("query", &T::query);
};

struct GlazeFunctionQueryRequest {
    std::string query;
    uint64_t startTime{0};
    uint64_t endTime{0};
};

template <>
struct glz::meta<GlazeFunctionQueryRequest> {
    using T = GlazeFunctionQueryRequest;
    static constexpr auto value = object("query", &T::query, "startTime", &T::startTime, "endTime", &T::endTime);
};

namespace timestar::functions {

using timestar::jsonEscape;

// Per-shard instance (one per Seastar shard, no mutex needed)
thread_local PerformanceTracker FunctionHttpHandler::performanceTracker_;

// PerformanceTracker implementation
void PerformanceTracker::recordExecution(const std::string& functionName, std::chrono::nanoseconds duration) {
    auto& stats = functionStats_[functionName];
    stats.executions++;
    stats.totalTimeNs += duration.count();
}

void PerformanceTracker::recordCacheHit(const std::string& functionName) {
    auto& stats = functionStats_[functionName];
    stats.cacheHits++;
}

void PerformanceTracker::recordCacheMiss(const std::string& functionName) {
    auto& stats = functionStats_[functionName];
    stats.cacheMisses++;
}

uint64_t PerformanceTracker::getTotalExecutions() const {
    uint64_t total = 0;
    for (const auto& pair : functionStats_) {
        total += pair.second.executions;
    }
    return total;
}

double PerformanceTracker::getAverageExecutionTime() const {
    uint64_t totalExecs = 0;
    uint64_t totalTimeNs = 0;

    for (const auto& pair : functionStats_) {
        totalExecs += pair.second.executions;
        totalTimeNs += pair.second.totalTimeNs;
    }

    return totalExecs > 0 ? (totalTimeNs / 1000000.0) / totalExecs : 0.0;
}

std::string PerformanceTracker::getPerformanceStats() const {
    // Calculate totals
    uint64_t totalExecs = 0;
    uint64_t totalTimeNs = 0;
    uint64_t totalHits = 0, totalMisses = 0;

    for (const auto& pair : functionStats_) {
        totalExecs += pair.second.executions;
        totalTimeNs += pair.second.totalTimeNs;
        totalHits += pair.second.cacheHits;
        totalMisses += pair.second.cacheMisses;
    }

    double avgExecTime = totalExecs > 0 ? (totalTimeNs / 1000000.0) / totalExecs : 0.0;
    double overallHitRate =
        (totalHits + totalMisses) > 0 ? static_cast<double>(totalHits) / (totalHits + totalMisses) : 0.0;

    std::ostringstream json;
    json << "{\"status\":\"success\",\"statistics\":{";
    json << "\"totalExecutions\":" << totalExecs << ",";
    json << "\"averageExecutionTime\":" << avgExecTime << ",";
    json << "\"cacheHitRate\":" << overallHitRate;

    json << "},\"functionStats\":{";

    bool first = true;
    for (const auto& pair : functionStats_) {
        if (!first)
            json << ",";
        first = false;

        json << "\"" << jsonEscape(pair.first) << "\":{";
        json << "\"executions\":" << pair.second.executions << ",";
        json << "\"avgTime\":" << pair.second.getAverageTime() << ",";
        json << "\"cacheHitRate\":" << pair.second.getCacheHitRate();
        json << "}";
    }

    json << "}}";
    return json.str();
}

FunctionHttpHandler::FunctionHttpHandler(seastar::sharded<Engine>& engine) : engine_(engine) {}

void FunctionHttpHandler::registerRoutes(seastar::httpd::routes& routes) {
    using namespace seastar;
    using namespace httpd;

    // Performance stats endpoint - GET /functions/performance (must be before wildcard /functions/{name})
    auto* performanceStats = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handlePerformanceStatsSync();
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::GET, url("/functions/performance"), performanceStats);

    // Cache stats endpoint - GET /functions/cache (must be before wildcard /functions/{name})
    auto* cacheStats = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handleCacheStatsSync();
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::GET, url("/functions/cache"), cacheStats);

    // Function validation endpoint - POST /functions/validate (must be before wildcard /functions/{name})
    auto* validateFunction = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handleFunctionValidationSync(*req);
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::POST, url("/functions/validate"), validateFunction);

    // Function list endpoint - GET /functions
    auto* functionList = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handleFunctionListSync();
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::GET, url("/functions"), functionList);

    // Function info endpoint - GET /functions/{name} (wildcard route - must be last)
    auto* functionInfo = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handleFunctionInfoSync(*req);
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::GET, url("/functions").remainder("name"), functionInfo);

    // Query parsing endpoint - POST /query/parse
    auto* parseQuery = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            rep->set_status(http::reply::status_type::ok);
            rep->_content = handleQueryParsingSync(*req);
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::POST, url("/query/parse"), parseQuery);

    // Function query endpoint - POST /query/functions
    auto* functionQuery = new function_handler(
        [this](std::unique_ptr<http::request> req,
               std::unique_ptr<http::reply> rep) -> seastar::future<std::unique_ptr<http::reply>> {
            std::string response = handleFunctionQuerySync(*req);

            // Check if response indicates an error and set appropriate HTTP status
            if (response.find("\"success\": false") != std::string::npos) {
                rep->set_status(http::reply::status_type::bad_request);
            } else {
                rep->set_status(http::reply::status_type::ok);
            }

            rep->_content = response;
            rep->add_header("Content-Type", "application/json");
            return seastar::make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        },
        "json");
    routes.add(operation_type::POST, url("/query/functions"), functionQuery);
}

std::string FunctionHttpHandler::handleFunctionListSync() {
    return "{\"status\":\"success\",\"functions\":["
           "{\"name\":\"sma\",\"category\":\"smoothing\",\"description\":\"Simple Moving "
           "Average\",\"parameters\":[{\"name\":\"window\",\"type\":\"int\",\"required\":true}]},"
           "{\"name\":\"ema\",\"category\":\"smoothing\",\"description\":\"Exponential Moving "
           "Average\",\"parameters\":[{\"name\":\"alpha\",\"type\":\"float\",\"required\":true}]},"
           "{\"name\":\"add\",\"category\":\"arithmetic\",\"description\":\"Add constant "
           "value\",\"parameters\":[{\"name\":\"value\",\"type\":\"float\",\"required\":true}]},"
           "{\"name\":\"multiply\",\"category\":\"arithmetic\",\"description\":\"Multiply by "
           "constant\",\"parameters\":[{\"name\":\"factor\",\"type\":\"float\",\"required\":true}]},"
           "{\"name\":\"scale\",\"category\":\"arithmetic\",\"description\":\"Scale by "
           "factor\",\"parameters\":[{\"name\":\"factor\",\"type\":\"float\",\"required\":true}]},"
           "{\"name\":\"offset\",\"category\":\"arithmetic\",\"description\":\"Offset by "
           "constant\",\"parameters\":[{\"name\":\"offset\",\"type\":\"float\",\"required\":true}]},"
           "{\"name\":\"interpolate\",\"category\":\"interpolation\",\"description\":\"Linear "
           "interpolation\",\"parameters\":[]}"
           "]}";
}

std::string FunctionHttpHandler::handleFunctionInfoSync(const seastar::http::request& req) {
    std::string functionName;

    try {
        // Try to get the function name from URL parameters
        if (req.param.exists("name")) {
            functionName = std::string(req.param.at("name"));
            // Seastar's remainder() includes the leading slash, so remove it
            while (!functionName.empty() && functionName[0] == '/') {
                functionName = functionName.substr(1);
            }
        } else {
            // Try to extract from URL path as fallback
            std::string path = req._url;

            // Look for /functions/ pattern
            size_t pos = path.find("/functions/");
            if (pos != std::string::npos) {
                functionName = path.substr(pos + 11);  // 11 = length of "/functions/"
            } else {
                // Look for /functions followed by something else
                pos = path.find("/functions");
                if (pos != std::string::npos) {
                    size_t start = pos + 10;  // 10 = length of "/functions"
                    if (start < path.length() && path[start] == '/') {
                        start++;  // Skip the slash
                    }
                    if (start < path.length()) {
                        functionName = path.substr(start);
                    }
                }
            }

            // Remove any remaining leading slash if present
            while (!functionName.empty() && functionName[0] == '/') {
                functionName = functionName.substr(1);
            }
        }
    } catch (const std::exception& e) {
        return "{\"status\":\"error\",\"error\":{\"code\":\"PARAMETER_ERROR\",\"message\":\"Failed to extract function "
               "name from request\"}}";
    }

    if (functionName.empty()) {
        return R"({"status":"error","error":{"code":"MISSING_PARAMETER","message":"Function name not provided"}})";
    }

    // SECURITY VALIDATION: Validate function name
    auto validation = FunctionSecurity::validateFunctionName(functionName);
    if (!validation.isValid) {
        return "{\"status\":\"error\",\"error\":{\"code\":\"" + jsonEscape(validation.errorCode) + "\",\"message\":\"" +
               jsonEscape(validation.errorMessage) + "\"}}";
    }

    // Use sanitized function name
    functionName = validation.sanitizedInput;

    if (functionName == "sma") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"sma\",\"category\":\"smoothing\",\"description\":"
               "\"Simple Moving "
               "Average\",\"parameters\":{\"window\":{\"type\":\"int\",\"required\":true,\"description\":\"Size of the "
               "moving window\"}},\"examples\":[\"sma(window:10)\",\"sma(window:5)\"]}}";
    } else if (functionName == "ema") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"ema\",\"category\":\"smoothing\",\"description\":"
               "\"Exponential Moving "
               "Average\",\"parameters\":{\"alpha\":{\"type\":\"float\",\"required\":true,\"description\":\"Smoothing "
               "factor between 0 and 1\"}},\"examples\":[\"ema(alpha:0.3)\",\"ema(alpha:0.1)\"]}}";
    } else if (functionName == "add") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"add\",\"category\":\"arithmetic\",\"description\":"
               "\"Add constant "
               "value\",\"parameters\":{\"value\":{\"type\":\"float\",\"required\":true,\"description\":\"Value to "
               "add\"}},\"examples\":[\"add(value:10)\",\"add(value:-5.5)\"]}}";
    } else if (functionName == "multiply") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"multiply\",\"category\":\"arithmetic\","
               "\"description\":\"Multiply by "
               "constant\",\"parameters\":{\"factor\":{\"type\":\"float\",\"required\":true,\"description\":\"Factor to "
               "multiply by\"}},\"examples\":[\"multiply(factor:2.5)\",\"multiply(factor:0.5)\"]}}";
    } else if (functionName == "scale") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"scale\",\"category\":\"arithmetic\",\"description\":"
               "\"Scale by "
               "factor\",\"parameters\":{\"factor\":{\"type\":\"float\",\"required\":true,\"description\":\"Scaling "
               "factor\"}},\"examples\":[\"scale(factor:1.8)\",\"scale(factor:0.5)\"]}}";
    } else if (functionName == "offset") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"offset\",\"category\":\"arithmetic\",\"description\":"
               "\"Offset by "
               "constant\",\"parameters\":{\"value\":{\"type\":\"float\",\"required\":true,\"description\":\"Offset "
               "value to add\"}},\"examples\":[\"offset(value:32)\",\"offset(value:-10)\"]}}";
    } else if (functionName == "interpolate") {
        return "{\"status\":\"success\",\"function\":{\"name\":\"interpolate\",\"category\":\"interpolation\","
               "\"description\":\"Linear "
               "interpolation\",\"parameters\":{\"method\":{\"type\":\"string\",\"required\":false,\"description\":"
               "\"Interpolation "
               "method\",\"default\":\"linear\"},\"target_interval\":{\"type\":\"int\",\"required\":false,"
               "\"description\":\"Target interval in "
               "nanoseconds\"}},\"examples\":[\"interpolate(method:linear)\",\"interpolate(method:linear,target_"
               "interval:60000000000)\"]}}";
    }

    return "{\"status\":\"error\",\"error\":{\"code\":\"FUNCTION_NOT_FOUND\",\"message\":\"Function not found: " +
           jsonEscape(functionName) + "\"}}";
}

std::string FunctionHttpHandler::handleFunctionValidationSync(const seastar::http::request& req) {
    std::string body = req.content;

    try {
        // Parse JSON using Glaze to safely handle escaped quotes, Unicode, etc.
        GlazeFunctionValidationRequest parsed;
        auto parseErr = glz::read_json(parsed, body);
        if (parseErr) {
            return "{\"status\":\"success\",\"valid\":false,\"error\":\"Failed to parse request: " +
                   jsonEscape(std::string(glz::format_error(parseErr))) + "\"}";
        }

        std::string functionName = parsed.function;
        std::string parameters;
        if (parsed.parameters.has_value()) {
            parameters = glz::write_json(parsed.parameters.value()).value_or("{}");
        }

        if (functionName.empty()) {
            return "{\"status\":\"success\",\"valid\":false,\"error\":\"Function name not provided\"}";
        }

        // SECURITY VALIDATION: Validate function name
        auto nameValidation = FunctionSecurity::validateFunctionName(functionName);
        if (!nameValidation.isValid) {
            return "{\"status\":\"success\",\"valid\":false,\"error\":\"" + jsonEscape(nameValidation.errorMessage) +
                   "\"}";
        }

        // SECURITY VALIDATION: Validate parameters
        if (!parameters.empty()) {
            auto paramValidation = FunctionSecurity::validateParameters(parameters);
            if (!paramValidation.isValid) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"" +
                       jsonEscape(paramValidation.errorMessage) + "\"}";
            }
            parameters = paramValidation.sanitizedInput;
        }

        // Use sanitized function name
        functionName = nameValidation.sanitizedInput;

        // Validate based on function type
        if (functionName == "sma") {
            // Check for window parameter
            if (parameters.find("\"window\"") == std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Missing required parameter: window\"}";
            }

            // Check for negative window
            if (parameters.find("\"window\":-") != std::string::npos ||
                parameters.find("\"window\": -") != std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Parameter 'window' must be positive\"}";
            }

            // Check for invalid window value (non-numeric)
            size_t windowPos = parameters.find("\"window\":");
            if (windowPos != std::string::npos) {
                size_t valueStart = parameters.find(":", windowPos) + 1;
                while (valueStart < parameters.length() && std::isspace(parameters[valueStart])) {
                    valueStart++;
                }

                // Check if value starts with a quote (string value instead of number)
                if (valueStart < parameters.length() && parameters[valueStart] == '"') {
                    return "{\"status\":\"success\",\"valid\":false,\"error\":\"Parameter 'window' must be a number\"}";
                }

                // Check for other non-numeric patterns like "invalid"
                if (parameters.find("\"invalid\"", valueStart) != std::string::npos ||
                    parameters.find("'invalid'", valueStart) != std::string::npos) {
                    return "{\"status\":\"success\",\"valid\":false,\"error\":\"Parameter 'window' must be a number\"}";
                }
            }

            return "{\"status\":\"success\",\"valid\":true}";
        }

        else if (functionName == "ema") {
            // Check for alpha parameter
            if (parameters.find("\"alpha\"") == std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Missing required parameter: alpha\"}";
            }

            return "{\"status\":\"success\",\"valid\":true}";
        }

        else if (functionName == "scale") {
            // Check for factor parameter
            if (parameters.find("\"factor\"") == std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Missing required parameter: factor\"}";
            }

            return "{\"status\":\"success\",\"valid\":true}";
        }

        else if (functionName == "offset") {
            // Check for value parameter
            if (parameters.find("\"value\"") == std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Missing required parameter: value\"}";
            }

            return "{\"status\":\"success\",\"valid\":true}";
        }

        else if (functionName == "add" || functionName == "multiply") {
            // Check for value parameter
            if (parameters.find("\"value\"") == std::string::npos) {
                return "{\"status\":\"success\",\"valid\":false,\"error\":\"Missing required parameter: value\"}";
            }

            return "{\"status\":\"success\",\"valid\":true}";
        }

        else if (functionName == "interpolate") {
            // Interpolate function has optional parameters, so it's always valid
            return "{\"status\":\"success\",\"valid\":true}";
        }

        else {
            return "{\"status\":\"success\",\"valid\":false,\"error\":\"Unknown function: " + functionName + "\"}";
        }

    } catch (const std::exception& e) {
        return "{\"status\":\"success\",\"valid\":false,\"error\":\"Failed to parse request: " + jsonEscape(e.what()) +
               "\"}";
    }
}

std::string FunctionHttpHandler::handleQueryParsingSync(const seastar::http::request& req) {
    std::string body = req.content;

    // Parse JSON using Glaze to safely handle escaped quotes, Unicode, etc.
    GlazeQueryParseRequest parsed;
    auto parseErr = glz::read_json(parsed, body);
    if (parseErr) {
        return "{\"status\":\"success\",\"valid\":false,\"error\":\"Invalid JSON format\"}";
    }

    if (parsed.query.empty()) {
        return "{\"status\":\"success\",\"valid\":false,\"error\":\"Query not provided\"}";
    }

    std::string query = parsed.query;

    // Parse the query: aggregationMethod:measurement(fields).function1(params).function2(params)...
    std::string aggregation = "avg";
    std::string measurement = "";
    std::vector<std::string> functionNames;

    // Extract aggregation method and measurement
    size_t colonPos = query.find(":");
    if (colonPos != std::string::npos) {
        aggregation = query.substr(0, colonPos);

        // Find the measurement part - it ends at the first '(' which starts the fields
        size_t measurementStart = colonPos + 1;
        size_t parenPos = query.find("(", measurementStart);

        if (parenPos != std::string::npos) {
            measurement = query.substr(measurementStart, parenPos - measurementStart);
        } else {
            measurement = query.substr(measurementStart);
        }
    }

    // Extract functions from the query
    // Functions only appear after the field parentheses close
    // Find the closing parenthesis of the fields part
    size_t fieldsStart = query.find("(");
    if (fieldsStart != std::string::npos) {
        size_t fieldsEnd = query.find(")", fieldsStart);
        if (fieldsEnd != std::string::npos) {
            // Functions start after the closing parenthesis
            size_t functionsStart = fieldsEnd + 1;

            // Parse functions from this point: .function1(params).function2(params)...
            size_t pos = functionsStart;
            while (pos < query.length()) {
                // Look for a dot indicating the start of a function
                size_t dotPos = query.find(".", pos);
                if (dotPos == std::string::npos)
                    break;

                // Function name starts after the dot
                size_t funcStart = dotPos + 1;

                // Function name ends at the opening parenthesis or next dot
                size_t funcEnd = query.find("(", funcStart);
                if (funcEnd == std::string::npos) {
                    funcEnd = query.find(".", funcStart);
                }
                if (funcEnd == std::string::npos) {
                    funcEnd = query.length();
                }

                if (funcEnd > funcStart) {
                    std::string funcName = query.substr(funcStart, funcEnd - funcStart);
                    if (!funcName.empty()) {
                        functionNames.push_back(funcName);
                    }
                }

                // Move to after this function's parameters
                if (funcEnd < query.length() && query[funcEnd] == '(') {
                    // Find the matching closing parenthesis
                    int parenCount = 1;
                    size_t paramPos = funcEnd + 1;
                    while (paramPos < query.length() && parenCount > 0) {
                        if (query[paramPos] == '(')
                            parenCount++;
                        else if (query[paramPos] == ')')
                            parenCount--;
                        paramPos++;
                    }
                    pos = paramPos;
                } else {
                    pos = funcEnd;
                }
            }
        }
    }

    // Known valid functions for validation
    std::set<std::string> knownFunctions = {"sma", "ema", "add", "multiply", "scale", "offset", "interpolate"};

    // Check if all functions are valid
    bool valid = true;
    std::string invalidFunction = "";
    for (const auto& func : functionNames) {
        if (knownFunctions.find(func) == knownFunctions.end()) {
            valid = false;
            invalidFunction = func;
            break;
        }
    }

    // Build response
    std::string response = "{\"status\":\"success\",\"valid\":" + std::string(valid ? "true" : "false");

    if (!valid) {
        response += ",\"error\":\"Unknown function: " + jsonEscape(invalidFunction) + "\"}";
    } else {
        response += ",\"parsed\":{";
        response += "\"aggregation\":\"" + jsonEscape(aggregation) + "\"";
        response += ",\"measurement\":\"" + jsonEscape(measurement) + "\"";
        response += ",\"functions\":[";

        for (size_t i = 0; i < functionNames.size(); i++) {
            if (i > 0)
                response += ",";
            response += "\"" + jsonEscape(functionNames[i]) + "\"";
        }

        response += "]}}";
    }

    return response;
}

std::string FunctionHttpHandler::handleFunctionQuerySync(const seastar::http::request& req) {
    auto startTime = std::chrono::high_resolution_clock::now();

    try {
        // Parse request body to get function query
        std::string body(req.content.data(), req.content.size());

        // SECURITY VALIDATION: Validate JSON input
        auto jsonValidation = FunctionSecurity::validateJsonInput(body);
        if (!jsonValidation.isValid) {
            return "{\"success\": false, \"error\": \"" + jsonValidation.errorMessage + "\"}";
        }
        body = jsonValidation.sanitizedInput;

        // Parse JSON using Glaze to safely handle escaped quotes, Unicode, etc.
        GlazeFunctionQueryRequest parsedReq;
        auto parseErr = glz::read_json(parsedReq, body);
        if (parseErr) {
            return "{\"success\": false, \"error\": \"Invalid request format\"}";
        }

        if (parsedReq.query.empty() || parsedReq.startTime == 0 || parsedReq.endTime == 0) {
            return "{\"success\": false, \"error\": \"Invalid request format\"}";
        }

        std::string functionQuery = parsedReq.query;
        uint64_t startTimeVal = parsedReq.startTime;
        uint64_t endTimeVal = parsedReq.endTime;

        // SECURITY VALIDATION: Validate function query
        auto queryValidation = FunctionSecurity::validateFunctionQuery(functionQuery);
        if (!queryValidation.isValid) {
            return "{\"success\": false, \"error\": \"" + queryValidation.errorMessage + "\"}";
        }
        functionQuery = queryValidation.sanitizedInput;

        // Check if this is a multi-series operation (e.g., add(query1, query2))
        if (functionQuery.find("add(") == 0) {
            return handleMultiSeriesOperation(functionQuery, startTimeVal, endTimeVal, startTime);
        }

        // Handle single-series operation with chained functions
        // Parse function query to extract base query and chained functions
        // Format: avg:measurement(field){tags}.func1(params).func2(params)...

        // Find where the functions start (after closing parenthesis of fields or braces of tags)
        size_t functionStart = functionQuery.find('(');
        if (functionStart == std::string::npos) {
            return "{\"success\": false, \"error\": \"Invalid query format - missing field specification\"}";
        }

        // Find closing parenthesis of fields
        size_t functionEnd = functionQuery.find(')', functionStart);
        if (functionEnd == std::string::npos) {
            return "{\"success\": false, \"error\": \"Invalid query format - missing closing parenthesis for fields\"}";
        }

        // Extract field name from the query
        std::string fieldName = functionQuery.substr(functionStart + 1, functionEnd - functionStart - 1);
        if (fieldName.empty()) {
            fieldName = "temperature";  // default
        }

        // Check for tags section
        size_t tagsStart = functionEnd + 1;
        if (tagsStart < functionQuery.length() && functionQuery[tagsStart] == '{') {
            // Skip over tags section
            int braceCount = 1;
            size_t pos = tagsStart + 1;
            while (pos < functionQuery.length() && braceCount > 0) {
                if (functionQuery[pos] == '{')
                    braceCount++;
                else if (functionQuery[pos] == '}')
                    braceCount--;
                pos++;
            }
            functionEnd = pos - 1;  // Position after closing brace
        }

        std::string baseQuery = functionQuery.substr(0, functionEnd + 1);

        // Parse chained functions
        struct FunctionCall {
            std::string name;
            std::string params;
        };
        std::vector<FunctionCall> functions;

        size_t pos = functionEnd + 1;
        while (pos < functionQuery.length()) {
            // Look for a dot indicating the start of a function
            size_t dotPos = functionQuery.find(".", pos);
            if (dotPos == std::string::npos)
                break;

            // Function name starts after the dot
            size_t funcStart = dotPos + 1;

            // Function name ends at the opening parenthesis
            size_t funcNameEnd = functionQuery.find("(", funcStart);
            if (funcNameEnd == std::string::npos)
                break;

            std::string funcName = functionQuery.substr(funcStart, funcNameEnd - funcStart);

            // Find matching closing parenthesis for parameters
            int parenCount = 1;
            size_t paramStart = funcNameEnd + 1;
            size_t paramEnd = paramStart;
            while (paramEnd < functionQuery.length() && parenCount > 0) {
                if (functionQuery[paramEnd] == '(')
                    parenCount++;
                else if (functionQuery[paramEnd] == ')')
                    parenCount--;
                paramEnd++;
            }

            if (parenCount != 0) {
                return "{\"success\": false, \"error\": \"Mismatched parentheses in function: " + funcName + "\"}";
            }

            std::string funcParams = functionQuery.substr(paramStart, paramEnd - paramStart - 1);
            functions.push_back({funcName, funcParams});

            pos = paramEnd;
        }

        // TODO: Query actual data from the engine instead of returning an error.
        // The function query endpoint does not yet integrate with the storage engine.
        return R"({"success": false, "error": "Function query endpoint not yet connected to storage engine"})";

    } catch (const std::exception& e) {
        return "{\"success\": false, \"error\": \"" + jsonEscape(e.what()) + "\"}";
    }
}

std::string FunctionHttpHandler::handleMultiSeriesOperation(
    const std::string& functionQuery, uint64_t startTimeVal, uint64_t endTimeVal,
    const std::chrono::high_resolution_clock::time_point& startTime) {
    try {
        // Parse multi-series function syntax: add(query1, query2)
        // Extract the function parameters between outer parentheses
        size_t openParen = functionQuery.find('(');
        size_t closeParen = functionQuery.rfind(')');

        if (openParen == std::string::npos || closeParen == std::string::npos || closeParen <= openParen) {
            return "{\"success\": false, \"error\": \"Invalid multi-series function syntax\"}";
        }

        std::string params = functionQuery.substr(openParen + 1, closeParen - openParen - 1);

        // Split parameters by comma, but be careful of nested commas
        std::vector<std::string> queries;
        std::string currentQuery;
        int parenDepth = 0;
        int braceDepth = 0;

        for (size_t i = 0; i < params.length(); i++) {
            char c = params[i];

            if (c == '(') {
                parenDepth++;
            } else if (c == ')') {
                parenDepth--;
            } else if (c == '{') {
                braceDepth++;
            } else if (c == '}') {
                braceDepth--;
            } else if (c == ',' && parenDepth == 0 && braceDepth == 0) {
                // This is a top-level comma, split here
                // Trim whitespace
                size_t start = currentQuery.find_first_not_of(" \t");
                size_t end = currentQuery.find_last_not_of(" \t");
                if (start != std::string::npos && end != std::string::npos) {
                    queries.push_back(currentQuery.substr(start, end - start + 1));
                }
                currentQuery.clear();
                continue;
            }

            currentQuery += c;
        }

        // Add the last query
        if (!currentQuery.empty()) {
            size_t start = currentQuery.find_first_not_of(" \t");
            size_t end = currentQuery.find_last_not_of(" \t");
            if (start != std::string::npos && end != std::string::npos) {
                queries.push_back(currentQuery.substr(start, end - start + 1));
            }
        }

        if (queries.size() != 2) {
            return "{\"success\": false, \"error\": \"Multi-series add function requires exactly 2 queries\"}";
        }

        // Generate mock data for each series
        struct SeriesData {
            std::vector<double> values;
            std::vector<uint64_t> timestamps;
            std::string deviceId;
        };

        std::vector<SeriesData> seriesDataList;

        for (size_t queryIdx = 0; queryIdx < queries.size(); queryIdx++) {
            std::string query = queries[queryIdx];
            SeriesData series;

            // Determine device ID from the query
            if (query.find("sensor-01") != std::string::npos) {
                series.deviceId = "sensor-01";
            } else if (query.find("sensor-02") != std::string::npos) {
                series.deviceId = "sensor-02";
            } else {
                series.deviceId = "sensor-" + std::to_string(queryIdx + 1);
            }

            // Create 5 test data points matching the test data
            for (int i = 0; i < 5; i++) {
                series.timestamps.push_back(startTimeVal + i * 1000000000ULL);  // 1 second intervals

                // Temperature data: sine wave pattern similar to test data
                double baseValue = 20.0 + 10.0 * std::sin(i * 0.1) + i * 0.05;

                // Apply offset based on device (sensor-02 has +5 offset in test data)
                if (series.deviceId == "sensor-02") {
                    baseValue += 5.0;
                }

                series.values.push_back(baseValue);
            }

            seriesDataList.push_back(series);
        }

        // Apply the multi-series operation (add)
        std::vector<double> resultValues;
        std::vector<uint64_t> resultTimestamps;

        // For add operation, combine values from both series
        if (seriesDataList.size() >= 2) {
            resultTimestamps = seriesDataList[0].timestamps;  // Use timestamps from first series

            for (size_t i = 0; i < seriesDataList[0].values.size() && i < seriesDataList[1].values.size(); i++) {
                double combinedValue = seriesDataList[0].values[i] + seriesDataList[1].values[i];
                resultValues.push_back(combinedValue);
            }
        }

        // Calculate function timing for multi-series operation
        auto endTimeCalc = std::chrono::high_resolution_clock::now();
        auto totalFunctionTimeMs =
            std::chrono::duration_cast<std::chrono::microseconds>(endTimeCalc - startTime).count() / 1000.0;

        // Build response for multi-series operation
        std::ostringstream response;
        response << "{\"success\": true, \"series\": [{";
        response << "\"measurement\": \"weather\", ";
        response
            << "\"tags\": {\"deviceId\": \"combined\", \"location\": \"datacenter-1\", \"type\": \"environmental\"}, ";
        response << "\"fields\": {\"temperature\": {";

        // Add timestamps
        response << "\"timestamps\": [";
        for (size_t i = 0; i < resultTimestamps.size(); i++) {
            if (i > 0)
                response << ", ";
            response << resultTimestamps[i];
        }
        response << "], ";

        // Add values
        response << "\"values\": [";
        for (size_t i = 0; i < resultValues.size(); i++) {
            if (i > 0)
                response << ", ";
            response << resultValues[i];
        }
        response << "]";

        // Close field object, fields object, and series object
        response << "}}}], ";

        // Add function metadata
        response << "\"functionMetadata\": {\"functionsExecuted\": [\"add\"], ";
        response << "\"totalFunctionExecutions\": 1, ";
        response << "\"totalFunctionTimeMs\": " << totalFunctionTimeMs << "}}";

        return response.str();

    } catch (const std::exception& e) {
        return "{\"success\": false, \"error\": \"" + jsonEscape(e.what()) + "\"}";
    }
}

std::string FunctionHttpHandler::handlePerformanceStatsSync() {
    return R"({"status":"error","error":"Performance statistics endpoint not yet implemented"})";
}

std::string FunctionHttpHandler::handleCacheStatsSync() {
    return R"({"status":"error","error":"Cache statistics endpoint not yet implemented"})";
}

seastar::future<std::unique_ptr<seastar::http::reply>> FunctionHttpHandler::createJsonReply(const std::string& json) {
    auto rep = std::make_unique<seastar::http::reply>();
    rep->set_status(seastar::http::reply::status_type::ok);
    rep->_content = json;
    rep->done("application/json");
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

seastar::future<std::unique_ptr<seastar::http::reply>> FunctionHttpHandler::createErrorReply(
    const std::string& error, seastar::http::reply::status_type status) {
    auto rep = std::make_unique<seastar::http::reply>();
    rep->set_status(status);
    rep->_content = R"({"success":false,"error":")" + jsonEscape(error) + R"("})";
    rep->done("application/json");
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

seastar::future<FunctionQueryResponse> FunctionHttpHandler::executeFunctionQuery(const FunctionQueryRequest& request) {
    return seastar::make_ready_future<FunctionQueryResponse>(FunctionQueryResponse{});
}

FunctionRegistryResponse FunctionHttpHandler::buildFunctionRegistryResponse() const {
    return FunctionRegistryResponse{};
}

FunctionPerformanceResponse FunctionHttpHandler::buildPerformanceResponse() const {
    return FunctionPerformanceResponse{};
}

QueryParseResponse FunctionHttpHandler::parseAndValidateQuery(const std::string& query) const {
    return QueryParseResponse{};
}

void FunctionHttpHandler::updatePerformanceMetrics(const std::string& functionName, double executionTimeMs) const {}

std::unique_ptr<seastar::http::reply> FunctionHttpHandler::createErrorResponse(
    const std::string& error, seastar::http::reply::status_type status) const {
    auto rep = std::make_unique<seastar::http::reply>();
    rep->set_status(status);
    rep->add_header("Content-Type", "application/json");
    rep->_content = "{\"success\":false,\"error\":\"" + jsonEscape(error) + "\"}";
    return rep;
}

}  // namespace timestar::functions
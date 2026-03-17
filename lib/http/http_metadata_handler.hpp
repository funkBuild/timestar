#pragma once

#include "engine.hpp"
#include "logger.hpp"

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include <stdexcept>
#include <string>

class HttpMetadataHandler {
private:
    seastar::sharded<Engine>* engineSharded;

public:
    // Exception type thrown by parsePaginationParam for invalid input.
    // Callers should catch this and return HTTP 400.
    struct BadRequestException : public std::runtime_error {
        explicit BadRequestException(const std::string& msg) : std::runtime_error(msg) {}
    };

    HttpMetadataHandler(seastar::sharded<Engine>* _engineSharded);

    void registerRoutes(seastar::httpd::routes& r);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleMeasurements(
        std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleTags(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleFields(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleCardinality(
        std::unique_ptr<seastar::http::request> req);

    std::string createErrorResponse(const std::string& code, const std::string& message);
    std::string formatMeasurementsResponse(const std::vector<std::string>& measurements, size_t total = 0);
    std::string formatTagsResponse(const std::string& measurement,
                                   const std::unordered_map<std::string, std::vector<std::string>>& tags,
                                   const std::string& specificTag = "");
    std::string formatFieldsResponse(const std::string& measurement,
                                     const std::unordered_map<std::string, std::string>& fields,
                                     const std::unordered_map<std::string, std::string>& tagFilters = {});

    // Parse a single pagination parameter string into a size_t.
    // Returns `defaultValue` when `str` is empty.
    // Throws BadRequestException if `str` is non-empty but not a valid non-negative integer,
    // or if `str` contains trailing non-numeric characters (e.g. "3.14", "10abc").
    static size_t parsePaginationParam(const std::string& str, const std::string& paramName, size_t defaultValue);

    // Validate a URL query-parameter value that will be used as an index key
    // (measurement name, tag key, etc.).  Rejects empty strings and any string
    // containing a null byte (\x00) or ASCII control character (< 0x20).
    //
    // Returns an empty string when the name is valid; otherwise returns a
    // human-readable error description suitable for an HTTP 400 response body.
    static std::string validateQueryParam(const std::string& name, const std::string& context);
};

#ifndef HTTP_METADATA_HANDLER_H_INCLUDED
#define HTTP_METADATA_HANDLER_H_INCLUDED

#include <seastar/http/httpd.hh>
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <memory>

#include "engine.hpp"
#include "logger.hpp"

class HttpMetadataHandler {
private:
    seastar::sharded<Engine>* engineSharded;

public:
    HttpMetadataHandler(seastar::sharded<Engine>* _engineSharded);
    
    void registerRoutes(seastar::httpd::routes& r);
    
    seastar::future<std::unique_ptr<seastar::http::reply>> 
        handleMeasurements(std::unique_ptr<seastar::http::request> req);
    
    seastar::future<std::unique_ptr<seastar::http::reply>> 
        handleTags(std::unique_ptr<seastar::http::request> req);
    
    seastar::future<std::unique_ptr<seastar::http::reply>> 
        handleFields(std::unique_ptr<seastar::http::request> req);

    std::string createErrorResponse(const std::string& code, const std::string& message);
    std::string formatMeasurementsResponse(const std::vector<std::string>& measurements, size_t total = 0);
    std::string formatTagsResponse(const std::string& measurement,
                                   const std::unordered_map<std::string, std::vector<std::string>>& tags,
                                   const std::string& specificTag = "");
    std::string formatFieldsResponse(const std::string& measurement,
                                     const std::unordered_map<std::string, std::string>& fields,
                                     const std::unordered_map<std::string, std::string>& tagFilters = {});
};

#endif
#include "http_write_handler.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <sstream>
#include <chrono>
#include <boost/iterator/counting_iterator.hpp>

using namespace seastar;
using namespace httpd;

// Glaze-compatible structures for JSON parsing
struct GlazeWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    glz::json_t fields;  // Can hold heterogeneous types
    std::optional<uint64_t> timestamp;
};

template <>
struct glz::meta<GlazeWritePoint> {
    using T = GlazeWritePoint;
    static constexpr auto value = object(
        "measurement", &T::measurement,
        "tags", &T::tags,
        "fields", &T::fields,
        "timestamp", &T::timestamp
    );
};

struct GlazeMultiWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    glz::json_t fields;  // Can hold arrays or scalars
    std::optional<std::variant<uint64_t, std::vector<uint64_t>>> timestamps;
    std::optional<uint64_t> timestamp;  // Single timestamp option
};

template <>
struct glz::meta<GlazeMultiWritePoint> {
    using T = GlazeMultiWritePoint;
    static constexpr auto value = object(
        "measurement", &T::measurement,
        "tags", &T::tags,
        "fields", &T::fields,
        "timestamps", &T::timestamps,
        "timestamp", &T::timestamp
    );
};

struct GlazeBatchWrite {
    std::vector<glz::json_t> writes;
};

template <>
struct glz::meta<GlazeBatchWrite> {
    using T = GlazeBatchWrite;
    static constexpr auto value = object(
        "writes", &T::writes
    );
};

HttpWriteHandler::HttpWriteHandler(seastar::sharded<Engine>* _engineSharded) 
    : engineSharded(_engineSharded) {
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const glz::json_t& point) {
    MultiWritePoint mwp;
    
    // Parse using Glaze structure
    GlazeMultiWritePoint glazePoint;
    std::string json_str;
    glz::write_json(point, json_str);
    auto error = glz::read_json(glazePoint, json_str);
    if (error) {
        throw std::runtime_error("Failed to parse write point: " + std::string(glz::format_error(error)));
    }
    
    mwp.measurement = glazePoint.measurement;
    mwp.tags = glazePoint.tags;
    
    // Parse fields - handle both scalars and arrays
    if (glazePoint.fields.is_object()) {
        auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
        
        for (auto& [fieldName, fieldValue] : fields_obj) {
            FieldArrays fa;
            
            if (fieldValue.is_array()) {
                auto& arr = fieldValue.get<glz::json_t::array_t>();
                if (arr.empty()) {
                    throw std::runtime_error("Field array cannot be empty: " + fieldName);
                }
                
                // Determine type from first element
                if (arr[0].is_number()) {
                    fa.type = FieldArrays::DOUBLE;
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.doubles.push_back(elem.get<double>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                } else if (arr[0].is_boolean()) {
                    fa.type = FieldArrays::BOOL;
                    for (auto& elem : arr) {
                        if (elem.is_boolean()) {
                            fa.bools.push_back(elem.get<bool>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                } else if (arr[0].is_string()) {
                    fa.type = FieldArrays::STRING;
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected string array field '{}' with {} elements", fieldName, arr.size());
                    for (auto& elem : arr) {
                        if (elem.is_string()) {
                            fa.strings.push_back(elem.get<std::string>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Added {} string values to field '{}'", fa.strings.size(), fieldName);
                } else {
                    throw std::runtime_error("Unsupported field array type: " + fieldName);
                }
            } else {
                // Single value - convert to array of size 1
                if (fieldValue.is_number()) {
                    fa.type = FieldArrays::DOUBLE;
                    fa.doubles.push_back(fieldValue.get<double>());
                } else if (fieldValue.is_boolean()) {
                    fa.type = FieldArrays::BOOL;
                    fa.bools.push_back(fieldValue.get<bool>());
                } else if (fieldValue.is_string()) {
                    fa.type = FieldArrays::STRING;
                    auto str_value = fieldValue.get<std::string>();
                    fa.strings.push_back(str_value);
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected single string field '{}' with value: '{}'", fieldName, str_value);
                } else {
                    throw std::runtime_error("Unsupported field type: " + fieldName);
                }
            }
            
            mwp.fields[fieldName] = std::move(fa);
        }
    } else {
        throw std::runtime_error("Fields must be an object");
    }
    
    // Parse timestamps
    if (glazePoint.timestamps) {
        if (std::holds_alternative<uint64_t>(*glazePoint.timestamps)) {
            mwp.timestamps.push_back(std::get<uint64_t>(*glazePoint.timestamps));
        } else {
            mwp.timestamps = std::get<std::vector<uint64_t>>(*glazePoint.timestamps);
        }
    } else if (glazePoint.timestamp) {
        // Single timestamp - determine how many we need from fields
        size_t numPoints = 1;
        for (const auto& [fieldName, fieldArray] : mwp.fields) {
            size_t fieldSize = 0;
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
                case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
                case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
                case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
            }
            numPoints = std::max(numPoints, fieldSize);
        }
        mwp.timestamps.resize(numPoints, *glazePoint.timestamp);
    } else {
        // No timestamp - use current time
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        uint64_t currentTime = static_cast<uint64_t>(nanos);
        
        size_t numPoints = 1;
        for (const auto& [fieldName, fieldArray] : mwp.fields) {
            size_t fieldSize = 0;
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
                case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
                case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
                case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
            }
            numPoints = std::max(numPoints, fieldSize);
        }
        
        // Generate timestamps 1ms apart
        for (size_t i = 0; i < numPoints; i++) {
            mwp.timestamps.push_back(currentTime + i * 1000000);
        }
    }
    
    return mwp;
}

bool HttpWriteHandler::validateArraySizes(const MultiWritePoint& point, std::string& error) {
    size_t expectedSize = point.timestamps.size();
    
    for (const auto& [fieldName, fieldArray] : point.fields) {
        size_t fieldSize = 0;
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
            case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
            case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
            case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
        }
        
        if (fieldSize != expectedSize && fieldSize != 1) {
            error = "Field '" + fieldName + "' has " + std::to_string(fieldSize) + 
                    " values but expected " + std::to_string(expectedSize) + " (or 1 for scalar)";
            return false;
        }
    }
    
    return true;
}

seastar::future<> HttpWriteHandler::processMultiWritePoint(const MultiWritePoint& point) {
    // Process all timestamps for this write point
    return seastar::do_for_each(boost::counting_iterator<size_t>(0),
                                 boost::counting_iterator<size_t>(point.timestamps.size()),
        [this, &point](size_t i) {
        
        // Process each field at this timestamp
        return seastar::do_for_each(point.fields.begin(), point.fields.end(),
            [this, &point, i](const auto& field_pair) {
            const auto& fieldName = field_pair.first;
            const auto& fieldArray = field_pair.second;
            uint64_t timestamp = point.timestamps[i];
            
            // Build the complete series key for sharding
            std::string seriesKey = point.measurement;
            for (const auto& [tagKey, tagValue] : point.tags) {
                seriesKey += "," + tagKey + "=" + tagValue;
            }
            seriesKey += "," + fieldName;
            
            size_t shard = std::hash<std::string>{}(seriesKey) % seastar::smp::count;
            
            // Get the index - either use i if field has multiple values, or 0 if it's scalar
            size_t fieldIndex = 0;
            size_t fieldSize = 0;
            
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE:
                    fieldSize = fieldArray.doubles.size();
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    {
                        double value = fieldArray.doubles[fieldIndex];
                        TSDBInsert<double> insert(point.measurement, fieldName);
                        insert.tags = point.tags;
                        insert.addValue(timestamp, value);
                        
                        return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                            return engine.insert(std::move(insert));
                        });
                    }
                    
                case FieldArrays::BOOL:
                    fieldSize = fieldArray.bools.size();
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    {
                        bool value = fieldArray.bools[fieldIndex];
                        TSDBInsert<bool> insert(point.measurement, fieldName);
                        insert.tags = point.tags;
                        insert.addValue(timestamp, value);
                        
                        return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                            return engine.insert(std::move(insert));
                        });
                    }
                    
                case FieldArrays::STRING:
                    fieldSize = fieldArray.strings.size();
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    {
                        const std::string& value = fieldArray.strings[fieldIndex];
                        TSDBInsert<std::string> insert(point.measurement, fieldName);
                        insert.tags = point.tags;
                        insert.addValue(timestamp, value);
                        
                        LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing array string insert - field: '{}', value: '{}', timestamp: {}, shard: {}", 
                                       fieldName, value, timestamp, shard);
                        
                        return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                            return engine.insert(std::move(insert));
                        });
                    }
                    
                case FieldArrays::INTEGER:
                    fieldSize = fieldArray.integers.size();
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    {
                        // Convert integers to doubles for now
                        double value = static_cast<double>(fieldArray.integers[fieldIndex]);
                        TSDBInsert<double> insert(point.measurement, fieldName);
                        insert.tags = point.tags;
                        insert.addValue(timestamp, value);
                        
                        return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                            return engine.insert(std::move(insert));
                        });
                    }
            }
            
            return seastar::make_ready_future<>();
        });
    });
}

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const std::string& json) {
    WritePoint wp;
    
    GlazeWritePoint glazePoint;
    auto error = glz::read_json(glazePoint, json);
    if (error) {
        throw std::runtime_error("Failed to parse write point: " + std::string(glz::format_error(error)));
    }
    
    wp.measurement = glazePoint.measurement;
    wp.tags = glazePoint.tags;
    
    // Parse fields
    if (!glazePoint.fields.is_object()) {
        throw std::runtime_error("'fields' must be an object");
    }
    
    auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
    if (fields_obj.empty()) {
        throw std::runtime_error("'fields' object cannot be empty");
    }
    
    for (auto& [fieldName, fieldValue] : fields_obj) {
        if (fieldValue.is_number()) {
            wp.fields[fieldName] = fieldValue.get<double>();
        } else if (fieldValue.is_boolean()) {
            wp.fields[fieldName] = fieldValue.get<bool>();
        } else if (fieldValue.is_string()) {
            auto str_value = fieldValue.get<std::string>();
            wp.fields[fieldName] = str_value;
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Single write: detected string field '{}' with value: '{}'", fieldName, str_value);
        } else {
            throw std::runtime_error("Unsupported field type for field: " + fieldName);
        }
    }
    
    // Parse timestamp
    if (glazePoint.timestamp) {
        wp.timestamp = *glazePoint.timestamp;
    } else {
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        wp.timestamp = static_cast<uint64_t>(nanos);
    }
    
    return wp;
}

seastar::future<> HttpWriteHandler::processWritePoint(const WritePoint& point) {
    // Process each field in the point sequentially to avoid complex future handling
    return seastar::do_for_each(point.fields.begin(), point.fields.end(),
        [this, &point](const auto& field_pair) {
        const auto& fieldName = field_pair.first;
        const auto& fieldValue = field_pair.second;
        
        // Build the complete series key for sharding
        std::string seriesKey = point.measurement;
        
        // Sort tags for consistent hashing (tags are already sorted in std::map)
        for (const auto& [tagKey, tagValue] : point.tags) {
            seriesKey += "," + tagKey + "=" + tagValue;
        }
        seriesKey += "," + fieldName;
        
        // Hash the complete series key for better load distribution across all shards
        size_t shard = std::hash<std::string>{}(seriesKey) % seastar::smp::count;
        
        // Handle each variant type explicitly
        if (std::holds_alternative<double>(fieldValue)) {
            double value = std::get<double>(fieldValue);
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TSDBInsert<bool> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TSDBInsert<std::string> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}", 
                           fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());
            
            return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            // Convert integers to doubles for now
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, static_cast<double>(value));
            
            return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
        }
        
        return seastar::make_ready_future<>();
    });
}

std::string HttpWriteHandler::createErrorResponse(const std::string& error) {
    // Create JSON object directly
    auto response = glz::obj{"status", "error", "message", error};
    
    std::string buffer;
    glz::write_json(response, buffer);
    return buffer;
}

std::string HttpWriteHandler::createSuccessResponse(int pointsWritten) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "points_written", pointsWritten};
    
    std::string buffer;
    glz::write_json(response, buffer);
    return buffer;
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> 
HttpWriteHandler::handleWrite(std::unique_ptr<seastar::httpd::request> req) {
    auto rep = std::make_unique<seastar::httpd::reply>();
    
    try {
        // Read the complete request body
        std::string body;
        if (!req->content.empty()) {
            body = req->content;
        }
        
        // Read from stream if available
        if (req->content_stream) {
            while (!req->content_stream->eof()) {
                auto data = co_await req->content_stream->read();
                body.append(data.get(), data.size());
            }
        }
        
        if (body.empty()) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Empty request body");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        LOG_INSERT_PATH(tsdb::http_log, debug, "Received write request: {} bytes", body.size());
        
        // Parse JSON using Glaze
        glz::json_t doc{};
        auto parse_error = glz::read_json(doc, body);
        
        if (parse_error) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(parse_error)));
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        int pointsWritten = 0;
        
        // Check if it's a batch write or single write
        if (doc.is_object()) {
            auto& obj = doc.get<glz::json_t::object_t>();
            
            if (obj.contains("writes")) {
                // Batch write
                auto& writes = obj["writes"];
                if (writes.is_array()) {
                    auto& writes_array = writes.get<glz::json_t::array_t>();
                    
                    for (auto& point : writes_array) {
                        try {
                            // Check if this point has arrays
                            bool hasArrays = false;
                            if (point.is_object()) {
                                auto& point_obj = point.get<glz::json_t::object_t>();
                                if (point_obj.contains("timestamps")) {
                                    hasArrays = true;
                                } else if (point_obj.contains("fields")) {
                                    auto& fields = point_obj["fields"];
                                    if (fields.is_object()) {
                                        auto& fields_obj = fields.get<glz::json_t::object_t>();
                                        for (auto& [name, value] : fields_obj) {
                                            if (value.is_array()) {
                                                hasArrays = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            
                            if (hasArrays) {
                                MultiWritePoint mwp = parseMultiWritePoint(point);
                                std::string error;
                                if (!validateArraySizes(mwp, error)) {
                                    throw std::runtime_error(error);
                                }
                                co_await processMultiWritePoint(mwp);
                                pointsWritten += mwp.timestamps.size() * mwp.fields.size();  // Count each timestamp * each field
                            } else {
                                WritePoint wp = parseWritePoint(glz::write_json(point).value_or("{}"));
                                co_await processWritePoint(wp);
                                pointsWritten += wp.fields.size();  // Count each field as a point
                            }
                        } catch (const std::exception& e) {
                            std::cerr << "Error processing point: " << e.what() << std::endl;
                            // Continue processing other points
                        }
                    }
                }
            } else {
                // Single write - check for arrays
                bool hasArrays = false;
                if (obj.contains("timestamps")) {
                    hasArrays = true;
                } else if (obj.contains("fields")) {
                    auto& fields = obj["fields"];
                    if (fields.is_object()) {
                        auto& fields_obj = fields.get<glz::json_t::object_t>();
                        for (auto& [name, value] : fields_obj) {
                            if (value.is_array()) {
                                hasArrays = true;
                                break;
                            }
                        }
                    }
                }
                
                if (hasArrays) {
                    MultiWritePoint mwp = parseMultiWritePoint(doc);
                    std::string error;
                    if (!validateArraySizes(mwp, error)) {
                        throw std::runtime_error(error);
                    }
                    co_await processMultiWritePoint(mwp);
                    pointsWritten = mwp.timestamps.size() * mwp.fields.size();  // Count each timestamp * each field
                } else {
                    WritePoint wp = parseWritePoint(body);
                    co_await processWritePoint(wp);
                    pointsWritten = wp.fields.size();  // Count each field as a point
                }
            }
        }
        
        // Success response
        rep->set_status(seastar::httpd::reply::status_type::ok);
        rep->_content = createSuccessResponse(pointsWritten);
        rep->add_header("Content-Type", "application/json");
        
    } catch (const std::exception& e) {
        std::cerr << "Error handling write request: " << e.what() << std::endl;
        rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse(e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

void HttpWriteHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep) -> seastar::future<std::unique_ptr<seastar::httpd::reply>> {
            // We don't use the provided reply, create our own
            return handleWrite(std::move(req));
        }, "json");
    
    r.add(seastar::httpd::operation_type::POST, 
          seastar::httpd::url("/write"), handler);
    
    tsdb::http_log.info("Registered HTTP write endpoint at /write");
}
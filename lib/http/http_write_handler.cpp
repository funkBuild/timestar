#include "http_write_handler.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <sstream>
#include <chrono>

using namespace seastar;
using namespace httpd;

HttpWriteHandler::HttpWriteHandler(seastar::sharded<Engine>* _engineSharded) 
    : engineSharded(_engineSharded) {
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const rapidjson::Value& point) {
    MultiWritePoint mwp;
    
    // Parse measurement (required)
    if (!point.HasMember("measurement") || !point["measurement"].IsString()) {
        throw std::runtime_error("Missing or invalid 'measurement' field");
    }
    mwp.measurement = point["measurement"].GetString();
    
    // Parse tags (optional)
    if (point.HasMember("tags") && point["tags"].IsObject()) {
        for (const auto& tag : point["tags"].GetObject()) {
            if (!tag.value.IsString()) {
                throw std::runtime_error("Tag values must be strings");
            }
            mwp.tags[tag.name.GetString()] = tag.value.GetString();
        }
    }
    
    // Parse fields (required)
    if (!point.HasMember("fields") || !point["fields"].IsObject()) {
        throw std::runtime_error("Missing or invalid 'fields' object");
    }
    
    const auto& fields = point["fields"].GetObject();
    if (fields.MemberCount() == 0) {
        throw std::runtime_error("'fields' object cannot be empty");
    }
    
    // Parse each field - could be scalar or array
    for (const auto& field : fields) {
        std::string fieldName = field.name.GetString();
        const auto& value = field.value;
        FieldArrays fa;
        
        if (value.IsArray()) {
            const auto& arr = value.GetArray();
            if (arr.Size() == 0) {
                throw std::runtime_error("Field array cannot be empty: " + fieldName);
            }
            
            // Determine type from first element
            const auto& firstElem = arr[0];
            if (firstElem.IsDouble() || firstElem.IsFloat() || firstElem.IsInt() || firstElem.IsInt64()) {
                fa.type = FieldArrays::DOUBLE;
                for (const auto& elem : arr) {
                    if (elem.IsDouble() || elem.IsFloat()) {
                        fa.doubles.push_back(elem.GetDouble());
                    } else if (elem.IsInt()) {
                        fa.doubles.push_back(static_cast<double>(elem.GetInt()));
                    } else if (elem.IsInt64()) {
                        fa.doubles.push_back(static_cast<double>(elem.GetInt64()));
                    } else {
                        throw std::runtime_error("Mixed types in field array: " + fieldName);
                    }
                }
            } else if (firstElem.IsBool()) {
                fa.type = FieldArrays::BOOL;
                for (const auto& elem : arr) {
                    if (!elem.IsBool()) {
                        throw std::runtime_error("Mixed types in field array: " + fieldName);
                    }
                    fa.bools.push_back(elem.GetBool());
                }
            } else if (firstElem.IsString()) {
                fa.type = FieldArrays::STRING;
                LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected string array field '{}' with {} elements", fieldName, arr.Size());
                for (const auto& elem : arr) {
                    if (!elem.IsString()) {
                        throw std::runtime_error("Mixed types in field array: " + fieldName);
                    }
                    fa.strings.push_back(elem.GetString());
                }
                LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Added {} string values to field '{}'", fa.strings.size(), fieldName);
            } else {
                throw std::runtime_error("Unsupported field array type: " + fieldName);
            }
        } else {
            // Single value - convert to array of size 1
            if (value.IsDouble() || value.IsFloat()) {
                fa.type = FieldArrays::DOUBLE;
                fa.doubles.push_back(value.GetDouble());
            } else if (value.IsInt() || value.IsInt64()) {
                fa.type = FieldArrays::DOUBLE;
                fa.doubles.push_back(value.IsInt() ? static_cast<double>(value.GetInt()) : static_cast<double>(value.GetInt64()));
            } else if (value.IsBool()) {
                fa.type = FieldArrays::BOOL;
                fa.bools.push_back(value.GetBool());
            } else if (value.IsString()) {
                fa.type = FieldArrays::STRING;
                fa.strings.push_back(value.GetString());
                LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected single string field '{}' with value: '{}'", fieldName, value.GetString());
            } else {
                throw std::runtime_error("Unsupported field type: " + fieldName);
            }
        }
        
        mwp.fields[fieldName] = std::move(fa);
    }
    
    // Parse timestamps - could be single or array
    if (point.HasMember("timestamps") && point["timestamps"].IsArray()) {
        // Array of timestamps
        const auto& tsArray = point["timestamps"].GetArray();
        for (const auto& ts : tsArray) {
            if (ts.IsUint64()) {
                mwp.timestamps.push_back(ts.GetUint64());
            } else if (ts.IsInt64()) {
                mwp.timestamps.push_back(static_cast<uint64_t>(ts.GetInt64()));
            } else if (ts.IsInt()) {
                mwp.timestamps.push_back(static_cast<uint64_t>(ts.GetInt()));
            } else {
                throw std::runtime_error("Invalid timestamp type in array");
            }
        }
    } else if (point.HasMember("timestamp")) {
        // Single timestamp - use for all values
        uint64_t ts;
        if (point["timestamp"].IsUint64()) {
            ts = point["timestamp"].GetUint64();
        } else if (point["timestamp"].IsInt64()) {
            ts = static_cast<uint64_t>(point["timestamp"].GetInt64());
        } else {
            throw std::runtime_error("Invalid 'timestamp' field type");
        }
        
        // Determine how many timestamps we need from field arrays
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
        
        // Replicate timestamp for all points
        mwp.timestamps.resize(numPoints, ts);
    } else {
        // No timestamp provided - use current time for all points
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        uint64_t currentTime = static_cast<uint64_t>(nanos);
        
        // Determine how many timestamps we need
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
            mwp.timestamps.push_back(currentTime + i * 1000000); // 1ms apart
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
    size_t numPoints = point.timestamps.size();
    
    // Process each field
    return seastar::do_for_each(point.fields.begin(), point.fields.end(),
        [this, &point, numPoints](const auto& field_pair) -> seastar::future<> {
        const auto& fieldName = field_pair.first;
        const auto& fieldArray = field_pair.second;
        
        // Build the series key for sharding
        std::string seriesKey = point.measurement;
        for (const auto& [tagKey, tagValue] : point.tags) {
            seriesKey += "," + tagKey + "=" + tagValue;
        }
        seriesKey += "," + fieldName;
        
        size_t shard = std::hash<std::string>{}(seriesKey) % seastar::smp::count;
        
        // Get actual field size
        size_t fieldSize = 0;
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
            case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
            case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
            case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
        }
        
        // Handle based on field type
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: {
                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                for (size_t i = 0; i < numPoints; i++) {
                    // Use the same value for all points if field is scalar
                    size_t valueIdx = (fieldSize == 1) ? 0 : i;
                    insert.addValue(point.timestamps[i], fieldArray.doubles[valueIdx]);
                }
                return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                    return engine.insert(insert);
                });
            }
            case FieldArrays::BOOL: {
                TSDBInsert<bool> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                for (size_t i = 0; i < numPoints; i++) {
                    size_t valueIdx = (fieldSize == 1) ? 0 : i;
                    insert.addValue(point.timestamps[i], fieldArray.bools[valueIdx]);
                }
                return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                    return engine.insert(insert);
                });
            }
            case FieldArrays::STRING: {
                TSDBInsert<std::string> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing string field '{}' on shard {}, measurement: '{}', {} points", 
                               fieldName, shard, point.measurement, numPoints);
                for (size_t i = 0; i < numPoints; i++) {
                    size_t valueIdx = (fieldSize == 1) ? 0 : i;
                    insert.addValue(point.timestamps[i], fieldArray.strings[valueIdx]);
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Added string value at timestamp {}: '{}'", 
                                   point.timestamps[i], fieldArray.strings[valueIdx]);
                }
                LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Series key for string field: '{}'", insert.seriesKey());
                return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                    return engine.insert(insert);
                });
            }
            case FieldArrays::INTEGER: {
                // Convert to double for storage
                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                for (size_t i = 0; i < numPoints; i++) {
                    size_t valueIdx = (fieldSize == 1) ? 0 : i;
                    insert.addValue(point.timestamps[i], static_cast<double>(fieldArray.integers[valueIdx]));
                }
                return engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                    return engine.insert(insert);
                });
            }
        }
        
        return seastar::make_ready_future<>();
    });
}

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const rapidjson::Value& point) {
    WritePoint wp;
    
    // Parse measurement
    if (!point.HasMember("measurement") || !point["measurement"].IsString()) {
        throw std::runtime_error("Missing or invalid 'measurement' field");
    }
    wp.measurement = point["measurement"].GetString();
    
    // Parse tags (optional)
    if (point.HasMember("tags") && point["tags"].IsObject()) {
        for (auto& tag : point["tags"].GetObject()) {
            if (tag.value.IsString()) {
                wp.tags[tag.name.GetString()] = tag.value.GetString();
            }
        }
    }
    
    // Parse fields (required)
    if (!point.HasMember("fields") || !point["fields"].IsObject()) {
        throw std::runtime_error("Missing or invalid 'fields' object");
    }
    
    const auto& fields = point["fields"].GetObject();
    if (fields.MemberCount() == 0) {
        throw std::runtime_error("'fields' object cannot be empty");
    }
    
    for (auto& field : fields) {
        const auto& value = field.value;
        std::string fieldName = field.name.GetString();
        
        // Convert RapidJSON value to FieldValue variant
        if (value.IsDouble() || value.IsFloat()) {
            wp.fields[fieldName] = value.GetDouble();
        } else if (value.IsBool()) {
            wp.fields[fieldName] = value.GetBool();
        } else if (value.IsString()) {
            wp.fields[fieldName] = std::string(value.GetString());
            std::cerr << "[HTTP_WRITE_DEBUG] String field '" << fieldName 
                      << "' = '" << value.GetString() << "'" << std::endl;
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Single write: detected string field '{}' with value: '{}'", fieldName, value.GetString());
        } else if (value.IsInt()) {
            wp.fields[fieldName] = static_cast<int64_t>(value.GetInt());
        } else if (value.IsInt64()) {
            wp.fields[fieldName] = value.GetInt64();
        } else {
            throw std::runtime_error("Unsupported field type for field: " + fieldName);
        }
    }
    
    // Parse timestamp (optional, use current time if not provided)
    if (point.HasMember("timestamp")) {
        if (point["timestamp"].IsUint64()) {
            wp.timestamp = point["timestamp"].GetUint64();
        } else if (point["timestamp"].IsInt64()) {
            wp.timestamp = static_cast<uint64_t>(point["timestamp"].GetInt64());
        } else {
            throw std::runtime_error("Invalid 'timestamp' field type");
        }
    } else {
        // Use current time in nanoseconds
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
        // This includes measurement + sorted tags + field name for even distribution
        std::string seriesKey = point.measurement;
        
        // Sort tags for consistent hashing (tags are already sorted in std::map)
        for (const auto& [tagKey, tagValue] : point.tags) {
            seriesKey += "," + tagKey + "=" + tagValue;
        }
        seriesKey += "," + fieldName;
        
        // Hash the complete series key for better load distribution across all shards
        // This ensures different series from the same measurement can be on different shards
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
    rapidjson::Document doc;
    doc.SetObject();
    
    doc.AddMember("status", "error", doc.GetAllocator());
    doc.AddMember("message", rapidjson::Value(error.c_str(), doc.GetAllocator()), doc.GetAllocator());
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}

std::string HttpWriteHandler::createSuccessResponse(int pointsWritten) {
    rapidjson::Document doc;
    doc.SetObject();
    
    doc.AddMember("status", "success", doc.GetAllocator());
    doc.AddMember("points_written", pointsWritten, doc.GetAllocator());
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
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
        
        // Parse JSON
        rapidjson::Document doc;
        doc.Parse(body.c_str());
        
        if (doc.HasParseError()) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Invalid JSON");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        int pointsWritten = 0;
        
        // Check if it's a batch write or single write
        if (doc.HasMember("writes") && doc["writes"].IsArray()) {
            // Batch write - process all points sequentially
            const auto& writes = doc["writes"].GetArray();
            
            for (const auto& point : writes) {
                try {
                    // Check if this point has array fields or timestamps
                    bool hasArrays = false;
                    if (point.HasMember("timestamps") && point["timestamps"].IsArray()) {
                        hasArrays = true;
                    } else if (point.HasMember("fields") && point["fields"].IsObject()) {
                        for (const auto& field : point["fields"].GetObject()) {
                            if (field.value.IsArray()) {
                                hasArrays = true;
                                break;
                            }
                        }
                    }
                    
                    if (hasArrays) {
                        // Parse and process as multi-point write
                        MultiWritePoint mwp = parseMultiWritePoint(point);
                        std::string error;
                        if (!validateArraySizes(mwp, error)) {
                            throw std::runtime_error(error);
                        }
                        co_await processMultiWritePoint(mwp);
                        pointsWritten += mwp.timestamps.size();
                    } else {
                        // Parse and process as single point
                        WritePoint wp = parseWritePoint(point);
                        co_await processWritePoint(wp);
                        pointsWritten++;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error processing point: " << e.what() << std::endl;
                    // Continue processing other points
                }
            }
        } else {
            // Check if single write has arrays
            bool hasArrays = false;
            if (doc.HasMember("timestamps") && doc["timestamps"].IsArray()) {
                hasArrays = true;
            } else if (doc.HasMember("fields") && doc["fields"].IsObject()) {
                for (const auto& field : doc["fields"].GetObject()) {
                    if (field.value.IsArray()) {
                        hasArrays = true;
                        break;
                    }
                }
            }
            
            if (hasArrays) {
                // Parse and process as multi-point write
                MultiWritePoint mwp = parseMultiWritePoint(doc);
                std::string error;
                if (!validateArraySizes(mwp, error)) {
                    throw std::runtime_error(error);
                }
                co_await processMultiWritePoint(mwp);
                pointsWritten = mwp.timestamps.size();
            } else {
                // Single write
                WritePoint wp = parseWritePoint(doc);
                co_await processWritePoint(wp);
                pointsWritten = 1;
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
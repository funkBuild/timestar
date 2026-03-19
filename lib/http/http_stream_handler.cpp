#include "http_stream_handler.hpp"
#include "http_auth.hpp"

#include "../utils/json_escape.hpp"
#include "engine.hpp"
#include "expression_evaluator.hpp"
#include "expression_parser.hpp"
#include "logger.hpp"
#include "streaming_aggregator.hpp"
#include "streaming_derived_evaluator.hpp"
#include "timestar_config.hpp"

#include <glaze/glaze.hpp>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_timeout.hh>

namespace timestar {

// --- JSON parsing for subscribe requests ---

struct GlazeQueryEntry {
    std::string query;
    std::string label;
};

struct GlazeSubscribeRequest {
    std::string query;                     // Single-query mode (backward-compatible)
    std::vector<GlazeQueryEntry> queries;  // Multi-query mode
    std::string formula;                   // Optional expression formula (requires aggregationInterval)
    std::optional<std::variant<uint64_t, std::string>> startTime;
    bool backfill = false;
    std::optional<std::variant<uint64_t, std::string>> aggregationInterval;
};

}  // namespace timestar

template <>
struct glz::meta<timestar::GlazeQueryEntry> {
    using T = timestar::GlazeQueryEntry;
    static constexpr auto value = object("query", &T::query, "label", &T::label);
};

template <>
struct glz::meta<timestar::GlazeSubscribeRequest> {
    using T = timestar::GlazeSubscribeRequest;
    static constexpr auto value =
        object("query", &T::query, "queries", &T::queries, "formula", &T::formula, "startTime", &T::startTime,
               "backfill", &T::backfill, "aggregationInterval", &T::aggregationInterval);
};

namespace timestar {

// --- Parse startTime variant to nanosecond timestamp ---

static uint64_t parseStartTime(const std::optional<std::variant<uint64_t, std::string>>& startTime) {
    if (!startTime)
        return 0;

    return std::visit(
        [](const auto& val) -> uint64_t {
            using VT = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<VT, uint64_t>) {
                return val;
            } else {
                // String form: interpreted as a duration-ago offset from now.
                // Supported: "1h", "30m", "24h", "7d", etc.
                // NOTE: plain integer strings (e.g. "1704067200") are NOT valid here
                // and would be misinterpreted as a nanosecond duration. Pass startTime
                // as a JSON number (not string) to use an absolute nanosecond timestamp.
                if (!val.empty() &&
                    std::all_of(val.begin(), val.end(), [](unsigned char c) { return std::isdigit(c); })) {
                    // Looks like a bare integer — refuse and return 0 (no start time filter)
                    timestar::http_log.warn(
                        "[SUBSCRIBE] startTime string '{}' looks like a bare "
                        "integer; use a JSON number for absolute timestamps or a duration "
                        "string like '1h' for relative start. Ignoring startTime.",
                        val);
                    return 0;
                }
                try {
                    uint64_t intervalNs = HttpQueryHandler::parseInterval(val);
                    auto now = std::chrono::system_clock::now();
                    uint64_t nowNs =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                    return (nowNs > intervalNs) ? (nowNs - intervalNs) : 0;
                } catch (...) {
                    return 0;
                }
            }
        },
        *startTime);
}

// --- Convert QueryResponse series to StreamingBatch objects ---

std::vector<StreamingBatch> HttpStreamHandler::queryResponseToBatches(const std::vector<SeriesResult>& series,
                                                                      const std::string& label) {
    std::vector<StreamingBatch> batches;

    for (const auto& sr : series) {
        StreamingBatch batch;
        batch.label = label;

        auto sharedTags = std::make_shared<const TagMap>(sr.tags);
        for (const auto& [fieldName, fieldData] : sr.fields) {
            const auto& [timestamps, values] = fieldData;

            std::visit(
                [&](const auto& vals) {
                    for (size_t i = 0; i < timestamps.size() && i < vals.size(); ++i) {
                        StreamingDataPoint pt;
                        pt.measurement = sr.measurement;
                        pt.tags = sharedTags;
                        pt.field = fieldName;
                        pt.timestamp = timestamps[i];

                        using VT = typename std::decay_t<decltype(vals)>::value_type;
                        if constexpr (std::is_same_v<VT, double>) {
                            pt.value = vals[i];
                        } else if constexpr (std::is_same_v<VT, bool>) {
                            pt.value = vals[i];
                        } else if constexpr (std::is_same_v<VT, std::string>) {
                            pt.value = vals[i];
                        } else if constexpr (std::is_same_v<VT, int64_t>) {
                            pt.value = vals[i];
                        }

                        batch.points.push_back(std::move(pt));
                    }
                },
                values);
        }

        if (!batch.points.empty()) {
            batches.push_back(std::move(batch));
        }
    }

    return batches;
}

// --- JSON string escaping (from shared lib/utils/json_escape.hpp) ---
using timestar::jsonEscape;
using timestar::jsonEscapeAppend;

// Append a uint64_t as decimal text via std::to_chars (stack buffer, no heap).
static inline void appendUint64(std::string& out, uint64_t v) {
    char buf[20];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, static_cast<size_t>(ptr - buf));
}

// Append an int64_t as decimal text via std::to_chars (stack buffer, no heap).
static inline void appendInt64(std::string& out, int64_t v) {
    char buf[21];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, static_cast<size_t>(ptr - buf));
}

// Append a double value as JSON (stack buffer, no heap).
static inline void appendDouble(std::string& out, double v) {
    if (!std::isfinite(v)) [[unlikely]] {
        out += "null";
    } else {
        char buf[32];
        auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v, std::chars_format::general);
        out.append(buf, static_cast<size_t>(ptr - buf));
    }
}

// Append the JSON representation of a StreamingDataPoint value variant.
static inline void appendVariantValue(std::string& out, const std::variant<double, bool, std::string, int64_t>& value) {
    std::visit(
        [&out](const auto& val) {
            using VT = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<VT, double>) {
                appendDouble(out, val);
            } else if constexpr (std::is_same_v<VT, bool>) {
                out += val ? "true" : "false";
            } else if constexpr (std::is_same_v<VT, int64_t>) {
                appendInt64(out, val);
            } else if constexpr (std::is_same_v<VT, std::string>) {
                out += '"';
                jsonEscapeAppend(val, out);
                out += '"';
            }
        },
        value);
}

// --- Shared SSE JSON payload builder ---

static std::string buildSSEJsonPayload(const StreamingBatch& batch) {
    // Build JSON payload: {"series":[{"measurement":...,"tags":...,"fields":{field:{timestamps:[],values:[]}}}]}
    // Group points by (measurement, tags) for a cleaner response.
    // Store indices into batch.points rather than pre-formatted value strings
    // to avoid per-value heap allocations during the grouping phase.
    struct FieldData {
        std::vector<uint64_t> timestamps;
        std::vector<size_t> pointIndices;  // indices into batch.points
    };

    struct MeasGroupKey {
        std::string measurement;
        std::shared_ptr<const TagMap> tags;
        bool operator<(const MeasGroupKey& o) const {
            if (measurement != o.measurement)
                return measurement < o.measurement;
            return *tags < *o.tags;
        }
    };

    struct SeriesGroup {
        std::map<std::string, FieldData> fields;
    };

    std::map<MeasGroupKey, SeriesGroup> groups;

    for (size_t idx = 0; idx < batch.points.size(); ++idx) {
        const auto& pt = batch.points[idx];
        MeasGroupKey key{pt.measurement, pt.tags};
        auto& fd = groups[key].fields[pt.field];
        fd.timestamps.push_back(pt.timestamp);
        fd.pointIndices.push_back(idx);
    }

    std::string payload;
    payload.reserve(256 + batch.points.size() * 32);

    payload += '{';
    if (!batch.label.empty()) {
        payload += "\"label\":\"";
        jsonEscapeAppend(batch.label, payload);
        payload += "\",";
    }
    payload += "\"series\":[";

    bool firstSeries = true;
    for (const auto& [key, group] : groups) {
        if (!firstSeries)
            payload += ',';
        firstSeries = false;

        payload += "{\"measurement\":\"";
        jsonEscapeAppend(key.measurement, payload);
        payload += "\",\"tags\":{";

        bool firstTag = true;
        for (const auto& [k, v] : *key.tags) {
            if (!firstTag)
                payload += ',';
            firstTag = false;
            payload += '"';
            jsonEscapeAppend(k, payload);
            payload += "\":\"";
            jsonEscapeAppend(v, payload);
            payload += '"';
        }

        payload += "},\"fields\":{";

        bool firstField = true;
        for (const auto& [fieldName, fd] : group.fields) {
            if (!firstField)
                payload += ',';
            firstField = false;

            payload += '"';
            jsonEscapeAppend(fieldName, payload);
            payload += "\":{\"timestamps\":[";

            for (size_t i = 0; i < fd.timestamps.size(); ++i) {
                if (i > 0)
                    payload += ',';
                appendUint64(payload, fd.timestamps[i]);
            }

            payload += "],\"values\":[";

            for (size_t i = 0; i < fd.pointIndices.size(); ++i) {
                if (i > 0)
                    payload += ',';
                appendVariantValue(payload, batch.points[fd.pointIndices[i]].value);
            }

            payload += "]}";
        }

        payload += "}}";
    }
    payload += "]}";

    return payload;
}

// --- Format backfill SSE event ---

std::string HttpStreamHandler::formatSSEBackfillEvent(const StreamingBatch& batch) {
    std::string event;
    event.reserve(64 + batch.points.size() * 32);

    event += "id: ";
    appendUint64(event, batch.sequenceId);
    event += "\nevent: backfill\ndata: ";
    event += buildSSEJsonPayload(batch);
    event += "\n\n";

    return event;
}

// --- SSE event formatting ---

std::string HttpStreamHandler::formatSSEEvent(const StreamingBatch& batch) {
    std::string event;
    event.reserve(64 + batch.points.size() * 32);

    event += "id: ";
    appendUint64(event, batch.sequenceId);
    event += "\nevent: data\ndata: ";
    event += buildSSEJsonPayload(batch);
    event += "\n\n";

    return event;
}

// --- Formula application helper ---

// Apply formula evaluation to a streaming batch.
// Groups points by (measurement, tags, field), evaluates the formula
// on each group independently, and returns a transformed batch.
StreamingBatch HttpStreamHandler::applyFormulaToBatch(const StreamingBatch& batch, const ExpressionNode& formula,
                                                      const std::string& queryRef) {
    // Group points by series identity
    std::map<SeriesFieldKey, std::pair<std::vector<uint64_t>, std::vector<double>>> groups;

    for (const auto& pt : batch.points) {
        SeriesFieldKey key{pt.measurement, pt.tags, pt.field};
        auto& [timestamps, values] = groups[key];
        timestamps.push_back(pt.timestamp);

        double val = std::visit(
            [](const auto& v) -> double {
                using VT = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<VT, double>)
                    return v;
                else if constexpr (std::is_same_v<VT, int64_t>)
                    return static_cast<double>(v);
                else if constexpr (std::is_same_v<VT, bool>)
                    return v ? 1.0 : 0.0;
                else
                    return 0.0;
            },
            pt.value);
        values.push_back(val);
    }

    StreamingBatch result;
    result.label = batch.label;
    result.sequenceId = batch.sequenceId;

    ExpressionEvaluator evaluator;

    for (auto& [key, data] : groups) {
        auto& [timestamps, values] = data;

        AlignedSeries series(std::move(timestamps), std::move(values));

        ExpressionEvaluator::QueryResultMap queryResults;
        queryResults[queryRef] = std::move(series);

        try {
            auto evaluated = evaluator.evaluate(formula, queryResults);

            for (size_t i = 0; i < evaluated.size(); ++i) {
                StreamingDataPoint pt;
                pt.measurement = key.measurement;
                pt.tags = key.tags;
                pt.field = key.field;
                pt.timestamp = (*evaluated.timestamps)[i];
                pt.value = evaluated.values[i];
                result.points.push_back(std::move(pt));
            }
        } catch (const EvaluationException&) {
            // If evaluation fails for this series, skip it
        }
    }

    return result;
}

// --- Route registration ---

// Custom handler that doesn't override Content-Type after write_body
class sse_handler : public seastar::httpd::handler_base {
    HttpStreamHandler* _parent;

public:
    explicit sse_handler(HttpStreamHandler* parent) : _parent(parent) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring&,
                                                                  std::unique_ptr<seastar::http::request> req,
                                                                  std::unique_ptr<seastar::http::reply>) override {
        return _parent->handleSubscribe(std::move(req)).then([](std::unique_ptr<seastar::http::reply> rep) {
            // Call done() without a type parameter to avoid overwriting
            // the Content-Type already set by write_body
            rep->done();
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
};

// Handler for GET /subscriptions monitoring endpoint
class subscriptions_handler : public seastar::httpd::handler_base {
    HttpStreamHandler* _parent;

public:
    explicit subscriptions_handler(HttpStreamHandler* parent) : _parent(parent) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring&,
                                                                  std::unique_ptr<seastar::http::request> req,
                                                                  std::unique_ptr<seastar::http::reply>) override {
        return _parent->handleGetSubscriptions(std::move(req));
    }
};

void HttpStreamHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    // seastar::httpd::routes takes ownership of raw handler pointers and
    // deletes them in its destructor — raw new is the required Seastar API.
    if (authToken.empty()) {
        r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/subscribe"), new sse_handler(this));
        r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/subscriptions"),
              new subscriptions_handler(this));
    } else {
        std::string token(authToken);
        r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/subscribe"),
              new timestar::AuthHandlerWrapper(
                  std::make_unique<sse_handler>(this), token));
        r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/subscriptions"),
              new timestar::AuthHandlerWrapper(
                  std::make_unique<subscriptions_handler>(this), token));
    }

    timestar::http_log.info("Registered HTTP streaming endpoints at /subscribe, /subscriptions{}",
                            authToken.empty() ? "" : " (auth required)");
}

// --- Main subscribe handler ---

// Holds parsed per-query info for multi-query subscriptions.
struct QueryEntry {
    std::string label;
    QueryRequest queryReq;
    uint64_t subId = 0;
};

seastar::future<std::unique_ptr<seastar::http::reply>> HttpStreamHandler::handleSubscribe(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();

    // Body size limit to prevent DoS via large payloads
    if (req->content.size() > timestar::config().http.max_query_body_size) {
        rep->set_status(seastar::http::reply::status_type::payload_too_large);
        rep->_content = R"({"status":"error","error":"Request body too large"})";
        rep->add_header("Content-Type", "application/json");
        co_return rep;
    }

    // Parse JSON body
    GlazeSubscribeRequest glazeReq;
    auto parseErr = glz::read_json(glazeReq, req->content);
    if (parseErr) {
        rep->set_status(seastar::http::reply::status_type::bad_request);
        rep->_content =
            "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_JSON\",\"message\":\"Failed to parse subscribe "
            "request\"}}";
        rep->add_header("Content-Type", "application/json");
        co_return rep;
    }

    // Normalize single-query and multi-query into a unified list of QueryEntry
    std::vector<QueryEntry> queryEntries;

    if (!glazeReq.queries.empty() && !glazeReq.query.empty()) {
        // Both specified — reject ambiguous request
        rep->set_status(seastar::http::reply::status_type::bad_request);
        rep->_content =
            "{\"status\":\"error\",\"error\":{\"code\":\"AMBIGUOUS_REQUEST\",\"message\":\"Specify either 'query' or "
            "'queries', not both\"}}";
        rep->add_header("Content-Type", "application/json");
        co_return rep;
    }

    constexpr size_t MAX_QUERIES_PER_SUBSCRIPTION = 100;

    if (!glazeReq.queries.empty()) {
        // Reject excessively large query lists to prevent resource exhaustion
        if (glazeReq.queries.size() > MAX_QUERIES_PER_SUBSCRIPTION) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content =
                "{\"status\":\"error\",\"error\":{\"code\":\"TOO_MANY_QUERIES\",\"message\":\"Too many queries (max "
                "100)\"}}";
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        // Multi-query mode
        for (size_t i = 0; i < glazeReq.queries.size(); ++i) {
            const auto& qe = glazeReq.queries[i];
            QueryEntry entry;
            entry.label = qe.label.empty() ? ("q" + std::to_string(i)) : qe.label;
            try {
                entry.queryReq = QueryParser::parseQueryString(qe.query);
            } catch (const QueryParseException& e) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_QUERY\",\"message\":\"Query '" +
                                jsonEscape(entry.label) + "': " + jsonEscape(e.what()) + "\"}}";
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }
            queryEntries.push_back(std::move(entry));
        }
    } else if (!glazeReq.query.empty()) {
        // Single-query mode (backward-compatible)
        QueryEntry entry;
        // No label for single-query mode (keeps existing behavior)
        try {
            entry.queryReq = QueryParser::parseQueryString(glazeReq.query);
        } catch (const QueryParseException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_QUERY\",\"message\":\"" +
                            jsonEscape(e.what()) + "\"}}";
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        queryEntries.push_back(std::move(entry));
    } else {
        rep->set_status(seastar::http::reply::status_type::bad_request);
        rep->_content =
            "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_QUERY\",\"message\":\"Either 'query' or 'queries' "
            "must be provided\"}}";
        rep->add_header("Content-Type", "application/json");
        co_return rep;
    }

    // Parse aggregation interval (0 = no aggregation, emit raw points)
    uint64_t aggIntervalNs = 0;
    if (glazeReq.aggregationInterval) {
        std::visit(
            [&aggIntervalNs](const auto& val) {
                using VT = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<VT, uint64_t>) {
                    aggIntervalNs = val;
                } else {
                    try {
                        aggIntervalNs = HttpQueryHandler::parseInterval(val);
                    } catch (...) { /* leave as 0 */
                    }
                }
            },
            *glazeReq.aggregationInterval);
    }

    // Parse and validate formula (if provided)
    std::shared_ptr<ExpressionNode> formulaAst;
    if (!glazeReq.formula.empty()) {
        if (aggIntervalNs == 0) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content =
                "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_QUERY\","
                "\"message\":\"aggregationInterval is required when formula is set\"}}";
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        try {
            ExpressionParser parser(glazeReq.formula);
            auto ast = parser.parse();
            if (ast->type == ExprNodeType::ANOMALY_FUNCTION || ast->type == ExprNodeType::FORECAST_FUNCTION) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content =
                    "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_QUERY\","
                    "\"message\":\"anomalies() and forecast() are not supported in streaming subscriptions\"}}";
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }

            // Validate query references against available queries
            auto refs = parser.getQueryReferences();
            if (queryEntries.size() == 1) {
                // Single-query mode: only "a" is a valid query reference
                for (const auto& ref : refs) {
                    if (ref != "a") {
                        rep->set_status(seastar::http::reply::status_type::bad_request);
                        rep->_content =
                            "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_FORMULA\","
                            "\"message\":\"Formula references undefined query '" +
                            jsonEscape(ref) +
                            "'. "
                            "Single-query subscriptions only support 'a' as the query reference.\"}}";
                        rep->add_header("Content-Type", "application/json");
                        co_return rep;
                    }
                }
            } else {
                // Multi-query mode: refs must match query labels
                std::set<std::string> validLabels;
                for (const auto& entry : queryEntries) {
                    validLabels.insert(entry.label);
                }
                for (const auto& ref : refs) {
                    if (validLabels.find(ref) == validLabels.end()) {
                        rep->set_status(seastar::http::reply::status_type::bad_request);
                        auto availLabels = [&validLabels]() {
                            std::string s;
                            for (const auto& l : validLabels) {
                                if (!s.empty())
                                    s += ", ";
                                s += jsonEscape(l);
                            }
                            return s;
                        }();
                        rep->_content =
                            "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_FORMULA\","
                            "\"message\":\"Formula references undefined query '" +
                            jsonEscape(ref) +
                            "'. "
                            "Available labels: " +
                            availLabels + "\"}}";
                        rep->add_header("Content-Type", "application/json");
                        co_return rep;
                    }
                }
            }

            formulaAst = std::move(ast);
        } catch (const ExpressionParseException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_FORMULA\",\"message\":\"" +
                            jsonEscape(e.what()) + "\"}}";
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
    }

    const auto& streamCfg = timestar::config().streaming;

    // Heuristic pre-check: fast-fail if we're clearly over the limit.
    // The authoritative enforcement is inside addSubscription() which is
    // race-free on a single Seastar shard (no interleaving between the
    // check and the insert).
    auto& localMgr = _engineSharded->local().getSubscriptionManager();
    localMgr.setMaxLocalSubscriptions(streamCfg.max_subscriptions_per_shard);
    if (localMgr.localSubscriptionCount() + queryEntries.size() > streamCfg.max_subscriptions_per_shard) {
        rep->set_status(seastar::http::reply::status_type{429});
        rep->_content =
            "{\"status\":\"error\",\"error\":{\"code\":\"TOO_MANY_SUBSCRIPTIONS\","
            "\"message\":\"Maximum subscriptions per shard exceeded\"}}";
        rep->add_header("Content-Type", "application/json");
        co_return rep;
    }

    // Create the shared output queue
    auto outputQueue =
        std::make_unique<seastar::queue<std::shared_ptr<const StreamingBatch>>>(streamCfg.output_queue_size);
    auto* queuePtr = outputQueue.get();

    unsigned thisShard = seastar::this_shard_id();
    unsigned shardCount = seastar::smp::count;

    // Register a subscription for each query entry
    std::vector<uint64_t> allSubIds;
    for (auto& entry : queryEntries) {
        Subscription sub;
        sub.id = localMgr.allocateId();
        sub.measurement = entry.queryReq.measurement;
        sub.scopes = entry.queryReq.scopes;
        sub.fields = entry.queryReq.fields;
        sub.aggregation = entry.queryReq.aggregation;
        sub.handlerShard = thisShard;
        sub.label = entry.label;
        entry.subId = sub.id;
        allSubIds.push_back(sub.id);

        // Register on handler shard with queue.
        // addSubscription throws std::runtime_error if the per-shard limit
        // is exceeded (authoritative, race-free check), or
        // std::invalid_argument if any scope pattern is an invalid regex.
        std::string subscriptionError;
        bool limitExceeded = false;
        try {
            localMgr.addSubscription(sub);
        } catch (const std::runtime_error& e) {
            subscriptionError = e.what();
            limitExceeded = true;
            allSubIds.pop_back();
        } catch (const std::exception& e) {
            subscriptionError = e.what();
            // Remove the sub ID we just failed to register (wasn't added)
            allSubIds.pop_back();
        }
        if (!subscriptionError.empty()) {
            // Clean up all previously registered subscriptions
            for (auto id : allSubIds) {
                localMgr.removeSubscription(id);
            }
            co_await _engineSharded->invoke_on_all([allSubIds, thisShard](Engine& engine) {
                if (seastar::this_shard_id() == thisShard)
                    return;
                for (auto id : allSubIds) {
                    engine.getSubscriptionManager().removeSubscription(id);
                }
            });
            if (limitExceeded) {
                rep->set_status(seastar::http::reply::status_type{429});
                rep->_content =
                    "{\"status\":\"error\",\"error\":{\"code\":\"TOO_MANY_SUBSCRIPTIONS\","
                    "\"message\":\"" + jsonEscape(subscriptionError) + "\"}}";
            } else {
                rep->set_status(seastar::http::reply::status_type{400});
                rep->_content = std::string(
                                    "{\"status\":\"error\",\"error\":{\"code\":\"INVALID_SCOPE_PATTERN\","
                                    "\"message\":\"") +
                                jsonEscape(subscriptionError) + "\"}}";
            }
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        localMgr.registerQueue(sub.id, queuePtr);

        // Register filter criteria on all other shards in parallel
        co_await _engineSharded->invoke_on_all([sub, thisShard](Engine& engine) {
            if (seastar::this_shard_id() == thisShard)
                return;
            engine.getSubscriptionManager().addSubscription(sub);
        });
    }

    timestar::http_log.info("[SUBSCRIBE] {} subscriptions registered on all {} shards (first id={})",
                            queryEntries.size(), shardCount, allSubIds.empty() ? 0 : allSubIds.front());

    // --- Backfill: execute queries for historical data AFTER subscription registration ---
    std::vector<StreamingBatch> backfillBatches;
    constexpr size_t MAX_BACKFILL_POINTS = 1000000;

    if (glazeReq.backfill) {
        uint64_t backfillStart = parseStartTime(glazeReq.startTime);
        auto now = std::chrono::system_clock::now();
        uint64_t backfillEnd = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

        HttpQueryHandler backfillHandler(_engineSharded);

        size_t totalBackfillPoints = 0;
        auto backfillTimeoutSeconds = HttpQueryHandler::defaultQueryTimeout();
        for (const auto& entry : queryEntries) {
            if (totalBackfillPoints >= MAX_BACKFILL_POINTS) {
                timestar::http_log.warn(
                    "[SUBSCRIBE] Backfill point limit reached ({} points); "
                    "skipping remaining queries",
                    totalBackfillPoints);
                break;
            }

            QueryRequest backfillReq = entry.queryReq;
            backfillReq.startTime = backfillStart;
            backfillReq.endTime = backfillEnd;

            try {
                auto deadline = seastar::lowres_clock::now() + backfillTimeoutSeconds;
                auto response = co_await seastar::with_timeout(deadline, backfillHandler.executeQuery(backfillReq));
                if (response.success && !response.series.empty()) {
                    auto batches = queryResponseToBatches(response.series, entry.label);
                    for (auto& b : batches) {
                        totalBackfillPoints += b.points.size();
                        backfillBatches.push_back(std::move(b));
                        if (totalBackfillPoints >= MAX_BACKFILL_POINTS) {
                            timestar::http_log.warn(
                                "[SUBSCRIBE] Backfill point limit reached ({} points); "
                                "truncating results",
                                totalBackfillPoints);
                            break;
                        }
                    }
                }
            } catch (seastar::timed_out_error&) {
                timestar::http_log.warn(
                    "[SUBSCRIBE] Backfill timed out after {}s for label '{}'; "
                    "subscription remains active for live data",
                    backfillTimeoutSeconds.count(), entry.label);
            } catch (const std::exception& e) {
                timestar::http_log.warn("[SUBSCRIBE] Backfill failed for label '{}': {}", entry.label, e.what());
            }
        }

        if (!backfillBatches.empty()) {
            timestar::http_log.info("[SUBSCRIBE] Backfill: {} batches across {} queries", backfillBatches.size(),
                                    queryEntries.size());
        }
    }

    // Build per-label aggregation method map for the body writer
    // (each query can have its own aggregation method)
    std::map<std::string, AggregationMethod> labelAggMethods;
    for (const auto& entry : queryEntries) {
        labelAggMethods[entry.label] = entry.queryReq.aggregation;
    }

    // Set SSE headers and start streaming via chunked transfer
    rep->add_header("Cache-Control", "no-cache");
    rep->add_header("Connection", "keep-alive");
    rep->add_header("X-Subscription-Ids", [&allSubIds]() {
        std::string ids;
        ids.reserve(allSubIds.size() * 8);
        for (size_t i = 0; i < allSubIds.size(); ++i) {
            if (i > 0)
                ids += ',';
            appendUint64(ids, allSubIds[i]);
        }
        return ids;
    }());
    rep->set_status(seastar::http::reply::status_type::ok);

    unsigned heartbeatSec = streamCfg.heartbeat_interval_seconds;

    // Cross-query derived mode: multi-query + formula
    bool useDerivedEvaluator = formulaAst && queryEntries.size() > 1;

    rep->write_body(
        "text/event-stream",
        [queuePtr, allSubIds = std::move(allSubIds), thisShard, shardCount, outputQueue = std::move(outputQueue),
         engineSharded = this->_engineSharded, backfillBatches = std::move(backfillBatches), aggIntervalNs,
         labelAggMethods = std::move(labelAggMethods), formulaAst, useDerivedEvaluator, heartbeatSec,
         gateHolder = _connectionGate.hold()](seastar::output_stream<char>&& os) mutable -> seastar::future<> {
            auto out = std::move(os);

            // Cancellation flag: set true in catch block so timer callbacks
            // can guard against dereferencing queuePtr after it is destroyed.
            // Declared outside try/catch so both blocks share the same instance.
            auto cancelled = std::make_shared<bool>(false);

            try {
                // Send initial SSE retry interval
                co_await out.write("retry: 5000\n\n");
                co_await out.flush();

                // Send backfill events first (if any)
                uint64_t seqId = 0;
                for (auto& batch : backfillBatches) {
                    batch.sequenceId = seqId++;
                    if (formulaAst) {
                        batch = applyFormulaToBatch(batch, *formulaAst);
                    }
                    std::string event = HttpStreamHandler::formatSSEBackfillEvent(batch);
                    co_await out.write(event);
                    co_await out.flush();
                }
                backfillBatches.clear();

                // Set up heartbeat timer
                bool heartbeatDue = false;
                seastar::timer<seastar::lowres_clock> heartbeat;
                heartbeat.set_callback([&heartbeatDue, queuePtr, cancelled] {
                    if (*cancelled)
                        return;
                    heartbeatDue = true;
                    // Push empty sentinel to wake pop_eventually()
                    if (!queuePtr->full()) {
                        queuePtr->push(std::make_shared<const StreamingBatch>());
                    }
                });
                heartbeat.arm_periodic(std::chrono::seconds(heartbeatSec));

                // Set up aggregation: either derived evaluator (cross-query) or
                // per-label aggregators (single-query / independent multi-query)
                std::map<std::string, std::unique_ptr<StreamingAggregator>> aggregators;
                std::unique_ptr<StreamingDerivedEvaluator> derivedEval;
                bool aggEmitDue = false;
                seastar::timer<seastar::lowres_clock> aggTimer;

                if (aggIntervalNs > 0) {
                    if (useDerivedEvaluator) {
                        derivedEval =
                            std::make_unique<StreamingDerivedEvaluator>(aggIntervalNs, labelAggMethods, formulaAst);
                    } else {
                        for (const auto& [label, method] : labelAggMethods) {
                            aggregators[label] = std::make_unique<StreamingAggregator>(aggIntervalNs, method);
                        }
                    }
                    aggTimer.set_callback([&aggEmitDue, queuePtr, cancelled] {
                        if (*cancelled)
                            return;
                        aggEmitDue = true;
                        // Push empty sentinel to wake pop_eventually()
                        if (!queuePtr->full()) {
                            queuePtr->push(std::make_shared<const StreamingBatch>());
                        }
                    });
                    auto intervalMs = std::max(uint64_t(100), aggIntervalNs / 1000000);
                    aggTimer.arm_periodic(std::chrono::milliseconds(intervalMs));
                }

                bool hasAggregation = !aggregators.empty() || derivedEval;

                // Safety: [&] captures coroutine-frame locals by reference. This is safe
                // because processBatch is only ever co_awaited inline within this coroutine
                // frame and is never stored, passed to another coroutine, or detached.
                // Do NOT change this to a detached or stored lambda.
                auto processBatch = [&](const StreamingBatch& batch) -> seastar::future<> {
                    if (batch.isDrop) {
                        // Emit a drop notification event to the client
                        std::string event;
                        event.reserve(96);
                        event += "id: ";
                        appendUint64(event, batch.sequenceId);
                        event += "\nevent: drop\ndata: {\"droppedPoints\":";
                        appendUint64(event, batch.droppedCount);
                        if (!batch.label.empty()) {
                            event += ",\"label\":\"";
                            jsonEscapeAppend(batch.label, event);
                            event += '"';
                        }
                        event += "}\n\n";
                        co_await out.write(event);
                        co_await out.flush();
                        co_return;
                    }
                    if (batch.points.empty())
                        co_return;  // Skip empty sentinel batches
                    if (derivedEval) {
                        // Cross-query derived: route by label
                        std::string label = batch.label;
                        if (label.empty() && labelAggMethods.size() == 1) {
                            label = labelAggMethods.begin()->first;
                        }
                        for (const auto& pt : batch.points) {
                            derivedEval->addPoint(label, pt);
                        }
                    } else if (!aggregators.empty()) {
                        auto it = aggregators.find(batch.label);
                        auto* agg = (it != aggregators.end()) ? it->second.get() : nullptr;
                        // For single-query (empty label), use first aggregator
                        if (!agg && batch.label.empty() && aggregators.size() == 1) {
                            agg = aggregators.begin()->second.get();
                        }
                        if (agg) {
                            for (const auto& pt : batch.points) {
                                agg->addPoint(pt);
                            }
                        }
                    } else {
                        // sequenceId was already stamped by deliverBatch on the manager side
                        std::string event = HttpStreamHandler::formatSSEEvent(batch);
                        co_await out.write(event);
                        co_await out.flush();
                    }
                };

                // Streaming loop
                while (true) {
                    // Check if aggregation emission is due
                    if (hasAggregation && aggEmitDue) {
                        aggEmitDue = false;
                        uint64_t nowNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                   std::chrono::system_clock::now().time_since_epoch())
                                                                   .count());
                        if (derivedEval) {
                            // Cross-query: evaluate formula across all queries
                            auto aggBatch = derivedEval->closeBuckets(nowNs);
                            if (!aggBatch.points.empty()) {
                                aggBatch.sequenceId = seqId++;
                                std::string event = HttpStreamHandler::formatSSEEvent(aggBatch);
                                co_await out.write(event);
                                co_await out.flush();
                            }
                        } else {
                            for (auto& [label, agg] : aggregators) {
                                auto aggBatch = agg->closeBuckets(nowNs);
                                if (aggBatch.points.empty())
                                    continue;
                                aggBatch.label = label;
                                aggBatch.sequenceId = seqId++;
                                if (formulaAst) {
                                    aggBatch = applyFormulaToBatch(aggBatch, *formulaAst);
                                }
                                if (!aggBatch.points.empty()) {
                                    std::string event = HttpStreamHandler::formatSSEEvent(aggBatch);
                                    co_await out.write(event);
                                    co_await out.flush();
                                }
                            }
                        }
                    }

                    // Send heartbeat if timer fired (check unconditionally — busy streams need it too)
                    if (heartbeatDue) {
                        heartbeatDue = false;
                        co_await out.write(":heartbeat\n\n");
                        co_await out.flush();
                    }

                    // Wait for data
                    if (queuePtr->empty()) {
                        auto batchPtr = co_await queuePtr->pop_eventually();
                        co_await processBatch(*batchPtr);
                    } else {
                        auto batchPtr = queuePtr->pop();
                        co_await processBatch(*batchPtr);
                    }
                }
            } catch (...) {
                *cancelled = true;
                timestar::http_log.info("[SUBSCRIBE] Subscription group disconnected (first id={})",
                                        allSubIds.empty() ? 0 : allSubIds.front());
            }

            // Unregister all subscriptions from all shards in parallel.
            // invoke_on_all fans out to every shard concurrently, reducing cleanup
            // from O(N*S) serial RPCs to O(N) parallel rounds.
            for (uint64_t subId : allSubIds) {
                try {
                    co_await engineSharded->invoke_on_all(
                        [subId](Engine& engine) { engine.getSubscriptionManager().removeSubscription(subId); });
                } catch (...) {
                    // Best-effort: log and continue with remaining subscriptions
                    timestar::http_log.warn("[SUBSCRIBE] Error during cleanup of subscription {}", subId);
                }
            }

            timestar::http_log.info("[SUBSCRIBE] {} subscriptions cleaned up from all shards in parallel",
                                    allSubIds.size());

            co_await out.close();
        });

    co_return rep;
}

// --- Monitoring endpoint: GET /subscriptions ---

seastar::future<std::unique_ptr<seastar::http::reply>> HttpStreamHandler::handleGetSubscriptions(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();

    // Collect subscription stats from all shards in parallel
    auto allStats = co_await _engineSharded->map_reduce0(
        [](Engine& engine) { return engine.getSubscriptionManager().getStats(); }, std::vector<SubscriptionStats>{},
        [](std::vector<SubscriptionStats> acc, std::vector<SubscriptionStats> shardStats) {
            acc.insert(acc.end(), std::make_move_iterator(shardStats.begin()),
                       std::make_move_iterator(shardStats.end()));
            return acc;
        });

    // Build JSON response manually
    std::string json;
    json.reserve(256 + allStats.size() * 256);
    json += "{\"subscriptions\":[";

    for (size_t i = 0; i < allStats.size(); ++i) {
        if (i > 0)
            json += ',';
        const auto& s = allStats[i];

        json += "{\"id\":";
        appendUint64(json, s.id);
        if (!s.label.empty()) {
            json += ",\"label\":\"";
            jsonEscapeAppend(s.label, json);
            json += '"';
        }
        json += ",\"measurement\":\"";
        jsonEscapeAppend(s.measurement, json);
        json += "\",\"fields\":[";
        for (size_t f = 0; f < s.fields.size(); ++f) {
            if (f > 0)
                json += ',';
            json += '"';
            jsonEscapeAppend(s.fields[f], json);
            json += '"';
        }
        json += "],\"scopes\":{";
        bool firstScope = true;
        for (const auto& [k, v] : s.scopes) {
            if (!firstScope)
                json += ',';
            firstScope = false;
            json += '"';
            jsonEscapeAppend(k, json);
            json += "\":\"";
            jsonEscapeAppend(v, json);
            json += '"';
        }
        json += "},\"handler_shard\":";
        appendUint64(json, s.handlerShard);
        json += ",\"queue_depth\":";
        appendUint64(json, s.queueDepth);
        json += ",\"queue_capacity\":";
        appendUint64(json, s.queueCapacity);
        json += ",\"dropped_points\":";
        appendUint64(json, s.droppedPoints);
        json += ",\"events_sent\":";
        appendUint64(json, s.eventsSent);
        json += '}';
    }

    json += "],\"total_subscriptions\":";
    appendUint64(json, allStats.size());
    json += '}';

    rep->set_status(seastar::http::reply::status_type::ok);
    rep->_content = std::move(json);
    rep->add_header("Content-Type", "application/json");
    rep->done();
    co_return rep;
}

}  // namespace timestar

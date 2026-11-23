#include "response_formatter.hpp"

#include "http_error.hpp"
#include "short_decimal.hpp"
#include "yield_policy.hpp"

// Glaze's optimised number formatters:
//   itoa: digit-pair lookup tables + 128-bit mul-shift division  (~2x std::to_chars)
//   zmij: shortest round-trip float text (glaze's port of vitaut/zmij)
#include <glaze/util/itoa.hpp>
#include <glaze/util/zmij.hpp>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <utility>
#include <vector>

namespace timestar::http {

// ──────────────────────────────────────────────────────────────────────
// Buffer helper — direct char* writes, no per-element string::append()
// ──────────────────────────────────────────────────────────────────────
//
// The body is built directly in a seastar::sstring, because that is the type
// http::reply carries. Building it in a std::string and assigning that to the
// reply copied the whole body once more — 27 MB for a million-point response,
// with both copies alive at once.
//
// sstring has no spare capacity of its own, so BodyBuffer keeps one sized to
// its capacity and tracks the written length separately; nothing past that
// length is ever read, and take() trims to it in place. The storage is
// uninitialized: no zero-fill of worst-case capacity that the serializer
// would only overwrite.

namespace {

// Longest text each numeric type can produce. These bound the up-front
// reservation, so they are exact rather than generous: a query response holds
// this much per point until it is sent.
//   uint64 / int64: 20 characters (20 digits, or a sign and 19); the integer
//           writer touches nothing past what it returns.
//   double: 24 — "-1.5131402663111935E-222"; fixed notation is shorter and
//           NaN/Inf print "null".
// (Short decimals take writeShortDecimal instead: the same text, at most 23
// characters, nothing touched beyond them.)
// The double writer (glaze's zmij port) additionally uses the bytes after the
// value as scratch: it may touch up to glz::zmij::double_buffer_size bytes
// from where the value starts, whatever the final length. Values are written
// back to back, so only the LAST value of a slice needs that scratch reserved
// beyond its text — SLICE_SLACK covers it.
// The lengths were confirmed by formatting ~400M random and extreme values
// against a canary-filled buffer (observed scratch reach: 27 bytes); the
// scratch figure used here is the writer's documented contract, not the
// observation.
static constexpr size_t MAX_INT_LEN = 20;
static constexpr size_t MAX_DOUBLE_LEN = 24;
static constexpr size_t DOUBLE_SCRATCH = glz::zmij::double_buffer_size;
static constexpr size_t SLICE_SLACK = 16;
static_assert(DOUBLE_SCRATCH <= MAX_DOUBLE_LEN + 1 + SLICE_SLACK,
              "a slice's reservation must cover the scratch of its final double");

// Where the body is written. Formatting code asks for worst-case room at an
// offset and gets a raw pointer back; the two implementations differ only in
// what happens when the current storage is full, which is the cold path.
//
// `pos` is always an offset into the CURRENT storage: a contiguous body has
// one, so it is the body length so far; a streamed body starts a new segment
// (offset 0) whenever one fills.
class BodyBuffer {
public:
    virtual ~BodyBuffer() = default;

    // Writable storage for `maxBytes` at `pos`. May switch storage, in which
    // case `pos` is updated to the offset to write at.
    char* reserveAt(size_t& pos, size_t maxBytes) {
        if (pos + maxBytes > capacity_) [[unlikely]] {
            makeRoom(pos, maxBytes);
        }
        return base_ + pos;
    }

    size_t offsetOf(const char* p) const { return static_cast<size_t>(p - base_); }

    // Finished segments are waiting to be handed to the connection.
    bool hasSealed() const { return hasSealed_; }
    virtual seastar::future<> drain() { return seastar::make_ready_future<>(); }

protected:
    virtual void makeRoom(size_t& pos, size_t maxBytes) = 0;

    char* base_ = nullptr;
    size_t capacity_ = 0;
    bool hasSealed_ = false;
};

// The whole body in one string, grown by doubling.
class ContiguousBody final : public BodyBuffer {
public:
    explicit ContiguousBody(size_t capacity) : storage_(seastar::sstring::initialized_later(), capacity) {
        base_ = storage_.data();
        capacity_ = storage_.size();
    }

    // The finished body: exactly the `length` bytes written.
    seastar::sstring take(size_t length) {
        storage_.resize(length);
        base_ = nullptr;
        capacity_ = 0;
        return std::move(storage_);
    }

protected:
    void makeRoom(size_t& pos, size_t maxBytes) override {
        seastar::sstring grown(seastar::sstring::initialized_later(), std::max(pos + maxBytes, storage_.size() * 2));
        std::memcpy(grown.data(), storage_.data(), pos);
        storage_ = std::move(grown);
        base_ = storage_.data();
        capacity_ = storage_.size();
    }

private:
    seastar::sstring storage_;
};

// The body as a sequence of segments handed to an output stream as they fill.
// A filled segment is given to the stream as a buffer, not copied into it, so
// the body is never held — or copied — whole: memory in flight is a couple of
// segments however large the result is, and the client's read speed applies
// backpressure through drain().
class StreamedBody final : public BodyBuffer {
public:
    // Large enough that one array slice's worst-case reservation
    // (SLICE_ELEMENTS doubles, ~205KB) leaves a segment mostly full.
    static constexpr size_t kSegmentBytes = 512 * 1024;

    explicit StreamedBody(seastar::output_stream<char>& out) : out_(out) {}

    seastar::future<> drain() override {
        auto sealed = std::exchange(sealed_, {});
        hasSealed_ = false;
        for (auto& segment : sealed) {
            co_await out_.write(std::move(segment));
        }
    }

    // Hand over everything, including the partly filled current segment.
    seastar::future<> finish(size_t pos) {
        seal(pos);
        return drain();
    }

protected:
    void makeRoom(size_t& pos, size_t maxBytes) override {
        seal(pos);
        segment_ = seastar::temporary_buffer<char>(std::max(maxBytes, kSegmentBytes));
        base_ = segment_.get_write();
        capacity_ = segment_.size();
        pos = 0;
    }

private:
    void seal(size_t pos) {
        if (pos > 0) {
            segment_.trim(pos);
            sealed_.push_back(std::move(segment_));
            hasSealed_ = true;
        }
        segment_ = {};
        base_ = nullptr;
        capacity_ = 0;
    }

    seastar::output_stream<char>& out_;
    seastar::temporary_buffer<char> segment_;
    std::vector<seastar::temporary_buffer<char>> sealed_;
};

// Writers must not throw, must stay within maxBytes, and must return the end
// of what they wrote. Returns the new write offset.
template <class Writer>
inline size_t appendUninitialized(BodyBuffer& buf, size_t pos, size_t maxBytes, Writer writer) {
    char* start = buf.reserveAt(pos, maxBytes);
    return buf.offsetOf(writer(start));
}

// ── JSON string helpers ─────────────────────────────────────────────

// Returns true if `sv` contains a character that requires JSON escaping.
// Measurement names, tag keys/values, and field names in TSDB data are
// almost always plain ASCII identifiers — this lets us skip per-char
// processing for the common case.
inline bool needsEscaping(std::string_view sv) noexcept {
    for (char c : sv) {
        if (static_cast<unsigned char>(c) < 0x20 || c == '"' || c == '\\') [[unlikely]] {
            return true;
        }
    }
    return false;
}

// Append a JSON-escaped string to `buf` starting at position `pos`.
// Returns the new position.
inline size_t appendJsonStringEscaped(BodyBuffer& buf, size_t pos, std::string_view sv) {
    // Worst case: every char becomes \uXXXX plus the two quotes.
    return appendUninitialized(buf, pos, sv.size() * 6 + 2, [sv](char* p) {
        *p++ = '"';
        for (char c : sv) {
            switch (c) {
                case '"':
                    *p++ = '\\';
                    *p++ = '"';
                    break;
                case '\\':
                    *p++ = '\\';
                    *p++ = '\\';
                    break;
                case '\n':
                    *p++ = '\\';
                    *p++ = 'n';
                    break;
                case '\r':
                    *p++ = '\\';
                    *p++ = 'r';
                    break;
                case '\t':
                    *p++ = '\\';
                    *p++ = 't';
                    break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        // \uXXXX
                        static constexpr char hex[] = "0123456789abcdef";
                        *p++ = '\\';
                        *p++ = 'u';
                        *p++ = '0';
                        *p++ = '0';
                        *p++ = hex[(static_cast<unsigned char>(c) >> 4) & 0xf];
                        *p++ = hex[static_cast<unsigned char>(c) & 0xf];
                    } else {
                        *p++ = c;
                    }
            }
        }
        *p++ = '"';
        return p;
    });
}

// Append a JSON string to `buf` at position `pos`.
// Fast path: if no escaping needed, direct memcpy.
inline size_t appendJsonString(BodyBuffer& buf, size_t pos, std::string_view sv) {
    if (!needsEscaping(sv)) [[likely]] {
        return appendUninitialized(buf, pos, sv.size() + 2, [sv](char* p) {
            *p++ = '"';
            std::memcpy(p, sv.data(), sv.size());
            p += sv.size();
            *p++ = '"';
            return p;
        });
    }
    return appendJsonStringEscaped(buf, pos, sv);
}

// Append raw bytes (no escaping).
inline size_t appendRaw(BodyBuffer& buf, size_t pos, std::string_view sv) {
    return appendUninitialized(buf, pos, sv.size(), [sv](char* p) {
        std::memcpy(p, sv.data(), sv.size());
        return p + sv.size();
    });
}

inline size_t appendChar(BodyBuffer& buf, size_t pos, char c) {
    return appendUninitialized(buf, pos, 1, [c](char* p) {
        *p++ = c;
        return p;
    });
}

// ── Array serialisers ────────────────────────────────────────────────
//
// Arrays are written in element slices so the yielding format() coroutine can
// hand the reactor back between slices — a single multi-million-point series
// must not serialise as one task (observed as 124ms reactor stalls).  Each
// slice writer emits elements [from, to), comma-joined, with a leading comma
// when from > 0; the '[' / ']' brackets are written by the caller.

// Elements per slice. Between slices the coroutine offers the reactor a yield
// and, for a streamed body, hands finished segments to the connection. Well
// under the shared yield chunk (lib/core/yield_policy.hpp) so that a slice's
// worst-case reservation fits inside one StreamedBody segment.
static constexpr size_t SLICE_ELEMENTS = 8192;
static_assert(SLICE_ELEMENTS <= kYieldChunkPoints);
static_assert(SLICE_ELEMENTS * (MAX_DOUBLE_LEN + 1) + SLICE_SLACK <= StreamedBody::kSegmentBytes / 2);

// Write a uint64_t slice using glz::to_chars (digit-pair tables).
inline size_t appendUint64Slice(BodyBuffer& buf, size_t pos, const std::vector<uint64_t>& arr, size_t from, size_t to) {
    return appendUninitialized(buf, pos, (to - from) * (MAX_INT_LEN + 1) + SLICE_SLACK, [&](char* p) {
        for (size_t i = from; i < to; ++i) {
            if (i > 0)
                *p++ = ',';
            p = glz::to_chars(p, arr[i]);
        }
        return p;
    });
}

// Write a double slice. NaN and Inf are written as JSON null.
//
// Most metric values are short decimals, which writeShortDecimal turns into
// the same text glz::to_chars would at a third of the cost; everything else
// takes the general writer (zmij shortest round-trip). A series is usually
// one kind or the other, so after a run of values the fast path declines, it
// is left alone for a stretch rather than tried and failed on every value.
inline size_t appendDoubleSlice(BodyBuffer& buf, size_t pos, const std::vector<double>& arr, size_t from, size_t to) {
    return appendUninitialized(buf, pos, (to - from) * (MAX_DOUBLE_LEN + 1) + SLICE_SLACK, [&](char* p) {
        constexpr unsigned kDeclinesBeforeBackoff = 8;
        constexpr unsigned kBackoffValues = 256;
        unsigned declines = 0;
        unsigned backoff = 0;
        for (size_t i = from; i < to; ++i) {
            if (i > 0)
                *p++ = ',';
            const double v = arr[i];
            if (backoff == 0) {
                if (char* end = writeShortDecimal(p, v)) [[likely]] {
                    p = end;
                    declines = 0;
                    continue;
                }
                if (++declines >= kDeclinesBeforeBackoff) {
                    declines = 0;
                    backoff = kBackoffValues;
                }
            } else {
                --backoff;
            }
            // glz::to_chars writes NaN/Inf as "null" (4 bytes)
            p = glz::to_chars(p, v);
        }
        return p;
    });
}

// Write a bool slice.
inline size_t appendBoolSlice(BodyBuffer& buf, size_t pos, const std::vector<bool>& arr, size_t from, size_t to) {
    return appendUninitialized(buf, pos, (to - from) * 6, [&](char* p) {
        for (size_t i = from; i < to; ++i) {
            if (i > 0)
                *p++ = ',';
            if (arr[i]) {
                std::memcpy(p, "true", 4);
                p += 4;
            } else {
                std::memcpy(p, "false", 5);
                p += 5;
            }
        }
        return p;
    });
}

// Write a string slice.
inline size_t appendStringSlice(BodyBuffer& buf, size_t pos, const std::vector<std::string>& arr, size_t from,
                                size_t to) {
    for (size_t i = from; i < to; ++i) {
        if (i > 0)
            pos = appendChar(buf, pos, ',');
        pos = appendJsonString(buf, pos, arr[i]);
    }
    return pos;
}

// Write an int64_t slice using glz::to_chars.
inline size_t appendInt64Slice(BodyBuffer& buf, size_t pos, const std::vector<int64_t>& arr, size_t from, size_t to) {
    return appendUninitialized(buf, pos, (to - from) * (MAX_INT_LEN + 1) + SLICE_SLACK, [&](char* p) {
        for (size_t i = from; i < to; ++i) {
            if (i > 0)
                *p++ = ',';
            p = glz::to_chars(p, arr[i]);
        }
        return p;
    });
}

// Write a full uint64_t array (brackets included) in yield-bounded slices.
seastar::future<> appendUint64ArrayYielding(BodyBuffer& buf, size_t& pos, const std::vector<uint64_t>& arr) {
    pos = appendChar(buf, pos, '[');
    for (size_t from = 0; from < arr.size(); from += SLICE_ELEMENTS) {
        pos = appendUint64Slice(buf, pos, arr, from, std::min(from + SLICE_ELEMENTS, arr.size()));
        // Slice boundary: pass on whatever segments filled, let the reactor in.
        if (buf.hasSealed()) {
            co_await buf.drain();
        }
        co_await seastar::coroutine::maybe_yield();
    }
    pos = appendChar(buf, pos, ']');
}

// Write full FieldValues (brackets included) in yield-bounded slices.
seastar::future<> appendFieldValuesYielding(BodyBuffer& buf, size_t& pos, const FieldValues& values) {
    pos = appendChar(buf, pos, '[');
    const size_t n = std::visit([](const auto& vec) { return vec.size(); }, values);
    for (size_t from = 0; from < n; from += SLICE_ELEMENTS) {
        const size_t to = std::min(from + SLICE_ELEMENTS, n);
        pos = std::visit(
            [&](const auto& vec) -> size_t {
                using T = std::decay_t<decltype(vec)>;
                if constexpr (std::is_same_v<T, std::vector<double>>)
                    return appendDoubleSlice(buf, pos, vec, from, to);
                else if constexpr (std::is_same_v<T, std::vector<bool>>)
                    return appendBoolSlice(buf, pos, vec, from, to);
                else if constexpr (std::is_same_v<T, std::vector<std::string>>)
                    return appendStringSlice(buf, pos, vec, from, to);
                else if constexpr (std::is_same_v<T, std::vector<int64_t>>)
                    return appendInt64Slice(buf, pos, vec, from, to);
                else
                    static_assert(sizeof(T) == 0,
                                  "unhandled FieldValues alternative — update appendFieldValuesYielding");
            },
            values);
        // Slice boundary: pass on whatever segments filled, let the reactor in.
        if (buf.hasSealed()) {
            co_await buf.drain();
        }
        co_await seastar::coroutine::maybe_yield();
    }
    pos = appendChar(buf, pos, ']');
}

}  // anonymous namespace

// ──────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────

seastar::future<std::string> ResponseFormatter::format(QueryResponse& response) {
    const auto body = co_await formatBody(response);
    co_return std::string(body.data(), body.size());
}

namespace {

// Write the whole success body through `buf`. Returns the final offset in its
// current storage.
seastar::future<size_t> writeBody(BodyBuffer& buf, QueryResponse& response) {
    size_t pos = appendRaw(buf, 0, R"({"status":"success","series":[)");

    bool firstSeries = true;
    for (auto& series : response.series) {
        if (series.fields.empty())
            continue;
        if (!firstSeries)
            pos = appendChar(buf, pos, ',');
        firstSeries = false;

        // {"measurement":"..."
        pos = appendRaw(buf, pos, R"({"measurement":)");
        pos = appendJsonString(buf, pos, series.measurement);

        // ,"groupTags":["key=val",...]
        pos = appendRaw(buf, pos, R"(,"groupTags":[)");
        bool firstTag = true;
        for (const auto& [key, value] : series.tags) {
            if (!firstTag)
                pos = appendChar(buf, pos, ',');
            firstTag = false;
            // Write "key=value" as a JSON string with proper escaping
            if (!needsEscaping(key) && !needsEscaping(value)) [[likely]] {
                // Fast path: no escaping needed
                pos = appendUninitialized(buf, pos, key.size() + value.size() + 3, [&](char* p) {
                    *p++ = '"';
                    std::memcpy(p, key.data(), key.size());
                    p += key.size();
                    *p++ = '=';
                    std::memcpy(p, value.data(), value.size());
                    p += value.size();
                    *p++ = '"';
                    return p;
                });
            } else {
                // Slow path: escape key and/or value
                std::string kv;
                kv.reserve(key.size() + 1 + value.size());
                kv.append(key);
                kv.push_back('=');
                kv.append(value);
                pos = appendJsonString(buf, pos, kv);
            }
        }
        pos = appendChar(buf, pos, ']');

        // ,"fields":{...}
        pos = appendRaw(buf, pos, R"(,"fields":{)");
        bool firstField = true;
        for (auto& [fieldName, fieldData] : series.fields) {
            if (!firstField)
                pos = appendChar(buf, pos, ',');
            firstField = false;

            pos = appendJsonString(buf, pos, fieldName);
            pos = appendRaw(buf, pos, R"(:{"timestamps":)");
            co_await appendUint64ArrayYielding(buf, pos, fieldData.first);
            pos = appendRaw(buf, pos, R"(,"values":)");
            co_await appendFieldValuesYielding(buf, pos, fieldData.second);
            pos = appendChar(buf, pos, '}');
        }
        pos = appendRaw(buf, pos, "}}");
    }

    // ],"statistics":{"series_count":N,"point_count":N,"execution_time_ms":N}}
    pos = appendRaw(buf, pos, R"(],"statistics":{"series_count":)");
    pos = appendUninitialized(buf, pos, 128, [&](char* p) {
        p = glz::to_chars(p, static_cast<int64_t>(response.statistics.seriesCount));
        constexpr std::string_view ptKey = R"(,"point_count":)";
        std::memcpy(p, ptKey.data(), ptKey.size());
        p += ptKey.size();
        p = glz::to_chars(p, static_cast<int64_t>(response.statistics.pointCount));
        constexpr std::string_view execKey = R"(,"execution_time_ms":)";
        std::memcpy(p, execKey.data(), execKey.size());
        p += execKey.size();
        p = glz::to_chars(p, response.statistics.executionTimeMs);
        *p++ = '}';
        *p++ = '}';
        return p;
    });
    co_return pos;
}

}  // anonymous namespace

seastar::future<seastar::sstring> ResponseFormatter::formatBody(QueryResponse& response) {
    // Reserve the numeric worst case — a timestamp and a double per point —
    // but touch only bytes actually written. String fields and metadata can
    // grow beyond this estimate; the buffer doubles when they do. The cap
    // keeps a pathological point count from asking for more than sstring can
    // address up front.
    constexpr size_t kMaxInitialReserve = size_t{1} << 30;
    const size_t estimate =
        std::min(kMaxInitialReserve,
                 static_cast<size_t>(response.statistics.pointCount) * (MAX_INT_LEN + 1 + MAX_DOUBLE_LEN + 1) + 512);
    ContiguousBody buf(estimate);
    const size_t length = co_await writeBody(buf, response);
    co_return buf.take(length);
}

seastar::future<> ResponseFormatter::streamBody(QueryResponse& response, seastar::output_stream<char>& out) {
    StreamedBody buf(out);
    const size_t tail = co_await writeBody(buf, response);
    co_await buf.finish(tail);
}

std::string ResponseFormatter::formatError(const std::string& message, const std::string& code) {
    return timestar::http::jsonError(message, code);
}

}  // namespace timestar::http

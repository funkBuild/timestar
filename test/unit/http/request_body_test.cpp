// readRequestBody() is the only way a handler obtains its request body: from
// the server's content stream in production, from an injected in-memory body
// when a test or internal caller invokes a handler directly. These tests drive
// the stream path with a scripted data source, since no handler-level test
// goes through a real connection.

#include "../../../lib/http/request_body.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <random>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/request.hh>
#include <stdexcept>
#include <string>
#include <vector>

using timestar::http::BodyStatus;
using timestar::http::injectedBody;
using timestar::http::readRequestBody;

namespace {

// Hands out the scripted chunks one get() at a time, then end-of-stream — or,
// when `failAfter` chunks have been served, throws the way Seastar's content
// source does for a body that ends before its declared length.
class ScriptedSource final : public seastar::data_source_impl {
public:
    ScriptedSource(std::vector<std::string> chunks, size_t& reads, size_t failAfter = SIZE_MAX)
        : chunks_(std::move(chunks)), reads_(reads), failAfter_(failAfter) {}

    seastar::future<seastar::temporary_buffer<char>> get() override {
        ++reads_;
        if (next_ >= failAfter_) {
            return seastar::make_exception_future<seastar::temporary_buffer<char>>(
                std::runtime_error("connection closed mid-body"));
        }
        if (next_ >= chunks_.size()) {
            return seastar::make_ready_future<seastar::temporary_buffer<char>>();
        }
        const std::string& chunk = chunks_[next_++];
        return seastar::make_ready_future<seastar::temporary_buffer<char>>(
            seastar::temporary_buffer<char>(chunk.data(), chunk.size()));
    }

private:
    std::vector<std::string> chunks_;
    size_t& reads_;
    size_t failAfter_;
    size_t next_ = 0;
};

struct StreamedRequest {
    seastar::http::request req;
    seastar::input_stream<char> stream;
    size_t reads = 0;

    // declaredLength 0 is how Seastar presents a chunked (or absent) body.
    StreamedRequest(std::vector<std::string> chunks, size_t declaredLength, size_t failAfter = SIZE_MAX)
        : stream(seastar::data_source(std::make_unique<ScriptedSource>(std::move(chunks), reads, failAfter))) {
        req.content_length = declaredLength;
        req.content_stream = &stream;
    }
};

std::string randomBytes(size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::string s(n, '\0');
    for (auto& c : s) {
        c = static_cast<char>(rng());
    }
    return s;
}

std::vector<std::string> split(const std::string& data, size_t chunkSize) {
    std::vector<std::string> out;
    for (size_t pos = 0; pos < data.size(); pos += chunkSize) {
        out.push_back(data.substr(pos, chunkSize));
    }
    return out;
}

std::string asString(const seastar::sstring& s) {
    return std::string(s.data(), s.size());
}

}  // namespace

TEST(RequestBodyTest, InjectedBodyIsReturnedWhenThereIsNoStream) {
    seastar::http::request req;
    injectedBody(req) = "{\"query\":\"avg:m()\"}";
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(req, 1024, body).get(), BodyStatus::Ok);
    EXPECT_EQ(asString(body), "{\"query\":\"avg:m()\"}");
}

TEST(RequestBodyTest, InjectedBodyHonoursTheLimit) {
    seastar::http::request req;
    injectedBody(req) = std::string(101, 'x');
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(req, 100, body).get(), BodyStatus::TooLarge);

    seastar::http::request atLimit;
    injectedBody(atLimit) = std::string(100, 'x');
    EXPECT_EQ(readRequestBody(atLimit, 100, body).get(), BodyStatus::Ok);
    EXPECT_EQ(body.size(), 100u);
}

TEST(RequestBodyTest, NoBodyAtAllIsAnEmptyOk) {
    seastar::http::request bare;
    seastar::sstring body = "stale";
    EXPECT_EQ(readRequestBody(bare, 100, body).get(), BodyStatus::Ok);
    EXPECT_TRUE(body.empty());

    StreamedRequest streamed({}, 0);
    body = "stale";
    EXPECT_EQ(readRequestBody(streamed.req, 100, body).get(), BodyStatus::Ok);
    EXPECT_TRUE(body.empty());
}

TEST(RequestBodyTest, DeclaredLengthBodyIsAssembledFromAnyChunking) {
    const std::string data = randomBytes(200'003, 1);
    for (size_t chunkSize : {size_t{1}, size_t{7}, size_t{4096}, size_t{65536}, data.size(), data.size() + 10}) {
        StreamedRequest r(split(data, chunkSize), data.size());
        seastar::sstring body;
        ASSERT_EQ(readRequestBody(r.req, data.size(), body).get(), BodyStatus::Ok) << "chunk=" << chunkSize;
        ASSERT_EQ(asString(body), data) << "chunk=" << chunkSize;
        // The JSON parser reads up to a terminator.
        ASSERT_EQ(body.c_str()[body.size()], '\0');
    }
}

TEST(RequestBodyTest, DeclaredLengthOverTheLimitIsRefusedBeforeReading) {
    StreamedRequest r(split(std::string(5000, 'x'), 1000), 5000);
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(r.req, 4999, body).get(), BodyStatus::TooLarge);
    EXPECT_EQ(r.reads, 0u) << "an oversized body must not be pulled off the connection";
    EXPECT_TRUE(body.empty());
}

TEST(RequestBodyTest, BodyShorterThanDeclaredIsIncomplete) {
    // The source reports end-of-stream after 600 of the 1000 declared bytes.
    StreamedRequest r(split(std::string(600, 'x'), 200), 1000);
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(r.req, 10'000, body).get(), BodyStatus::Incomplete);
    EXPECT_TRUE(body.empty()) << "partial bytes must not reach the handler";
}

TEST(RequestBodyTest, ReadFailureIsIncompleteNotAnException) {
    for (size_t declared : {size_t{1000}, size_t{0}}) {
        StreamedRequest r(split(std::string(1000, 'x'), 100), declared, /*failAfter=*/3);
        seastar::sstring body;
        EXPECT_EQ(readRequestBody(r.req, 10'000, body).get(), BodyStatus::Incomplete) << "declared=" << declared;
        EXPECT_TRUE(body.empty());
    }
}

TEST(RequestBodyTest, BytesBeyondTheDeclaredLengthAreNotTaken) {
    // Seastar's content source never over-delivers; the reader must not rely on it.
    StreamedRequest r({std::string(60, 'a'), std::string(60, 'b')}, 100);
    seastar::sstring body;
    ASSERT_EQ(readRequestBody(r.req, 1000, body).get(), BodyStatus::Ok);
    EXPECT_EQ(asString(body), std::string(60, 'a') + std::string(40, 'b'));
}

TEST(RequestBodyTest, ChunkedBodyIsAssembled) {
    const std::string data = randomBytes(70'001, 2);
    for (size_t chunkSize : {size_t{1}, size_t{333}, size_t{8192}}) {
        StreamedRequest r(split(data, chunkSize), 0);
        seastar::sstring body;
        ASSERT_EQ(readRequestBody(r.req, data.size(), body).get(), BodyStatus::Ok) << "chunk=" << chunkSize;
        ASSERT_EQ(asString(body), data) << "chunk=" << chunkSize;
        ASSERT_EQ(body.c_str()[body.size()], '\0');
    }
}

TEST(RequestBodyTest, ChunkedBodyStopsReadingOnceOverTheLimit) {
    // 100 chunks of 100 bytes against a 1000-byte limit: the 11th chunk tips
    // it over, and nothing after that is read.
    StreamedRequest r(split(std::string(10'000, 'x'), 100), 0);
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(r.req, 1000, body).get(), BodyStatus::TooLarge);
    EXPECT_EQ(r.reads, 11u);
    EXPECT_TRUE(body.empty());
}

TEST(RequestBodyTest, BufferedServerModeStillWorks) {
    // With content streaming off, Seastar fills the body before the handler
    // runs and leaves a drained stream attached.
    StreamedRequest r({}, 12);
    injectedBody(r.req) = "hello, world";
    seastar::sstring body;
    EXPECT_EQ(readRequestBody(r.req, 100, body).get(), BodyStatus::Ok);
    EXPECT_EQ(asString(body), "hello, world");
    EXPECT_EQ(r.reads, 0u);
}

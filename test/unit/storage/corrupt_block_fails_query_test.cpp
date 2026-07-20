// A corrupt TSM block must FAIL the query, never return a short answer.
//
// WHY THIS TEST EXISTS
//
// This is the test that would have caught the string-decoder bug at its root,
// and nothing like it existed. That bug made a multi-block string series decode
// every block's timestamps but only the last block's values. The query returned
// HTTP 200 with fewer points than were written -- a silent partial answer, which
// is precisely the failure this codebase already rejects for unreadable series
// (QUERY_INCOMPLETE) but had no equivalent for at the block level.
//
// The decode count contract now covers it: a block whose value count does not
// match its timestamp count raises BlockDecodeError, which routes into the same
// QUERY_INCOMPLETE path. This test pins the END-TO-END consequence rather than
// the mechanism, so it survives a rewrite of the internals.
//
// FAULT INJECTION
//
// Corrupting bytes inside a written TSM file is the only way to reach the
// failure path without hand-building a block: the writer always emits matching
// counts, by construction. String payloads are zstd-compressed, so damaged bytes
// reliably fail to decode rather than silently producing a different-but-valid
// value -- which is what makes the outcome deterministic enough to assert.
//
// Seastar reads TSM blocks with O_DIRECT, so damage on disk is visible to the
// next query without restarting the engine.
//
// WHAT IS ASSERTED
//
// Not "the query fails" -- a corrupt file could in principle still decode. The
// assertion is the invariant that actually matters:
//
//     the query must not report SUCCESS while silently returning fewer points
//     than were written.
//
// Either it succeeds completely, or it fails visibly. A short 200 is the bug.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

using namespace timestar;
namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec = 1000000000ULL;
constexpr uint64_t kBase = 1700000000ULL * kSec;

std::unique_ptr<seastar::http::request> jsonRequest(const std::string& body) {
    auto req = std::make_unique<seastar::http::request>();
    req->content = body;
    req->_headers["Content-Type"] = "application/json";
    return req;
}

void flushToTsm(seastar::sharded<Engine>& eng) {
    eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int attempt = 0; attempt < 200; ++attempt) {
        const size_t files =
            eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
                .get();
        if (files >= 1) {
            return;
        }
        seastar::sleep(std::chrono::milliseconds(50)).get();
    }
    FAIL() << "no TSM file appeared after rollover";
}

// The largest .tsm file under any shard directory -- the one holding the series.
std::string findLargestTsmFile() {
    std::string best;
    uintmax_t bestSize = 0;
    for (int shard = 0; shard < 64; ++shard) {
        const fs::path dir = fs::path("shard_" + std::to_string(shard)) / "tsm";
        std::error_code ec;
        if (!fs::exists(dir, ec)) {
            continue;
        }
        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }
            const auto size = entry.file_size(ec);
            if (!ec && size > bestSize) {
                bestSize = size;
                best = entry.path().string();
            }
        }
    }
    return best;
}

// Overwrite a span in the DATA region with garbage, keeping the file length
// identical so every offset in the index still resolves. Starting at 25% keeps
// it clear of both the header and the index/footer at the tail.
bool corruptDataRegion(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "r+b");
    if (f == nullptr) {
        return false;
    }
    std::fseek(f, 0, SEEK_END);
    const long size = std::ftell(f);
    if (size < 512) {
        std::fclose(f);
        return false;
    }
    // Span scales with the file: string payloads compress hard, so a TSM file
    // holding thousands of repetitive values can be only a few KB.
    const size_t spanBytes = static_cast<size_t>(size) * 3 / 5;
    const long offset = size / 5;
    std::fseek(f, offset, SEEK_SET);
    std::vector<unsigned char> garbage(spanBytes, 0xA5);
    const size_t wrote = std::fwrite(garbage.data(), 1, garbage.size(), f);
    std::fclose(f);
    return wrote == garbage.size();
}

struct QueryOutcome {
    int status = 0;
    size_t pointCount = 0;
    std::string body;
};

QueryOutcome runQuery(HttpQueryHandler& handler, const std::string& measurement, uint64_t start, uint64_t end) {
    const std::string body = R"({"query":"latest:)" + measurement + R"((v){}","startTime":)" + std::to_string(start) +
                             R"(,"endTime":)" + std::to_string(end) + "}";
    auto rep = handler.handleQuery(jsonRequest(body)).get();

    QueryOutcome out;
    out.status = static_cast<int>(rep->_status);
    out.body = std::string(rep->_content.substr(0, 300));
    if (out.status != 200) {
        return out;
    }

    glz::generic parsed;
    if (glz::read_json(parsed, rep->_content)) {
        return out;
    }
    auto& obj = parsed.get<glz::generic::object_t>();
    if (!obj.contains("series")) {
        return out;
    }
    auto& arr = obj["series"].get<glz::generic::array_t>();
    if (arr.empty()) {
        return out;
    }
    auto& fields = arr[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
    for (auto& [name, fieldObj] : fields) {
        out.pointCount = fieldObj.get<glz::generic::object_t>()["timestamps"].get<glz::generic::array_t>().size();
        break;
    }
    return out;
}

void writeStringSeries(HttpWriteHandler& handler, const std::string& measurement, size_t count) {
    constexpr size_t kBatch = 500;
    for (size_t start = 0; start < count; start += kBatch) {
        const size_t end = std::min(count, start + kBatch);
        std::string ts = "[";
        std::string vs = "[";
        for (size_t i = start; i < end; ++i) {
            if (i > start) {
                ts += ",";
                vs += ",";
            }
            ts += std::to_string(kBase + static_cast<uint64_t>(i) * kSec);
            vs += "\"payload_value_" + std::to_string(i) + "\"";
        }
        ts += "]";
        vs += "]";
        auto rep = handler
                       .handleWrite(jsonRequest(R"({"measurement":")" + measurement +
                                                R"(","tags":{"host":"h0"},"fields":{"v":)" + vs +
                                                R"(},"field_types":{"v":"string"},"timestamps":)" + ts + "}"))
                       .get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
    }
}

}  // namespace

class CorruptBlockFailsQueryTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

TEST_F(CorruptBlockFailsQueryTest, CorruptBlockNeverYieldsASuccessfulShortAnswer) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // More than MaxPointsPerBlock so the series spans several blocks: damage
        // to one block must not be quietly absorbed by returning the others.
        constexpr size_t kCount = 6000;
        writeStringSeries(writeHandler, "corrupt_probe", kCount);
        flushToTsm(eng.eng);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount) * kSec;

        auto healthy = runQuery(queryHandler, "corrupt_probe", start, end);
        ASSERT_EQ(healthy.status, 200) << healthy.body;
        ASSERT_EQ(healthy.pointCount, kCount) << "baseline read was already short; test cannot conclude anything";

        const std::string tsmFile = findLargestTsmFile();
        ASSERT_FALSE(tsmFile.empty()) << "no TSM file found to corrupt";
        ASSERT_TRUE(corruptDataRegion(tsmFile)) << "could not corrupt " << tsmFile;

        auto damaged = runQuery(queryHandler, "corrupt_probe", start, end);

        // ANTI-VACUITY GUARD, first. An earlier version of this test corrupted a
        // 256-byte span that happened to land somewhere inert: the query returned
        // 200 with all 6000 points, the assertions below passed, and the test
        // proved precisely nothing. A fault-injection test that cannot detect its
        // own fault is worse than no test, so an unaffected read fails here
        // rather than sailing through.
        ASSERT_FALSE(damaged.status == 200 && damaged.pointCount == kCount)
            << "fault injection was inert -- the read was unaffected by corrupting 60% of the file, "
               "so this test demonstrated nothing about corrupt-block handling";

        // THE INVARIANT: never a successful SHORT answer.
        ASSERT_NE(damaged.status, 200) << "query reported SUCCESS while silently dropping "
                                       << (kCount > damaged.pointCount ? kCount - damaged.pointCount : 0) << " of "
                                       << kCount << " points after block corruption -- exactly the "
                                       << "silent partial answer this contract exists to prevent";

        // And it must be the TYPED failure a caller can act on, not an
        // unhandled crash-shaped error.
        EXPECT_NE(damaged.body.find("QUERY_INCOMPLETE"), std::string::npos)
            << "corrupt block failed the query, but not with QUERY_INCOMPLETE: " << damaged.body;
    })
        .join()
        .get();
}

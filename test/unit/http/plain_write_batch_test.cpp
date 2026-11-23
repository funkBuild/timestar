// The plain-batch fast path reads {"writes":[...]} without building a JSON
// DOM. It must never change what a request means, so the DOM path is kept as
// the specification: for EVERY body the scanner accepts, the points it
// coalesces into must be exactly the DOM path's — same series, same value
// types, bit-identical values, same timestamps, same count of rejected
// entries. And for any shape the scanner was not built for, it must say "not
// plain" so the DOM path handles the request as it always has.

#include "../../../lib/http/plain_write_batch.hpp"

#include "../../../lib/http/http_write_handler.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <limits>
#include <random>
#include <string>
#include <vector>

using timestar::http::HttpWriteHandler;
using timestar::http::PlainField;
using timestar::http::PlainWriteBatch;
using timestar::http::scanPlainWriteBatch;

namespace {

constexpr uint64_t kDefaultTs = 1'700'000'000'123'456'789ULL;

struct Outcome {
    bool plainAccepted = false;
    bool domAccepted = false;
    std::string plain;
    std::string dom;
};

Outcome run(const std::string& body) {
    Outcome o;
    o.plain = HttpWriteHandler::describeCoalescedBatch(body, kDefaultTs, /*plainPath=*/true, &o.plainAccepted);
    o.dom = HttpWriteHandler::describeCoalescedBatch(body, kDefaultTs, /*plainPath=*/false, &o.domAccepted);
    return o;
}

// The invariant, for any body whatsoever.
void expectConsistent(const std::string& body) {
    const Outcome o = run(body);
    if (o.plainAccepted) {
        ASSERT_TRUE(o.domAccepted) << "scanner accepted a body the DOM path rejects:\n" << body.substr(0, 600);
        ASSERT_EQ(o.plain, o.dom) << "body:\n" << body.substr(0, 1200);
    }
}

void expectPlain(const std::string& body) {
    const Outcome o = run(body);
    ASSERT_TRUE(o.plainAccepted) << "expected the fast path to take:\n" << body.substr(0, 600);
    ASSERT_TRUE(o.domAccepted) << body.substr(0, 600);
    ASSERT_EQ(o.plain, o.dom) << "body:\n" << body.substr(0, 1200);
}

void expectNotPlain(const std::string& body) {
    const Outcome o = run(body);
    ASSERT_FALSE(o.plainAccepted) << "the fast path must leave this to the DOM path:\n" << body.substr(0, 600);
}

std::string batchOf(const std::vector<std::string>& writes) {
    std::string body = R"({"writes":[)";
    for (size_t i = 0; i < writes.size(); ++i) {
        if (i) {
            body += ',';
        }
        body += writes[i];
    }
    body += "]}";
    return body;
}

const char* const kGood = R"({"measurement":"cpu","tags":{"host":"a"},"fields":{"v":1.5},"timestamp":1000})";

// ── random plain batches ────────────────────────────────────────────

class PlainGen {
public:
    explicit PlainGen(uint64_t seed) : rng_(seed) {}

    std::string batch(size_t writes) {
        std::vector<std::string> w;
        for (size_t i = 0; i < writes; ++i) {
            w.push_back(write());
        }
        return batchOf(w);
    }

    std::string number() {
        char buf[64];
        switch (rng_() % 12) {
            case 0:
                return std::to_string(static_cast<int64_t>(rng_() % 2001) - 1000);
            case 1:
                return pick({"0", "1", "-1", "9223372036854775807", "-9223372036854775808", "1000000000000"});
            case 2:
                std::snprintf(buf, sizeof buf, "%.3f", static_cast<double>(rng_() % 2000000) / 1000.0 - 1000.0);
                return buf;
            case 3:
                std::snprintf(buf, sizeof buf, "%.17g", std::bit_cast<double>(rng_()));
                return finiteOr(buf);
            case 4:
                std::snprintf(buf, sizeof buf, "%g",
                              std::ldexp(static_cast<double>(rng_() % 100000), int(rng_() % 60) - 30));
                return finiteOr(buf);
            case 5:
                return pick({"1.0", "-0.0", "0.0", "1e5", "1E5", "1e+5", "1.5e-7", "-2.5E+10", "0.1",
                             "0.30000000000000004", "123456789012345680000.0", "1.7976931348623157e308",
                             "2.2250738585072014e-308", "100.0", "0e0", "12.50"});
            case 6:
                std::snprintf(buf, sizeof buf, "%.1f", static_cast<double>(static_cast<int64_t>(rng_() % 2000) - 1000));
                return buf;
            case 7:
                std::snprintf(buf, sizeof buf, "%lld", static_cast<long long>(rng_() >> (1 + rng_() % 63)));
                return buf;
            default:
                std::snprintf(buf, sizeof buf, "%.6f", static_cast<double>(rng_() % 100000000) / 1e6);
                return buf;
        }
    }

private:
    std::string pick(std::initializer_list<const char*> options) {
        auto it = options.begin();
        std::advance(it, rng_() % options.size());
        return *it;
    }
    static std::string finiteOr(const char* text) {
        const std::string s = text;
        return (s.find("inf") != std::string::npos || s.find("nan") != std::string::npos) ? "42.5" : s;
    }
    std::string ws() {
        switch (rng_() % 12) {
            case 0:
                return " ";
            case 1:
                return "\n  ";
            case 2:
                return "\t";
            case 3:
                return "\r\n";
            default:
                return "";
        }
    }
    std::string str(std::initializer_list<const char*> options) { return "\"" + pick(options) + "\""; }

    std::string value() {
        switch (rng_() % 10) {
            case 0:
                return "true";
            case 1:
                return "false";
            case 2:
                return str({"", "on", "off", "state with spaces", "a=b,c d", "#1", "{not json}", "x"});
            default:
                return number();
        }
    }

    std::string write() {
        // A small universe, so series repeat: type conflicts, promotions,
        // repeated timestamps and series-memo hits all occur.
        std::string measurement = "\"measurement\":" + ws() + str({"cpu", "cpu", "mem", "disk.io", "m", ""});
        std::string tags = "\"tags\":" + ws() + "{";
        const size_t nTags = rng_() % 4;
        const char* keys[] = {"host", "rack", "dc", ""};
        size_t order[] = {0, 1, 2, 3};
        std::shuffle(std::begin(order), std::end(order), rng_);
        for (size_t i = 0; i < nTags; ++i) {
            if (i) {
                tags += "," + ws();
            }
            tags += "\"" + std::string(keys[order[i]]) + "\"" + ws() + ":" + ws() +
                    str({"a", "b", "host-01", "rack 2", ""});
        }
        tags += ws() + "}";
        std::string fields = "\"fields\":" + ws() + "{";
        const size_t nFields = 1 + rng_() % 3;
        const char* names[] = {"v", "usage", "state", ""};
        std::shuffle(std::begin(order), std::end(order), rng_);
        for (size_t i = 0; i < nFields; ++i) {
            if (i) {
                fields += "," + ws();
            }
            fields += "\"" + std::string(names[order[i]]) + "\"" + ws() + ":" + ws() + value();
        }
        fields += ws() + "}";
        std::string timestamp = "\"timestamp\":" + ws() +
                                pick({"0", "1", "1000", "1000", "2000", "1700000000000000000", "18446744073709551615"});

        std::vector<std::string> members = {measurement, fields};
        if (rng_() % 5) {
            members.push_back(tags);
        }
        if (rng_() % 4) {
            members.push_back(timestamp);
        }
        std::shuffle(members.begin(), members.end(), rng_);
        std::string out = ws() + "{" + ws();
        for (size_t i = 0; i < members.size(); ++i) {
            if (i) {
                out += ws() + "," + ws();
            }
            out += members[i];
        }
        return out + ws() + "}" + ws();
    }

    std::mt19937_64 rng_;
};

}  // namespace

TEST(PlainWriteBatchTest, ScannerReadsThePlainShape) {
    PlainWriteBatch batch;
    const std::string body = batchOf(
        {R"({"measurement":"cpu","tags":{"host":"a","rack":"r1"},"fields":{"usage":12.5,"cores":8,"ok":true,"state":"up"},"timestamp":1700000000000000000})",
         R"({"fields":{"v":-3},"measurement":"mem"})"});
    ASSERT_TRUE(scanPlainWriteBatch(body, batch, 100));
    ASSERT_EQ(batch.writes.size(), 2u);

    const auto& first = batch.writes[0];
    EXPECT_EQ(first.measurement, "cpu");
    EXPECT_TRUE(first.hasTimestamp);
    EXPECT_EQ(first.timestamp, 1700000000000000000ULL);
    ASSERT_EQ(first.tagsEnd - first.tagsBegin, 2u);
    EXPECT_EQ(batch.tags[first.tagsBegin].key, "host");
    EXPECT_EQ(batch.tags[first.tagsBegin + 1].value, "r1");
    ASSERT_EQ(first.fieldsEnd - first.fieldsBegin, 4u);
    const auto* f = &batch.fields[first.fieldsBegin];
    EXPECT_EQ(f[0].kind, PlainField::Kind::Float);
    EXPECT_EQ(f[0].real, 12.5);
    EXPECT_EQ(f[1].kind, PlainField::Kind::Integer);
    EXPECT_EQ(f[1].integer, 8);
    EXPECT_EQ(f[2].kind, PlainField::Kind::Boolean);
    EXPECT_TRUE(f[2].boolean);
    EXPECT_EQ(f[3].kind, PlainField::Kind::String);
    EXPECT_EQ(f[3].text, "up");

    const auto& second = batch.writes[1];
    EXPECT_EQ(second.measurement, "mem");
    EXPECT_FALSE(second.hasTimestamp);
    EXPECT_EQ(second.tagsBegin, second.tagsEnd);
    EXPECT_EQ(batch.fields[second.fieldsBegin].integer, -3);

    // The write cap is enforced by the scanner, so an oversized batch reaches
    // the DOM path and gets its "batch too large" answer.
    EXPECT_FALSE(scanPlainWriteBatch(body, batch, 1));
}

TEST(PlainWriteBatchTest, RandomBatchesCoalesceExactlyAsTheDomPath) {
    // The generator stays inside the plain shape except for the occasional
    // number form the scanner leaves to the DOM ("1e5"), so most small
    // batches are accepted; each accepted one must match the DOM path.
    PlainGen gen(20261122);
    size_t accepted = 0;
    const int rounds = 3000;
    for (int i = 0; i < rounds; ++i) {
        const std::string body = gen.batch(1 + static_cast<size_t>(i) % 12);
        const Outcome o = run(body);
        ASSERT_TRUE(o.domAccepted) << body;
        if (o.plainAccepted) {
            ++accepted;
            ASSERT_EQ(o.plain, o.dom) << "body:\n" << body.substr(0, 2000);
        }
    }
    EXPECT_GT(accepted, static_cast<size_t>(rounds) / 2);
}

TEST(PlainWriteBatchTest, NumbersAreReadExactlyAsTheDomPathReadsThem) {
    PlainGen gen(7);
    size_t accepted = 0;
    const int rounds = 40'000;
    for (int round = 0; round < rounds; ++round) {
        std::vector<std::string> writes;
        for (int i = 0; i < 6; ++i) {
            // One series per value, so nothing is promoted or merged: the
            // dump shows each number's type and exact bits.
            writes.push_back("{\"measurement\":\"n\",\"tags\":{\"i\":\"" + std::to_string(i) +
                             "\"},\"fields\":{\"v\":" + gen.number() + "},\"timestamp\":1}");
        }
        const std::string body = batchOf(writes);
        const Outcome o = run(body);
        ASSERT_TRUE(o.domAccepted) << body;
        if (o.plainAccepted) {
            ++accepted;
            ASSERT_EQ(o.plain, o.dom) << "body:\n" << body;
        }
    }
    EXPECT_GT(accepted, static_cast<size_t>(rounds) / 3);
}

TEST(PlainWriteBatchTest, TypicalBatchesTakeTheFastPath) {
    // What clients actually send must not fall back.
    std::vector<std::string> writes;
    for (int i = 0; i < 5000; ++i) {
        char buf[256];
        std::snprintf(
            buf, sizeof buf,
            R"({"measurement":"server.metrics","tags":{"host":"host-%02d","rack":"rack-%d"},"fields":{"cpu_usage":%.6f,"cores":%d,"healthy":%s,"state":"%s"},"timestamp":%llu})",
            i % 10, i % 2, static_cast<double>(i) * 0.37, i % 64, (i % 3) ? "true" : "false",
            (i % 5) ? "up" : "degraded", 1700000000000000000ULL + static_cast<unsigned long long>(i) * 1000000000ULL);
        writes.push_back(buf);
    }
    expectPlain(batchOf(writes));
    expectPlain(batchOf({kGood}));
    expectPlain("  {\n \"writes\" : [ " + std::string(kGood) + " , " + kGood + " ]\n}\n");
    expectPlain(batchOf(
        {R"({"measurement":"m","fields":{"v":-12.5}})", R"({"measurement":"m","fields":{"v":0},"tags":{}})",
         R"({"timestamp":18446744073709551615,"fields":{"v":"text"},"measurement":"m"})",
         R"({"measurement":"m","fields":{"a":9223372036854775807,"b":-9223372036854775808,"c":0.1,"d":1.5e-7,"e":2.5E+10}})"}));
}

TEST(PlainWriteBatchTest, TypeConflictsAndFullCandidatesResolveIdentically) {
    auto w = [](const char* value, uint64_t ts) {
        return std::string(R"({"measurement":"m","tags":{"h":"a"},"fields":{"v":)") + value + R"(},"timestamp":)" +
               std::to_string(ts) + "}";
    };
    // Integer then float (promotion), float then integer, and non-numeric
    // conflicts (disambiguated candidates), in several interleavings.
    expectPlain(batchOf({w("1", 1), w("2", 2), w("2.5", 3), w("4", 4)}));
    {
        // Large random batches: many repeats per series, memo hits, promotions.
        PlainGen gen(5);
        for (int i = 0; i < 6; ++i) {
            expectConsistent(gen.batch(5000));
        }
    }
    expectPlain(batchOf({w("1.5", 1), w("2", 2), w("3", 3)}));
    expectPlain(batchOf({w("1", 1), w("true", 2), w("\"s\"", 3), w("2", 4), w("false", 5), w("\"t\"", 6)}));
    expectPlain(batchOf({w("true", 1), w("1.5", 2), w("1", 3), w("true", 4), w("2.5", 5)}));
    expectPlain(batchOf({w("\"a\"", 1), w("\"a\"", 1), w("1", 1), w("1", 1), w("1.0", 1)}));

    // More values for one series than a candidate holds: the overflow goes
    // to a second candidate.
    std::vector<std::string> many;
    for (uint64_t i = 0; i < 10'050; ++i) {
        many.push_back(w(i % 3 == 0 ? "7" : "7.25", i));
    }
    expectPlain(batchOf(many));
}

TEST(PlainWriteBatchTest, RejectedWritesAreCountedAndPartiallyAppliedIdentically) {
    // An empty name fails validation. Which fields of the write were already
    // taken when it does is part of the result.
    expectPlain(batchOf({R"({"measurement":"","fields":{"v":1}})", kGood}));
    expectPlain(batchOf({R"({"measurement":"m","tags":{"":"x"},"fields":{"v":1}})", kGood}));
    expectPlain(batchOf({R"({"measurement":"m","tags":{"k":""},"fields":{"v":1}})", kGood}));
    expectPlain(batchOf({R"({"measurement":"m","fields":{"a":1,"":2,"c":3}})", kGood}));
    expectPlain(
        batchOf({R"({"measurement":"m","fields":{"":2,"c":3}})", kGood, R"({"measurement":"m","fields":{"c":4}})"}));
    // The series memo must survive a rejected write exactly as the DOM path's does.
    expectPlain(batchOf({R"({"measurement":"m","tags":{"h":"a"},"fields":{"v":1}})",
                         R"({"measurement":"m","tags":{"h":"a"},"fields":{"":1}})",
                         R"({"measurement":"m","tags":{"h":"a"},"fields":{"v":2}})",
                         R"({"measurement":"m","tags":{"":"a"},"fields":{"v":3}})",
                         R"({"measurement":"m","tags":{"h":"a"},"fields":{"v":4}})"}));
}

TEST(PlainWriteBatchTest, EverythingElseIsLeftToTheDomPath) {
    auto with = [](const std::string& write) { return batchOf({kGood, write, kGood}); };

    const std::vector<std::string> notPlain = {
        // field values that are not scalars
        with(R"({"measurement":"m","fields":{"v":[1,2,3]},"timestamps":[1,2,3]})"),
        with(R"({"measurement":"m","fields":{"v":[1.5]}})"),
        with(R"({"measurement":"m","fields":{"v":null}})"),
        with(R"({"measurement":"m","fields":{"v":{"x":1}}})"),
        // keys outside the plain shape
        with(R"({"measurement":"m","fields":{"v":1},"timestamps":[5]})"),
        with(R"({"measurement":"m","fields":{"v":1},"field_types":{"v":"float"}})"),
        with(R"({"measurement":"m","fields":{"v":1},"extra":true})"),
        // repeated keys
        with(R"({"measurement":"m","measurement":"n","fields":{"v":1}})"),
        with(R"({"measurement":"m","fields":{"v":1},"fields":{"w":2}})"),
        with(R"({"measurement":"m","tags":{"h":"a","h":"b"},"fields":{"v":1}})"),
        with(R"({"measurement":"m","fields":{"v":1,"v":2}})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":1,"timestamp":2})"),
        with(R"({"measurement":"m","tags":{},"tags":{},"fields":{"v":1}})"),
        // strings needing the real JSON reader
        with(R"({"measurement":"m\n","fields":{"v":1}})"),
        with(R"({"measurement":"m","tags":{"h":"a\"b"},"fields":{"v":1}})"),
        with(std::string(R"({"measurement":"m","fields":{"v":")") + "\\u0041" + R"("}})"),
        with(R"({"measurement":"m","fields":{"v\\":1}})"),
        with("{\"measurement\":\"caf\xC3\xA9\",\"fields\":{\"v\":1}}"),
        with("{\"measurement\":\"m\",\"fields\":{\"v\":\"\xF0\x9F\x98\x80\"}}"),
        with("{\"measurement\":\"m\x01\",\"fields\":{\"v\":1}}"),
        with(std::string("{\"measurement\":\"m\0x\",\"fields\":{\"v\":1}}", 37)),
        // timestamps that are not plain unsigned integers
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":1.7e18})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":1000.0})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":-5})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":"1000"})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":null})"),
        with(R"({"measurement":"m","fields":{"v":1},"timestamp":18446744073709551616})"),
        // numbers the DOM stores in ways the scanner does not reproduce
        with(R"({"measurement":"m","fields":{"v":9223372036854775808}})"),
        with(R"({"measurement":"m","fields":{"v":18446744073709551615}})"),
        with(R"({"measurement":"m","fields":{"v":-9223372036854775809}})"),
        with(R"({"measurement":"m","fields":{"v":123456789012345678901234567890}})"),
        with(R"({"measurement":"m","fields":{"v":-0}})"),
        with(R"({"measurement":"m","fields":{"v":1e999}})"),
        with(R"({"measurement":"m","fields":{"v":-1e999}})"),
        with(R"({"measurement":"m","fields":{"v":1e-999}})"),
        // not JSON numbers at all
        with(R"({"measurement":"m","fields":{"v":01}})"),
        with(R"({"measurement":"m","fields":{"v":1.}})"),
        with(R"({"measurement":"m","fields":{"v":.5}})"),
        with(R"({"measurement":"m","fields":{"v":+1}})"),
        with(R"({"measurement":"m","fields":{"v":1e}})"),
        with(R"({"measurement":"m","fields":{"v":1e+}})"),
        with(R"({"measurement":"m","fields":{"v":0x10}})"),
        with(R"({"measurement":"m","fields":{"v":NaN}})"),
        with(R"({"measurement":"m","fields":{"v":Infinity}})"),
        with(R"({"measurement":"m","fields":{"v":-}})"),
        with(R"({"measurement":"m","fields":{"v":1 2}})"),
        with(R"({"measurement":"m","fields":{"v":12abc}})"),
        with(R"({"measurement":"m","fields":{"v":True}})"),
        with(R"({"measurement":"m","fields":{"v":truex}})"),
        with(R"({"measurement":"m","fields":{"v":tru}})"),
        // missing or mistyped members
        with(R"({"fields":{"v":1}})"),
        with(R"({"measurement":"m"})"),
        with(R"({"measurement":"m","fields":{}})"),
        with(R"({"measurement":7,"fields":{"v":1}})"),
        with(R"({"measurement":"m","fields":[1]})"),
        with(R"({"measurement":"m","tags":["a"],"fields":{"v":1}})"),
        with(R"({"measurement":"m","tags":null,"fields":{"v":1}})"),
        with(R"({"measurement":"m","tags":{"h":1},"fields":{"v":1}})"),
        with(R"({"measurement":"m","tags":{"h":true},"fields":{"v":1}})"),
        with(R"({"measurement":"m","tags":{"h":null},"fields":{"v":1}})"),
        with(R"({})"),
        // elements that are not objects
        with("7"),
        with("null"),
        with("\"cpu\""),
        with("[]"),
        // the envelope
        R"({"writes":[]})",
        R"({"writes":{}})",
        R"({"writes":null})",
        R"({"writes":[)" + std::string(kGood) + R"(],"other":1})",
        R"({"other":1,"writes":[)" + std::string(kGood) + "]}",
        R"({"Writes":[)" + std::string(kGood) + "]}",
        "[" + std::string(kGood) + "]",
        std::string(kGood),
        // malformed
        R"({"writes":[)" + std::string(kGood),
        R"({"writes":[)" + std::string(kGood) + "]",
        R"({"writes":[)" + std::string(kGood) + ",]}",
        R"({"writes":[,)" + std::string(kGood) + "]}",
        R"({"writes":[)" + std::string(kGood) + "]}x",
        R"({"writes":[)" + std::string(kGood) + "]}{}",
        R"({"writes":[{"measurement":"m","fields":{"v":1},}]})",
        R"({"writes":[{"measurement":"m" "fields":{"v":1}}]})",
        R"({"writes":[{"measurement":"m","fields":{"v" 1}}]})",
        R"({"writes":[{"measurement":"m","fields":{"v":1})",
        R"({"writes":[{"measurement":"m,"fields":{"v":1}}]})",
        "",
        " ",
        "{",
    };
    for (const auto& body : notPlain) {
        expectNotPlain(body);
        if (HasFatalFailure()) {
            return;
        }
        // And whatever the DOM path makes of it is unaffected by the scanner
        // having looked first.
        expectConsistent(body);
    }
}

TEST(PlainWriteBatchTest, MutatedBodiesNeverDiverge) {
    // Flip, drop and insert bytes in plain batches. Most mutants are not plain
    // (or not JSON); the ones the scanner still accepts must still agree.
    PlainGen gen(99);
    std::mt19937_64 rng(1234);
    const std::string alphabet = "{}[]\":,-+.eE0123456789tfn\\ \n\tax\x01\xC3";
    size_t accepted = 0;
    for (int i = 0; i < 6000; ++i) {
        std::string body = gen.batch(1 + rng() % 4);
        const int edits = 1 + static_cast<int>(rng() % 3);
        for (int e = 0; e < edits && !body.empty(); ++e) {
            const size_t pos = rng() % body.size();
            switch (rng() % 3) {
                case 0:
                    body[pos] = alphabet[rng() % alphabet.size()];
                    break;
                case 1:
                    body.erase(pos, 1);
                    break;
                default:
                    body.insert(pos, 1, alphabet[rng() % alphabet.size()]);
                    break;
            }
        }
        const Outcome o = run(body);
        if (o.plainAccepted) {
            ++accepted;
            ASSERT_TRUE(o.domAccepted) << "scanner accepted a body the DOM path rejects:\n" << body;
            ASSERT_EQ(o.plain, o.dom) << "body:\n" << body;
        }
    }
    // The mutation rate is low enough that a good share still parse: the
    // equality above is exercised, not vacuous.
    EXPECT_GT(accepted, size_t{200});
}

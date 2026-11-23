// Parse cost of a {"writes":[...]} scalar batch: glaze generic DOM (what the write handler used) vs the flat scanner.
#include "http/plain_write_batch.hpp"
#include <glaze/glaze.hpp>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
using Clock = std::chrono::steady_clock;
int main() {
    std::mt19937_64 rng(1); std::uniform_real_distribution<double> dist(0.0, 100.0);
    const char* fields[] = {"cpu_usage","memory_usage","disk_io_read","disk_io_write","network_in","network_out","load_avg_1m","load_avg_5m","load_avg_15m","temperature"};
    std::string body = R"({"writes":[)";
    const size_t n = 10000;
    for (size_t i = 0; i < n; ++i) {
        if (i) body += ',';
        char buf[256];
        snprintf(buf, sizeof buf, R"({"measurement":"server.metrics","tags":{"host":"host-%02d","rack":"rack-%d"},"fields":{"%s":%.6f},"timestamp":%llu})",
                 int(i / 10 % 10) + 1, int(i / 100 % 2) + 1, fields[i % 10], dist(rng), 1700000000000000000ULL + i * 60000000000ULL);
        body += buf;
    }
    body += "]}";
    printf("body %zu bytes, %zu writes (%.0f B/write)\n", body.size(), n, double(body.size()) / n);
    for (int round = 0; round < 3; ++round) {
        auto t0 = Clock::now(); size_t nodes = 0;
        for (int r = 0; r < 50; ++r) { glz::generic_u64 doc{}; auto ec = glz::read_json(doc, body); if (ec) return 1; nodes += doc.get<glz::generic_u64::object_t>().size(); }
        auto t1 = Clock::now(); size_t writes = 0;
        timestar::http::PlainWriteBatch batch;
        for (int r = 0; r < 50; ++r) { if (!timestar::http::scanPlainWriteBatch(body, batch, 100000)) return 2; writes += batch.writes.size(); }
        auto t2 = Clock::now();
        const double dom = std::chrono::duration<double, std::micro>(t1 - t0).count() / 50, scan = std::chrono::duration<double, std::micro>(t2 - t1).count() / 50;
        printf("DOM parse+free %.0f us (%.0f ns/write)   scan %.0f us (%.0f ns/write)   %.1fx  [%zu %zu]\n", dom, dom * 1000 / n, scan, scan * 1000 / n, dom / scan, nodes, writes);
    }
}

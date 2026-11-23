#include "wal.hpp"
#include "memory_store.hpp"
#include "timestar_config.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <stdexcept>

using Clock = std::chrono::steady_clock;

seastar::future<int> benchmark() {
    auto cfg = timestar::config();
    // Isolate encoding/buffered append on tmpfs. HTTP measurements separately
    // retain the default durable-ack mode; this is not an fsync benchmark.
    cfg.storage.wal_sync_mode = "rollover";
    timestar::setGlobalConfig(cfg);
    unsigned sequence = 100;
    for (const std::string shape : {"shared_regular", "shared_irregular", "owned_control", "shared_scalar"}) {
        const size_t n = shape == "shared_scalar" ? 1 : 65536;
        auto timestamps = std::make_shared<std::vector<uint64_t>>();
        uint64_t timestamp = 1700000000000000000ULL;
        for (size_t i = 0; i < n; ++i) {
            timestamp += shape == "shared_irregular" ? 1 + (i * 7919) % 1000003 : 1000000000;
            timestamps->push_back(timestamp);
        }
        auto shared = std::make_shared<const SharedTimestampBatch>(timestamps);
        std::vector<TimeStarInsert<double>> requests;
        for (size_t field = 0; field < 32; ++field) {
            TimeStarInsert<double> request("wal_cpu", "f" + std::to_string(field));
            if (shape == "owned_control")
                request.timestamps = *timestamps;
            else
                request.setSharedTimestamps(shared);
            request.values.assign(n, 25.25 + field);
            (void)request.seriesId128();
            requests.push_back(std::move(request));
        }
        std::vector<double> samples;
        size_t bytes = 0;
        for (int rep = 0; rep < 7; ++rep) {
            MemoryStore store(sequence);
            WAL wal(sequence++);
            co_await wal.init(&store);
            for (auto& request : requests)
                (void)wal.estimateInsertSize(request);
            const auto start = Clock::now();
            const auto result = co_await wal.insertBatch(requests);
            const double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
            if (result != WALInsertResult::Success)
                throw std::runtime_error("WAL insert failed");
            if (rep && bytes != wal.getCurrentSize())
                throw std::runtime_error("WAL byte count changed");
            bytes = wal.getCurrentSize();
            if (rep)
                samples.push_back(ms);
            co_await wal.close();
            std::filesystem::remove(WAL::sequenceNumberToFilename(sequence - 1));
        }
        std::sort(samples.begin(), samples.end());
        std::printf("%s points=%zu bytes=%zu median_ms=%.6f min_ms=%.6f max_ms=%.6f\n", shape.c_str(), n * 32,
                    bytes, (samples[2] + samples[3]) / 2, samples.front(), samples.back());
    }
    co_return 0;
}

int main(int argc, char** argv) {
    seastar::app_template app;
    return app.run(argc, argv, benchmark);
}

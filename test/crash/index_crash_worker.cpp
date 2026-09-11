// Separate process: SIGKILL must bypass every Engine/NativeIndex destructor.
#include "../test_helpers/native_index_test_access.hpp"
#include "engine.hpp"
#include "key_encoding.hpp"
#include "placement_table.hpp"
#include "series_key.hpp"

#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

namespace ke = timestar::index::keys;
using timestar::index::NativeIndexTestAccess;

int main(int argc, char** argv) {
    if (argc < 3)
        return 2;
    const std::string mode = argv[1];
    timestar::TimestarConfig cfg;
    cfg.server.data_dir = argv[2];
    cfg.index.day_bitmap_flush_interval_seconds = 0;
    timestar::setGlobalConfig(cfg);
    std::vector<char*> args{argv[0]};
    for (int i = 3; i < argc; ++i)
        args.push_back(argv[i]);
    seastar::app_template app;
    return app.run(static_cast<int>(args.size()), args.data(), [&] {
        return seastar::async([&] {
            timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(seastar::smp::count));
            if (mode.starts_with("marker-")) {
                timestar::index::NativeIndex index(0);
                index.open().get();
                NativeIndexTestAccess::cancelTimers(index);
                if (mode == "marker-crash") {
                    ::kill(::getpid(), SIGKILL);
                    std::abort();
                }
                const bool wronglyClean = mode == "marker-verify" && index.openedCleanly();
                index.close().get();
                return wronglyClean ? 1 : 0;
            }

            Engine engine;
            engine.init().get();
            NativeIndexTestAccess::cancelTimers(engine.getIndex());
            if (mode == "seed") {
                TimeStarInsert<double> insert("crash", "v");
                insert.addTag("host", "established");
                insert.addValue(2000 * ke::NS_PER_DAY, 1.0);
                engine.insert(std::move(insert)).get();
            } else if (mode == "write-crash" || mode == "write-crash-no-metadata") {
                std::vector<TimeStarInsert<double>> batch;
                std::vector<MetadataOp> metadata;
                for (const std::string host : {"established", "new"}) {
                    TimeStarInsert<double> insert("crash", "v");
                    insert.addTag("host", host);
                    insert.addValue(1950 * ke::NS_PER_DAY, 2.0);
                    MetadataOp op;
                    op.measurement = "crash";
                    op.tags = insert.getTags();
                    op.fieldName = "v";
                    op.valueType = TSMValueType::Float;
                    op.minTs = op.maxTs = 1950 * ke::NS_PER_DAY;
                    metadata.push_back(std::move(op));
                    batch.push_back(std::move(insert));
                }
                // Match the HTTP acknowledgement path: data batch, then
                // metadata barrier. No clean close, extra flush or timer.
                engine.insertBatch<double>(std::move(batch)).get();
                if (mode == "write-crash")
                    engine.indexMetadataBatch(metadata).get();
                ::kill(::getpid(), SIGKILL);
                std::abort();
            } else if (mode == "verify") {
                auto found = engine.getIndex()
                                 .findSeriesWithMetadataTimeScoped("crash", {}, {}, 1950 * ke::NS_PER_DAY,
                                                                   1951 * ke::NS_PER_DAY - 1, 0)
                                 .get();
                bool valid = found.has_value() && found->size() == 2;
                if (found) {
                    for (const auto& series : *found) {
                        auto key = timestar::buildSeriesKey(series.metadata.measurement, series.metadata.tags,
                                                            series.metadata.field);
                        auto data =
                            engine.query(key, series.seriesId, 1950 * ke::NS_PER_DAY, 1951 * ke::NS_PER_DAY - 1).get();
                        valid &= data.has_value() && std::get<QueryResult<double>>(*data).timestamps.size() == 1;
                    }
                }
                engine.stop().get();
                return valid ? 0 : 1;
            } else {
                engine.stop().get();
                return 2;
            }
            engine.stop().get();
            return 0;
        });
    });
}

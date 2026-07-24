// M5 scrub + repair gate (in-process): the Scrubber detects a corrupt VShard replica,
// quarantines it, and emits a re-replication request; the repair rebuilds the replica
// through the SAME snapshot install path a move uses (Engine::buildVShardSnapshotFiles
// on a healthy source -> installVShardSnapshotFiles on the repaired node), after which
// the object leaves quarantine and a re-scrub finds it healthy. Composes the Scrubber
// brick with the real snapshot-repair path.
#include "../../../lib/cluster/movement/scrubber.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // vshardsCohesiveOnCores

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
namespace fs = std::filesystem;

TEST(ScrubRepair, DetectsCorruptionAndRepairsViaSnapshot) {
    seastar::thread([] {
        ASSERT_TRUE(vshardsCohesiveOnCores(seastar::smp::count));
        fs::remove_all("scrub_src");
        fs::remove_all("scrub_dst");

        // --- Healthy source: write + flush + build the repair snapshot. ---
        VShardId vshard{0};
        std::pair<VShardSnapshotManifest, std::vector<std::pair<std::string, std::string>>> snap;
        std::string key;
        SeriesId128 sid;
        {
            Engine src(StorageLayout("scrub_src").anchored());
            src.init().get();
            src.setRevisionAssignment(true);
            TimeStarInsert<double> probe("scrub", "value");
            probe.addTag("host", "h1");
            sid = probe.seriesId128();
            vshard = VShardId{virtualShard(sid)};
            key = "scrub,host=h1 value";
            {
                TimeStarInsert<double> ins("scrub", "value");
                ins.addTag("host", "h1");
                ins.addValue(1000, 9.0);
                src.insert(std::move(ins)).get();
            }
            src.rolloverMemoryStore().get();
            for (int i = 0; i < 300 && src.getTSMFileCount() == 0; ++i)
                seastar::sleep(std::chrono::milliseconds(100)).get();
            snap = src.buildVShardSnapshotFiles(vshard, std::string(32, '0')).get();
            src.stop().get();
        }
        ASSERT_FALSE(snap.second.empty());

        // The scrubbed object is this node's replica of the VShard. Its hash reports
        // corrupt until the repair re-replicates it.
        const std::string objectId = "vshard-" + std::to_string(vshard.value()) + "@dst";
        bool repaired = false;
        movement::Scrubber scrubber(
            [&](const std::string&) { return repaired ? std::string("good") : std::string("bad"); });
        std::vector<movement::ScrubTarget> targets = {{objectId, "good"}};

        // First scrub: corruption detected -> quarantined + one repair request.
        auto repairs = scrubber.scrub(targets);
        ASSERT_EQ(repairs.size(), 1u);
        EXPECT_EQ(repairs[0].objectId, objectId);
        EXPECT_TRUE(scrubber.isQuarantined(objectId));
        // A second scrub while still quarantined must NOT re-emit (no movement spam).
        EXPECT_TRUE(scrubber.scrub(targets).empty());

        // --- Repair: rebuild the replica on the corrupt node via the snapshot path. ---
        {
            Engine dst(StorageLayout("scrub_dst").anchored());
            dst.init().get();
            const bool ok = dst.installVShardSnapshotFiles(snap.first, snap.second).get();
            EXPECT_TRUE(ok) << "repair must install the healthy snapshot";
            // The repaired replica now serves the data.
            auto r = dst.query(key, sid, 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_DOUBLE_EQ(std::get<QueryResult<double>>(r.value()).values[0], 9.0);
            dst.stop().get();
        }

        // Repair complete: leave quarantine; a re-scrub now finds it healthy.
        repaired = true;
        scrubber.clearQuarantine(objectId);
        EXPECT_FALSE(scrubber.isQuarantined(objectId));
        EXPECT_TRUE(scrubber.scrub(targets).empty()) << "repaired object scrubs clean";

        fs::remove_all("scrub_src");
        fs::remove_all("scrub_dst");
    })
        .join()
        .get();
}

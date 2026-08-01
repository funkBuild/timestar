#include "../../../lib/index/native/native_index.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>
#include <utility>

namespace timestar::index {

// Narrow friend seam: deterministically hold the same gates the two periodic
// timer callbacks use. This avoids timing a test against real fsync latency.
class NativeIndexLifecycleTestPeer {
public:
    static seastar::future<> holdWalSyncGate(NativeIndex& index, seastar::future<> release) {
        return seastar::with_gate(index.walSyncGate_, [release = std::move(release)]() mutable {
            return std::move(release);
        });
    }

    static seastar::future<> holdDirtyCacheGate(NativeIndex& index, seastar::future<> release) {
        return seastar::with_gate(index.dirtyCacheGate_, [release = std::move(release)]() mutable {
            return std::move(release);
        });
    }

    static size_t walSyncCount(const NativeIndex& index) { return index.walSyncGate_.get_count(); }
    static size_t dirtyCacheCount(const NativeIndex& index) { return index.dirtyCacheGate_.get_count(); }
    static bool walSyncClosed(const NativeIndex& index) { return index.walSyncGate_.is_closed(); }
    static bool dirtyCacheClosed(const NativeIndex& index) { return index.dirtyCacheGate_.is_closed(); }
};

}  // namespace timestar::index

using timestar::index::NativeIndex;
using timestar::index::NativeIndexLifecycleTestPeer;

SEASTAR_TEST(NativeIndexLifecycleTest, AbandonForTestingDrainsBothTimerGatesBeforeDestruction) {
    const std::filesystem::path root = "shard_0/native_index";
    std::filesystem::remove_all(root);

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        seastar::promise<> releaseWal;
        seastar::promise<> releaseDirty;
        auto walHeld = NativeIndexLifecycleTestPeer::holdWalSyncGate(index, releaseWal.get_future());
        auto dirtyHeld = NativeIndexLifecycleTestPeer::holdDirtyCacheGate(index, releaseDirty.get_future());
        EXPECT_EQ(NativeIndexLifecycleTestPeer::walSyncCount(index), 1u);
        EXPECT_EQ(NativeIndexLifecycleTestPeer::dirtyCacheCount(index), 1u);

        auto abandoning = index.abandonForTesting();
        co_await seastar::yield();
        EXPECT_FALSE(abandoning.available()) << "abandon returned while the WAL-sync gate was still held";
        EXPECT_TRUE(NativeIndexLifecycleTestPeer::walSyncClosed(index));

        releaseWal.set_value();
        co_await std::move(walHeld);
        while (!NativeIndexLifecycleTestPeer::dirtyCacheClosed(index)) {
            co_await seastar::yield();
        }
        EXPECT_FALSE(abandoning.available()) << "abandon returned while the dirty-cache gate was still held";

        releaseDirty.set_value();
        co_await std::move(dirtyHeld);
        co_await std::move(abandoning);
        EXPECT_EQ(NativeIndexLifecycleTestPeer::walSyncCount(index), 0u);
        EXPECT_EQ(NativeIndexLifecycleTestPeer::dirtyCacheCount(index), 0u);
    }  // A gate destructor trap or raw-this callback would fail the subprocess.

    std::filesystem::remove_all(root);
}

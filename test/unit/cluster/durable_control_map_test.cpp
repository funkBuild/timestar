#include "../../../lib/cluster/control/durable_control_map.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

using timestar::VIRTUAL_SHARD_COUNT;
using timestar::control::ControlMap;
using timestar::control::DurableControlMapStore;

namespace {

std::filesystem::path freshDir(const std::string& tag) {
    auto path = std::filesystem::temp_directory_path() /
                ("timestar_control_map_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

ControlMap completeMap(uint64_t epoch) {
    ControlMap map;
    map.epoch = epoch;
    for (uint16_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement.emplace(vshard, std::vector<timestar::raft::NodeId>{1, 2, 3});
    return map;
}

}  // namespace

TEST(DurableControlMapStoreTest, MissingCacheIsDistinctFromCorruption) {
    const auto dir = freshDir("missing");
    DurableControlMapStore store(dir);
    EXPECT_FALSE(store.load().has_value());
    std::filesystem::remove_all(dir);
}

TEST(DurableControlMapStoreTest, CompleteMapSurvivesRestart) {
    const auto dir = freshDir("restart");
    const ControlMap expected = completeMap(4096);
    DurableControlMapStore(dir).persist(expected);

    DurableControlMapStore restarted(dir);
    auto recovered = restarted.load();
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(*recovered, expected);
    EXPECT_FALSE(std::filesystem::exists(restarted.path().string() + ".tmp"));
    std::filesystem::remove_all(dir);
}

TEST(DurableControlMapStoreTest, RejectsIncompleteRegressionAndEpochConflict) {
    const auto dir = freshDir("ordering");
    DurableControlMapStore store(dir);
    EXPECT_THROW(store.persist(ControlMap{1, {{0, {1}}}}), std::invalid_argument);
    ControlMap invalid = completeMap(1);
    invalid.placement.at(0) = {1, 1};
    EXPECT_THROW(store.persist(invalid), std::invalid_argument);
    invalid = completeMap(1);
    invalid.groups.emplace(0, VIRTUAL_SHARD_COUNT);
    EXPECT_THROW(store.persist(invalid), std::invalid_argument);

    const ControlMap epoch7 = completeMap(7);
    ASSERT_NO_THROW(store.persist(epoch7));
    EXPECT_NO_THROW(store.persist(epoch7));  // exact retry also re-syncs the directory

    EXPECT_THROW(store.persist(completeMap(6)), std::invalid_argument);
    ControlMap conflict = epoch7;
    conflict.placement.at(17) = {2, 3, 4};
    EXPECT_THROW(store.persist(conflict), std::invalid_argument);
    EXPECT_EQ(*store.load(), epoch7);

    const ControlMap epoch8 = completeMap(8);
    ASSERT_NO_THROW(store.persist(epoch8));
    EXPECT_EQ(*store.load(), epoch8);
    std::filesystem::remove_all(dir);
}

TEST(DurableControlMapStoreTest, CorruptionFailsClosedInsteadOfFallingBack) {
    const auto dir = freshDir("corrupt");
    DurableControlMapStore store(dir);
    store.persist(completeMap(9));

    std::fstream file(store.path(), std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.good());
    file.seekg(-1, std::ios::end);
    char last = 0;
    file.read(&last, 1);
    last ^= 0x01;
    file.seekp(-1, std::ios::end);
    file.write(&last, 1);
    file.close();

    EXPECT_THROW(store.load(), std::runtime_error);
    EXPECT_THROW(store.persist(completeMap(10)), std::runtime_error)
        << "a corrupt last-known map must not be silently overwritten";
    std::filesystem::remove_all(dir);
}

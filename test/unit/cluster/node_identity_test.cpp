// Integration M3 (group-0 bootstrap): the node's persistent cluster identity in
// node.json. node_uuid is minted ONCE and is stable across restarts; cluster_uuid is
// empty until init/join and then persists; a present-but-corrupt identity is never
// silently overwritten.
#include "../../../lib/cluster/integration/node_identity.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

using timestar::cluster::NodeIdentity;

namespace {
// Each test gets its own scratch dir under the build tree's temp area.
std::filesystem::path freshDir(const std::string& tag) {
    auto base = std::filesystem::temp_directory_path() /
                ("node_identity_test_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}
}  // namespace

TEST(NodeIdentity, GeneratesUuidOnFirstStartAndPersists) {
    auto dir = freshDir("first");
    EXPECT_FALSE(std::filesystem::exists(NodeIdentity::pathIn(dir)));

    auto id = NodeIdentity::loadOrCreate(dir);
    EXPECT_EQ(id.node_uuid.size(), 32u);  // 16 bytes hex
    EXPECT_TRUE(id.cluster_uuid.empty());
    EXPECT_TRUE(std::filesystem::exists(NodeIdentity::pathIn(dir)));
    std::filesystem::remove_all(dir);
}

TEST(NodeIdentity, NodeUuidIsStableAcrossRestarts) {
    auto dir = freshDir("stable");
    auto first = NodeIdentity::loadOrCreate(dir);
    auto second = NodeIdentity::loadOrCreate(dir);  // "restart"
    EXPECT_EQ(first.node_uuid, second.node_uuid);
    std::filesystem::remove_all(dir);
}

TEST(NodeIdentity, DistinctDataDirsGetDistinctUuids) {
    auto a = freshDir("distinct_a");
    auto b = freshDir("distinct_b");
    EXPECT_NE(NodeIdentity::loadOrCreate(a).node_uuid, NodeIdentity::loadOrCreate(b).node_uuid);
    std::filesystem::remove_all(a);
    std::filesystem::remove_all(b);
}

TEST(NodeIdentity, ClusterUuidPersistsOnceSet) {
    auto dir = freshDir("cluster");
    auto id = NodeIdentity::loadOrCreate(dir);
    id.cluster_uuid = "cluster-abc";
    id.persist(dir);

    auto reloaded = NodeIdentity::loadOrCreate(dir);
    EXPECT_EQ(reloaded.node_uuid, id.node_uuid);
    EXPECT_EQ(reloaded.cluster_uuid, "cluster-abc");
    std::filesystem::remove_all(dir);
}

TEST(NodeIdentity, CorruptIdentityIsNotSilentlyOverwritten) {
    auto dir = freshDir("corrupt");
    {
        std::ofstream o(NodeIdentity::pathIn(dir), std::ios::binary);
        o << "{not valid json";
    }
    EXPECT_THROW(NodeIdentity::loadOrCreate(dir), std::runtime_error);
    std::filesystem::remove_all(dir);
}

TEST(NodeIdentity, EmptyNodeUuidIsRejected) {
    auto dir = freshDir("empty_uuid");
    {
        std::ofstream o(NodeIdentity::pathIn(dir), std::ios::binary);
        o << R"({"node_uuid":"","cluster_uuid":"x"})";
    }
    EXPECT_THROW(NodeIdentity::loadOrCreate(dir), std::runtime_error);
    std::filesystem::remove_all(dir);
}

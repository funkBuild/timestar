#include "../../../lib/index/native/manifest.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>

using namespace timestar::index;

class ManifestTest : public ::testing::Test {
public:
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "timestar_manifest_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

SEASTAR_TEST_F(ManifestTest, OpenAndClose) {
    auto m = co_await Manifest::open(self->dir_);
    EXPECT_TRUE(m.files().empty());
    EXPECT_EQ(m.currentFileNumber(), 1u);
    co_await m.close();
}

SEASTAR_TEST_F(ManifestTest, AddAndRecover) {
    {
        auto m = co_await Manifest::open(self->dir_);
        SSTableMetadata f1;
        f1.fileNumber = m.nextFileNumber();
        f1.level = 0;
        f1.fileSize = 1000;
        f1.entryCount = 50;
        f1.minKey = "aaa";
        f1.maxKey = "zzz";
        co_await m.addFile(f1);

        SSTableMetadata f2;
        f2.fileNumber = m.nextFileNumber();
        f2.level = 1;
        f2.fileSize = 2000;
        f2.entryCount = 100;
        f2.minKey = "bbb";
        f2.maxKey = "yyy";
        co_await m.addFile(f2);

        EXPECT_EQ(m.files().size(), 2u);
        co_await m.close();
    }

    // Reopen and verify recovery
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 2u);
        EXPECT_EQ(m.files()[0].fileNumber, 1u);
        EXPECT_EQ(m.files()[0].minKey, "aaa");
        EXPECT_EQ(m.files()[1].fileNumber, 2u);
        EXPECT_EQ(m.files()[1].level, 1);
        EXPECT_GE(m.currentFileNumber(), 3u);
        co_await m.close();
    }
}

SEASTAR_TEST_F(ManifestTest, RemoveAndRecover) {
    {
        auto m = co_await Manifest::open(self->dir_);
        SSTableMetadata f1;
        f1.fileNumber = m.nextFileNumber();
        f1.level = 0;
        f1.fileSize = 1000;
        f1.entryCount = 50;
        co_await m.addFile(f1);

        SSTableMetadata f2;
        f2.fileNumber = m.nextFileNumber();
        f2.level = 0;
        f2.fileSize = 2000;
        f2.entryCount = 100;
        co_await m.addFile(f2);

        co_await m.removeFiles({1});
        EXPECT_EQ(m.files().size(), 1u);
        EXPECT_EQ(m.files()[0].fileNumber, 2u);
        co_await m.close();
    }

    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        EXPECT_EQ(m.files()[0].fileNumber, 2u);
        co_await m.close();
    }
}

SEASTAR_TEST_F(ManifestTest, SnapshotCompaction) {
    auto m = co_await Manifest::open(self->dir_);

    for (int i = 0; i < 10; ++i) {
        SSTableMetadata f;
        f.fileNumber = m.nextFileNumber();
        f.level = 0;
        f.fileSize = 1000;
        f.entryCount = 50;
        f.minKey = "key";
        f.maxKey = "key";
        co_await m.addFile(f);
    }

    // Snapshot should compact the manifest file
    co_await m.writeSnapshot();
    EXPECT_EQ(m.files().size(), 10u);

    co_await m.close();

    // Reopen and verify
    auto m2 = co_await Manifest::open(self->dir_);
    EXPECT_EQ(m2.files().size(), 10u);
    co_await m2.close();
}

SEASTAR_TEST_F(ManifestTest, FilesAtLevel) {
    auto m = co_await Manifest::open(self->dir_);

    for (int level = 0; level < 3; ++level) {
        for (int i = 0; i < 3; ++i) {
            SSTableMetadata f;
            f.fileNumber = m.nextFileNumber();
            f.level = level;
            f.fileSize = 1000;
            f.entryCount = 50;
            co_await m.addFile(f);
        }
    }

    auto l0 = m.filesAtLevel(0);
    auto l1 = m.filesAtLevel(1);
    auto l2 = m.filesAtLevel(2);
    EXPECT_EQ(l0.size(), 3u);
    EXPECT_EQ(l1.size(), 3u);
    EXPECT_EQ(l2.size(), 3u);

    auto l3 = m.filesAtLevel(3);
    EXPECT_EQ(l3.size(), 0u);

    co_await m.close();
}

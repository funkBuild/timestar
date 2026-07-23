#include "../../../lib/index/native/manifest.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

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

// ============================================================================
// CRC framing (manifest format v2) tests
// ============================================================================

namespace {

void appendLE32(std::string& out, uint32_t v) {
    char buf[4];
    for (int i = 0; i < 4; ++i)
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    out.append(buf, 4);
}

void appendLE64(std::string& out, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i)
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    out.append(buf, 8);
}

std::string readWholeFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void writeWholeFile(const std::string& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

}  // namespace

// A record whose CRC does not match must stop recovery at that record,
// keeping everything recovered before it (same policy as WAL replay).
SEASTAR_TEST_F(ManifestTest, CorruptRecordCRCStopsRecovery) {
    {
        auto m = co_await Manifest::open(self->dir_);
        SSTableMetadata f1;
        f1.fileNumber = m.nextFileNumber();
        f1.level = 0;
        f1.minKey = "aaa";
        f1.maxKey = "mmm";
        co_await m.addFile(f1);

        SSTableMetadata f2;
        f2.fileNumber = m.nextFileNumber();
        f2.level = 1;
        f2.minKey = "nnn";
        f2.maxKey = "zzz";
        co_await m.addFile(f2);
        co_await m.close();
    }

    // Corrupt the last byte of the file — inside f2's AddFile record payload.
    auto path = self->dir_ + "/MANIFEST";
    auto data = readWholeFile(path);
    EXPECT_FALSE(data.empty());
    if (data.empty())
        co_return;
    data.back() = static_cast<char>(data.back() ^ 0x5A);
    writeWholeFile(path, data);

    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u) << "recovery must stop at the corrupt record";
        if (m.files().size() == 1) {
            EXPECT_EQ(m.files()[0].fileNumber, 1u);
            EXPECT_EQ(m.files()[0].minKey, "aaa");
        }
        co_await m.close();
    }

    // open() must have rewritten a clean snapshot: reopening again succeeds
    // with the same state and new appends are recoverable.
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        SSTableMetadata f3;
        f3.fileNumber = m.nextFileNumber();
        f3.level = 0;
        co_await m.addFile(f3);
        co_await m.close();
    }
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 2u);
        co_await m.close();
    }
}

// Legacy (v1, pre-CRC) manifests must recover correctly and be upgraded to
// the CRC-framed v2 format on open.
SEASTAR_TEST_F(ManifestTest, LegacyFormatBackwardCompat) {
    // Hand-craft a legacy manifest: no header, frames are [len][record].
    // Record: AddFile = type(1) fileNumber(8) level(4) fileSize(8)
    //         entryCount(8) minKeyLen(4)+minKey maxKeyLen(4)+maxKey writeTs(8)
    std::string record;
    record.push_back(static_cast<char>(1));  // RecordType::AddFile
    appendLE64(record, 7);                   // fileNumber
    appendLE32(record, 2);                   // level
    appendLE64(record, 12345);               // fileSize
    appendLE64(record, 678);                 // entryCount
    appendLE32(record, 3);
    record.append("abc");                    // minKey
    appendLE32(record, 3);
    record.append("xyz");                    // maxKey
    appendLE64(record, 999999);              // writeTimestamp

    std::string file;
    appendLE32(file, static_cast<uint32_t>(record.size()));  // legacy frame: no CRC
    file.append(record);

    auto path = self->dir_ + "/MANIFEST";
    writeWholeFile(path, file);

    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        if (m.files().size() != 1) {
            co_await m.close();
            co_return;
        }
        EXPECT_EQ(m.files()[0].fileNumber, 7u);
        EXPECT_EQ(m.files()[0].level, 2);
        EXPECT_EQ(m.files()[0].fileSize, 12345u);
        EXPECT_EQ(m.files()[0].entryCount, 678u);
        EXPECT_EQ(m.files()[0].minKey, "abc");
        EXPECT_EQ(m.files()[0].maxKey, "xyz");
        EXPECT_EQ(m.files()[0].writeTimestamp, 999999u);
        EXPECT_EQ(m.currentFileNumber(), 8u);
        co_await m.close();
    }

    // The file must now be v2: starts with the "TSMF" magic.
    auto upgraded = readWholeFile(path);
    EXPECT_GE(upgraded.size(), 8u);
    EXPECT_EQ(upgraded.substr(0, 4), upgraded.size() >= 4 ? "TSMF" : "");

    // And it must still recover the same state through the v2 path.
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        if (m.files().size() == 1) {
            EXPECT_EQ(m.files()[0].fileNumber, 7u);
            EXPECT_EQ(m.files()[0].maxKey, "xyz");
        }
        co_await m.close();
    }
}

// A torn tail (partial frame from a crash mid-append) must not lose the
// preceding records, and the manifest must be rewritten clean.
SEASTAR_TEST_F(ManifestTest, TornTailDiscardedOnRecovery) {
    {
        auto m = co_await Manifest::open(self->dir_);
        SSTableMetadata f1;
        f1.fileNumber = m.nextFileNumber();
        f1.level = 0;
        f1.minKey = "a";
        f1.maxKey = "z";
        co_await m.addFile(f1);
        co_await m.close();
    }

    // Append a torn frame: a length header promising more bytes than exist.
    auto path = self->dir_ + "/MANIFEST";
    auto data = readWholeFile(path);
    std::string torn;
    appendLE32(torn, 1000);       // record_len
    appendLE32(torn, 0xDEADBEEF); // record_crc
    torn.append("partial");       // far fewer than 1000 bytes
    writeWholeFile(path, data + torn);

    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        if (m.files().size() == 1) {
            EXPECT_EQ(m.files()[0].fileNumber, 1u);
        }
        co_await m.close();
    }

    // Clean after rewrite: no torn bytes remain, state survives reopen.
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 1u);
        co_await m.close();
    }
}

#include "../../../lib/index/native/manifest.hpp"

#include "../../../lib/utils/crc32.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <seastar/core/coroutine.hh>
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
// CRC-framed v1 manifest tests
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

// A fully present record whose CRC does not match is corruption, not a torn
// append. Startup must preserve it and fail closed rather than rewriting away
// an acknowledged manifest mutation.
SEASTAR_TEST_F(ManifestTest, CompleteCorruptRecordFailsClosedAndIsPreserved) {
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

    EXPECT_THROW(co_await Manifest::open(self->dir_), std::runtime_error);
    EXPECT_EQ(readWholeFile(path), data) << "failed recovery must not rewrite the corrupt source";
}

SEASTAR_TEST_F(ManifestTest, CompleteMalformedRecordFailsClosedAndIsPreserved) {
    {
        auto m = co_await Manifest::open(self->dir_);
        co_await m.close();
    }

    auto path = self->dir_ + "/MANIFEST";
    auto data = readWholeFile(path);
    std::string record(1, static_cast<char>(99));  // unknown RecordType
    appendLE32(data, static_cast<uint32_t>(record.size()));
    appendLE32(data, CRC32::compute(record.data(), record.size()));
    data.append(record);
    writeWholeFile(path, data);

    EXPECT_THROW(co_await Manifest::open(self->dir_), std::runtime_error);
    EXPECT_EQ(readWholeFile(path), data);
}

SEASTAR_TEST_F(ManifestTest, CorruptMagicFailsClosed) {
    {
        auto m = co_await Manifest::open(self->dir_);
        co_await m.close();
    }
    auto path = self->dir_ + "/MANIFEST";
    auto data = readWholeFile(path);
    EXPECT_GE(data.size(), Manifest::MANIFEST_HEADER_SIZE);
    if (data.size() < Manifest::MANIFEST_HEADER_SIZE)
        co_return;
    data[0] ^= 0x01;
    writeWholeFile(path, data);

    EXPECT_THROW(co_await Manifest::open(self->dir_), std::runtime_error);
    EXPECT_EQ(readWholeFile(path), data);
}

SEASTAR_TEST_F(ManifestTest, HeaderWithoutSnapshotFailsClosedAndIsPreserved) {
    std::string data;
    appendLE32(data, Manifest::MANIFEST_MAGIC);
    appendLE32(data, Manifest::MANIFEST_VERSION);
    const auto path = self->dir_ + "/MANIFEST";
    writeWholeFile(path, data);

    EXPECT_THROW(co_await Manifest::open(self->dir_), std::runtime_error);
    EXPECT_EQ(readWholeFile(path), data);
}

SEASTAR_TEST_F(ManifestTest, FileNumberExhaustionCannotWrap) {
    std::string snapshot;
    snapshot.push_back(static_cast<char>(0));  // RecordType::Snapshot
    appendLE64(snapshot, std::numeric_limits<uint64_t>::max());
    appendLE32(snapshot, 0);  // file count

    std::string data;
    appendLE32(data, Manifest::MANIFEST_MAGIC);
    appendLE32(data, Manifest::MANIFEST_VERSION);
    appendLE32(data, static_cast<uint32_t>(snapshot.size()));
    appendLE32(data, CRC32::compute(snapshot.data(), snapshot.size()));
    data.append(snapshot);
    writeWholeFile(self->dir_ + "/MANIFEST", data);

    auto m = co_await Manifest::open(self->dir_);
    EXPECT_THROW((void)m.nextFileNumber(), std::overflow_error);
    EXPECT_EQ(m.currentFileNumber(), std::numeric_limits<uint64_t>::max());
    co_await m.close();
}

SEASTAR_TEST_F(ManifestTest, SymlinkedManifestIsRejectedWithoutTouchingTarget) {
    const auto target = self->dir_ + "/manifest-target";
    const auto manifest = self->dir_ + "/MANIFEST";
    const std::string contents = "durable manifest target";
    writeWholeFile(target, contents);
    std::filesystem::create_symlink(std::filesystem::path(target).filename(), manifest);

    EXPECT_THROW(co_await Manifest::open(self->dir_), std::runtime_error);
    EXPECT_EQ(readWholeFile(target), contents);
    EXPECT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(manifest)));
}

SEASTAR_TEST_F(ManifestTest, SymlinkedSnapshotTempIsRejectedWithoutTouchingTarget) {
    auto m = co_await Manifest::open(self->dir_);
    const auto target = self->dir_ + "/temp-target";
    const auto temp = self->dir_ + "/MANIFEST.tmp";
    const std::string contents = "durable temp target";
    writeWholeFile(target, contents);
    std::filesystem::create_symlink(std::filesystem::path(target).filename(), temp);

    EXPECT_THROW(co_await m.writeSnapshot(), std::runtime_error);
    EXPECT_EQ(readWholeFile(target), contents);
    EXPECT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(temp)));
    co_await m.close();
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
    appendLE32(torn, 1000);        // record_len
    appendLE32(torn, 0xDEADBEEF);  // record_crc
    torn.append("partial");        // far fewer than 1000 bytes
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

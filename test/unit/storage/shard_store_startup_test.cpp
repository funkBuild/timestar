#include "../../../lib/storage/shard_store_startup.hpp"

#include "../../../lib/index/native/manifest_format.hpp"
#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

void appendLE32(std::string& output, uint32_t value) {
    for (int byte = 0; byte < 4; ++byte)
        output.push_back(static_cast<char>((value >> (byte * 8)) & 0xff));
}

void appendLE64(std::string& output, uint64_t value) {
    for (int byte = 0; byte < 8; ++byte)
        output.push_back(static_cast<char>((value >> (byte * 8)) & 0xff));
}

void appendV2Record(std::string& output, const std::string& record) {
    appendLE32(output, static_cast<uint32_t>(record.size()));
    appendLE32(output, CRC32::compute(record.data(), record.size()));
    output.append(record);
}

std::string emptyV2Manifest() {
    std::string manifest;
    appendLE32(manifest, timestar::index::MANIFEST_MAGIC);
    appendLE32(manifest, timestar::index::MANIFEST_VERSION);

    std::string snapshot;
    snapshot.push_back(0);  // Snapshot
    appendLE64(snapshot, 1);
    appendLE32(snapshot, 0);
    appendV2Record(manifest, snapshot);
    return manifest;
}

std::string v2ManifestReferencing(uint64_t fileNumber, uint64_t fileSize) {
    std::string manifest = emptyV2Manifest();
    std::string addFile;
    addFile.push_back(1);  // AddFile
    appendLE64(addFile, fileNumber);
    appendLE32(addFile, 0);  // level
    appendLE64(addFile, fileSize);
    appendLE64(addFile, 0);  // entry count
    appendLE32(addFile, 0);  // minimum key length
    appendLE32(addFile, 0);  // maximum key length
    appendLE64(addFile, 0);  // write timestamp
    appendV2Record(manifest, addFile);
    return manifest;
}

std::string snapshotManifest(unsigned fileCount, bool timestampsPresent, bool crcFramed) {
    std::string snapshot;
    snapshot.push_back(0);  // Snapshot
    appendLE64(snapshot, static_cast<uint64_t>(fileCount) + 1);
    appendLE32(snapshot, fileCount);
    for (unsigned index = 0; index < fileCount; ++index) {
        const uint64_t fileNumber = static_cast<uint64_t>(index) + 1;
        appendLE64(snapshot, fileNumber);
        appendLE32(snapshot, index);
        appendLE64(snapshot, fileNumber + 2);
        appendLE64(snapshot, fileNumber + 10);
        appendLE32(snapshot, 1);
        snapshot.push_back('a');
        appendLE32(snapshot, 1);
        snapshot.push_back('z');
        if (timestampsPresent)
            appendLE64(snapshot, fileNumber + 100);
    }

    std::string manifest;
    if (crcFramed) {
        appendLE32(manifest, timestar::index::MANIFEST_MAGIC);
        appendLE32(manifest, timestar::index::MANIFEST_VERSION);
        appendV2Record(manifest, snapshot);
    } else {
        appendLE32(manifest, static_cast<uint32_t>(snapshot.size()));
        manifest.append(snapshot);
    }
    return manifest;
}

std::string emptyLegacyManifest() {
    return snapshotManifest(0, false, false);
}

std::string ambiguousHistoricalSnapshot() {
    std::string snapshot;
    snapshot.push_back(0);  // Snapshot
    appendLE64(snapshot, 3);
    appendLE32(snapshot, 2);

    appendLE64(snapshot, 1);
    appendLE32(snapshot, 0);
    appendLE64(snapshot, 3);
    appendLE64(snapshot, 11);
    appendLE32(snapshot, 1);
    snapshot.push_back('a');
    appendLE32(snapshot, 1);
    snapshot.push_back('z');

    appendLE64(snapshot, 2);
    appendLE32(snapshot, 1);
    appendLE64(snapshot, 5);
    appendLE64(snapshot, 12);
    appendLE32(snapshot, 12);
    snapshot.append("abcd", 4);
    snapshot.append(8, '\0');
    appendLE32(snapshot, 4);
    snapshot.append("WXYZ", 4);

    std::string manifest;
    appendLE32(manifest, static_cast<uint32_t>(snapshot.size()));
    manifest.append(snapshot);
    return manifest;
}

std::string incrementDecimal(std::string value) {
    for (auto it = value.rbegin(); it != value.rend(); ++it) {
        if (*it != '9') {
            ++*it;
            return value;
        }
        *it = '0';
    }
    value.insert(value.begin(), '1');
    return value;
}

class CurrentPathGuard {
public:
    CurrentPathGuard() : original_(fs::current_path()) {}
    ~CurrentPathGuard() {
        std::error_code error;
        fs::current_path(original_, error);
    }

    [[nodiscard]] const fs::path& original() const noexcept { return original_; }

private:
    fs::path original_;
};

class ShardStoreStartupTest : public ::testing::Test {
protected:
    fs::path testDir;

    void SetUp() override {
        static std::atomic_uint64_t sequence{0};
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        testDir = fs::temp_directory_path() / (std::string("timestar_shard_startup_") + std::to_string(::getpid()) +
                                               "_" + std::to_string(sequence.fetch_add(1)) + "_" + info->name());
        fs::remove_all(testDir);
        fs::create_directories(testDir);
    }

    void TearDown() override { fs::remove_all(testDir); }

    void writeFile(const fs::path& relativePath, const std::string& contents) {
        const auto path = testDir / relativePath;
        if (!path.parent_path().empty())
            fs::create_directories(path.parent_path());
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output) << path;
        output << contents;
        output.close();
    }

    void createCompleteShards(unsigned count) {
        for (unsigned shard = 0; shard < count; ++shard) {
            const auto shardDir = testDir / ("shard_" + std::to_string(shard));
            fs::create_directories(shardDir / "tsm");
            fs::create_directories(shardDir / "native_index");
            writeFile(shardDir.lexically_relative(testDir) / "native_index/MANIFEST", emptyV2Manifest());
        }
    }

    timestar::ShardStoreInspection inspect(timestar::ShardStoreStartup& startup, unsigned count) {
        auto lock = startup.acquireExclusiveLock();
        return startup.inspect(count, lock);
    }

    void createCommittedStore(unsigned count) {
        timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
        auto lock = startup.acquireExclusiveLock();
        const auto fresh = startup.inspect(count, lock);
        ASSERT_EQ(fresh.status, timestar::ShardStoreStartupStatus::FreshStore);
        createCompleteShards(count);
        startup.commitAfterInitialization(fresh, lock);
    }
};

TEST_F(ShardStoreStartupTest, FreshInspectionIsReadOnly) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();

    const auto result = startup.inspect(4, lock);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::FreshStore);
    EXPECT_TRUE(result.canStart());
    EXPECT_TRUE(result.isFresh());
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));
    EXPECT_TRUE(fs::is_empty(testDir));
}

TEST_F(ShardStoreStartupTest, DecommissionedWorkerArtifactsFailClosed) {
    const std::vector<std::string> artifacts = {"workers.json", "workers.json.tmp", "vshard_ownership.manifest",
                                                "vshard_ownership.initializing"};
    for (const auto& artifact : artifacts) {
        SCOPED_TRACE(artifact);
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        writeFile(artifact, "left behind by a pre-release build");

        timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
        const auto result = inspect(startup, 2);

        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
        EXPECT_FALSE(result.canStart());
        EXPECT_NE(result.detail.find("decommissioned VShard-worker artifact"), std::string::npos) << result.detail;
        EXPECT_NE(result.detail.find(artifact), std::string::npos) << result.detail;
    }
}

TEST_F(ShardStoreStartupTest, DecommissionedOwnershipDirectoryFailsClosedEvenInACommittedStore) {
    createCommittedStore(2);
    fs::create_directories(testDir / "vshard_ownership");

    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const auto result = inspect(startup, 2);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(result.canStart());
    EXPECT_NE(result.detail.find("vshard_ownership"), std::string::npos) << result.detail;
}

TEST_F(ShardStoreStartupTest, RelativeLayoutRemainsBoundToItsConstructionDirectory) {
    const auto relativeRoot =
        fs::path("shard startup relative root") /
        (std::to_string(::getpid()) + "_" + ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::path expectedRoot;

    {
        CurrentPathGuard cwd;
        expectedRoot = cwd.original() / relativeRoot;
        fs::remove_all(expectedRoot);
        fs::create_directories(testDir / "alternate cwd");

        timestar::ShardStoreStartup startup{timestar::StorageLayout(relativeRoot)};
        fs::current_path(testDir / "alternate cwd");
        auto lock = startup.acquireExclusiveLock();
        const auto result = startup.inspect(2, lock);

        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::FreshStore);
        EXPECT_TRUE(fs::is_directory(expectedRoot));
        EXPECT_FALSE(fs::exists(testDir / "alternate cwd" / relativeRoot));
    }

    fs::remove_all(expectedRoot);
    std::error_code cleanupEc;
    fs::remove(expectedRoot.parent_path(), cleanupEc);  // parent is shared across param runs; drop it once empty
}

TEST_F(ShardStoreStartupTest, LockCreatesMissingRootButInspectionCreatesNoStoreArtifacts) {
    fs::remove_all(testDir);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    auto lock = startup.acquireExclusiveLock();
    const auto result = startup.inspect(2, lock);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::FreshStore);
    EXPECT_TRUE(fs::is_directory(testDir));
    EXPECT_TRUE(fs::is_empty(testDir));
}

TEST_F(ShardStoreStartupTest, ExclusiveRootLockRejectsASecondOwner) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto first = startup.acquireExclusiveLock();

    EXPECT_THROW(
        {
            auto second = startup.acquireExclusiveLock();
            (void)second;
        },
        std::system_error);
}

TEST_F(ShardStoreStartupTest, RootLockRejectsSymlinkedDataRoot) {
    const auto realRoot = testDir.string() + "_real";
    fs::remove_all(testDir);
    fs::create_directories(realRoot);
    fs::create_directory_symlink(realRoot, testDir);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    EXPECT_THROW(
        {
            auto lock = startup.acquireExclusiveLock();
            (void)lock;
        },
        std::system_error);

    fs::remove(testDir);
    fs::remove_all(realRoot);
}

TEST_F(ShardStoreStartupTest, InspectionRejectsAReplacedDataRootPath) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();
    const auto movedRoot = fs::path(testDir.string() + "_locked_inode");
    fs::remove_all(movedRoot);
    fs::rename(testDir, movedRoot);
    fs::create_directories(testDir);

    const auto result = startup.inspect(2, lock);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("replaced"), std::string::npos);

    fs::remove_all(testDir);
    fs::rename(movedRoot, testDir);
}

TEST_F(ShardStoreStartupTest, FreshLayoutCommitsOnlyAfterCompleteShardStructureExists) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();
    const auto fresh = startup.inspect(2, lock);
    createCompleteShards(2);

    startup.commitAfterInitialization(fresh, lock);

    EXPECT_EQ(startup.inspect(2, lock).status, timestar::ShardStoreStartupStatus::MatchingShardCount);
    EXPECT_TRUE(fs::exists(testDir / "shard_count.meta"));
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta.tmp"));
}

TEST_F(ShardStoreStartupTest, FreshCommitRejectsIncompleteEngineLayout) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();
    const auto fresh = startup.inspect(2, lock);
    fs::create_directories(testDir / "shard_0/tsm");

    EXPECT_THROW(startup.commitAfterInitialization(fresh, lock), std::runtime_error);
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta.tmp"));
}

TEST_F(ShardStoreStartupTest, FreshCommitRejectsRebalanceStateInsertedAfterInspection) {
    const std::vector<std::string> states = {
        "1 2 4\n",
        "malformed\n",
    };

    for (const auto& state : states) {
        SCOPED_TRACE(state);
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
        auto lock = startup.acquireExclusiveLock();
        const auto fresh = startup.inspect(2, lock);
        ASSERT_EQ(fresh.status, timestar::ShardStoreStartupStatus::FreshStore);
        createCompleteShards(2);
        writeFile("rebalance.state", state);

        EXPECT_THROW(startup.commitAfterInitialization(fresh, lock), std::runtime_error);
        EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));
    }
}

TEST_F(ShardStoreStartupTest, StartupSessionEnforcesInitializationCommitOrder) {
    timestar::ShardStoreStartupSession session(timestar::StorageLayout(testDir), 2);
    ASSERT_TRUE(session.canStart());

    EXPECT_THROW(session.commitEngineInitialization(), std::logic_error);
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));

    session.authorizeFirstStorageMutation();
    createCompleteShards(2);
    session.commitEngineInitialization();

    EXPECT_TRUE(fs::exists(testDir / "shard_count.meta"));
    EXPECT_THROW(session.authorizeFirstStorageMutation(), std::logic_error);
    EXPECT_THROW(session.commitEngineInitialization(), std::logic_error);
}

TEST_F(ShardStoreStartupTest, StartupSessionRevalidatesBeforeTheFirstMutation) {
    timestar::ShardStoreStartupSession session(timestar::StorageLayout(testDir), 2);
    ASSERT_TRUE(session.canStart());
    writeFile("shard_0.partial", "unexpected");

    EXPECT_THROW(session.authorizeFirstStorageMutation(), std::runtime_error);
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));
}

TEST_F(ShardStoreStartupTest, MatchingStoreCommitHookIsANoOp) {
    createCommittedStore(2);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();
    const auto matching = startup.inspect(2, lock);
    ASSERT_EQ(matching.status, timestar::ShardStoreStartupStatus::MatchingShardCount);

    struct stat before{};
    ASSERT_EQ(::stat((testDir / "shard_count.meta").c_str(), &before), 0);
    startup.commitAfterInitialization(matching, lock);
    struct stat after{};
    ASSERT_EQ(::stat((testDir / "shard_count.meta").c_str(), &after), 0);

    EXPECT_EQ(before.st_ino, after.st_ino);
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta.tmp"));
}

TEST_F(ShardStoreStartupTest, MatchingStoreCommitRejectsRebalanceStateInsertedAfterAuthorization) {
    const std::vector<std::string> states = {
        "1 2 2\n",
        "malformed\n",
    };

    for (const auto& state : states) {
        SCOPED_TRACE(state);
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        createCommittedStore(2);
        timestar::ShardStoreStartupSession session(timestar::StorageLayout(testDir), 2);
        ASSERT_TRUE(session.canStart());
        session.authorizeFirstStorageMutation();
        writeFile("rebalance.state", state);

        EXPECT_THROW(session.commitEngineInitialization(), std::runtime_error);
        EXPECT_EQ(std::ifstream(testDir / "shard_count.meta").peek(), '2');
    }
}

TEST_F(ShardStoreStartupTest, MatchingStoreCommitRejectsAReplacedDataRootPath) {
    createCommittedStore(1);
    timestar::ShardStoreStartupSession session(timestar::StorageLayout(testDir), 1);
    ASSERT_TRUE(session.canStart());
    session.authorizeFirstStorageMutation();

    const auto movedRoot = fs::path(testDir.string() + "_matching_locked_inode");
    fs::remove_all(movedRoot);
    fs::rename(testDir, movedRoot);
    fs::create_directories(testDir);

    EXPECT_THROW(session.commitEngineInitialization(), std::runtime_error);
    EXPECT_FALSE(fs::exists(testDir / "shard_count.meta"));

    fs::remove_all(testDir);
    fs::rename(movedRoot, testDir);
}

TEST_F(ShardStoreStartupTest, UnsafeInspectionCannotBeCommitted) {
    createCommittedStore(2);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    auto lock = startup.acquireExclusiveLock();
    const auto mismatch = startup.inspect(4, lock);

    EXPECT_THROW(startup.commitAfterInitialization(mismatch, lock), std::logic_error);
}

TEST_F(ShardStoreStartupTest, PersistedMatchingCountAndStructureAreAccepted) {
    createCommittedStore(4);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 4);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::MatchingShardCount);
    EXPECT_EQ(result.previousShardCount, 4u);
    EXPECT_TRUE(result.canStart());
}

TEST_F(ShardStoreStartupTest, CoreCountMismatchIsRejectedWithoutMutation) {
    createCommittedStore(2);
    writeFile("shard_0/tsm/sentinel.tsm", "unchanged-data");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 4);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::UnsafeShardCountChange);
    EXPECT_EQ(result.previousShardCount, 2u);
    EXPECT_EQ(result.requestedShardCount, 4u);
    EXPECT_FALSE(result.canStart());
    EXPECT_TRUE(fs::exists(testDir / "shard_0/tsm/sentinel.tsm"));
    EXPECT_TRUE(fs::exists(testDir / "shard_1"));
    EXPECT_FALSE(fs::exists(testDir / "shard_0_old"));
    EXPECT_FALSE(fs::exists(testDir / "shard_0_new"));
    EXPECT_FALSE(fs::exists(testDir / "rebalance.state"));

    std::ifstream sentinel(testDir / "shard_0/tsm/sentinel.tsm");
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(sentinel), {}), "unchanged-data");
}

TEST_F(ShardStoreStartupTest, DirectoryOnlyStoreIsUncommittedNotMatching) {
    createCompleteShards(3);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 3);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::UncommittedInitialization);
    EXPECT_FALSE(result.canStart());
}

TEST_F(ShardStoreStartupTest, MetadataOnlyStoreIsIncompleteNotMatching) {
    writeFile("shard_count.meta", "3\n");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 3);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_FALSE(result.canStart());
}

TEST_F(ShardStoreStartupTest, MissingNativeIndexManifestIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    fs::create_directories(testDir / "shard_0/tsm");
    fs::create_directories(testDir / "shard_0/native_index");
    writeFile("shard_0/tsm/data.tsm", "data-needing-index");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("MANIFEST"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, EmptyNativeIndexManifestIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", "");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("empty"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, RandomNativeIndexManifestIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", "not-a-manifest");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("MANIFEST is not fully recoverable"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, TruncatedNativeIndexManifestIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    auto manifest = emptyV2Manifest();
    manifest.pop_back();
    writeFile("shard_0/native_index/MANIFEST", manifest);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("truncated"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, UnsupportedNativeIndexManifestVersionIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    auto manifest = emptyV2Manifest();
    manifest[4] = 99;
    writeFile("shard_0/native_index/MANIFEST", manifest);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("unsupported manifest version"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, BadNativeIndexManifestCrcIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    auto manifest = emptyV2Manifest();
    manifest.back() ^= 0x5a;
    writeFile("shard_0/native_index/MANIFEST", manifest);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("CRC mismatch"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, SharedManifestDecoderPreservesOnlyACompletePrefixForRuntimeRecovery) {
    auto manifest = v2ManifestReferencing(7, 123);
    appendLE32(manifest, 1000);
    appendLE32(manifest, 0xdeadbeef);
    manifest.append("partial");

    const auto decoded = timestar::index::decodeManifest(manifest);

    EXPECT_EQ(decoded.status, timestar::index::ManifestDecodeStatus::RecoverableTail);
    ASSERT_EQ(decoded.files.size(), 1u);
    EXPECT_EQ(decoded.files.front().fileNumber, 7u);
    EXPECT_NE(decoded.issue.find("truncated"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, ChecksummedSemanticManifestCorruptionIsFatal) {
    auto manifest = emptyV2Manifest();
    const std::string unknownRecord(1, static_cast<char>(99));
    appendV2Record(manifest, unknownRecord);

    const auto decoded = timestar::index::decodeManifest(manifest);

    EXPECT_EQ(decoded.status, timestar::index::ManifestDecodeStatus::Fatal);
    EXPECT_NE(decoded.issue.find("unknown manifest record type"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, CompleteLegacyNativeIndexManifestRemainsReadable) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", emptyLegacyManifest());
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::MatchingShardCount);
}

TEST_F(ShardStoreStartupTest, HistoricalSnapshotsWithoutTimestampsRemainReadable) {
    for (const bool crcFramed : {false, true}) {
        for (const unsigned fileCount : {1u, 3u}) {
            SCOPED_TRACE(std::string(crcFramed ? "v2" : "legacy") + " files=" + std::to_string(fileCount));
            const auto decoded = timestar::index::decodeManifest(snapshotManifest(fileCount, false, crcFramed));

            ASSERT_EQ(decoded.status, timestar::index::ManifestDecodeStatus::Complete);
            ASSERT_EQ(decoded.files.size(), fileCount);
            EXPECT_EQ(decoded.nextFileNumber, static_cast<uint64_t>(fileCount) + 1);
            for (const auto& file : decoded.files)
                EXPECT_EQ(file.writeTimestamp, 0u);
        }
    }
}

TEST_F(ShardStoreStartupTest, CurrentMultiFileSnapshotsPreserveEveryTimestamp) {
    const auto decoded = timestar::index::decodeManifest(snapshotManifest(3, true, true));

    ASSERT_EQ(decoded.status, timestar::index::ManifestDecodeStatus::Complete);
    ASSERT_EQ(decoded.files.size(), 3u);
    EXPECT_EQ(decoded.files[0].writeTimestamp, 101u);
    EXPECT_EQ(decoded.files[1].writeTimestamp, 102u);
    EXPECT_EQ(decoded.files[2].writeTimestamp, 103u);
}

TEST_F(ShardStoreStartupTest, HistoricalSnapshotFallsBackAfterAFalseTimestampedSemanticMatch) {
    const auto decoded = timestar::index::decodeManifest(ambiguousHistoricalSnapshot());

    ASSERT_EQ(decoded.status, timestar::index::ManifestDecodeStatus::Complete);
    ASSERT_EQ(decoded.files.size(), 2u);
    EXPECT_EQ(decoded.files[0].fileNumber, 1u);
    EXPECT_EQ(decoded.files[1].fileNumber, 2u);
    EXPECT_EQ(decoded.files[1].minKey.size(), 12u);
    EXPECT_EQ(decoded.files[1].maxKey, "WXYZ");
    EXPECT_EQ(decoded.files[1].writeTimestamp, 0u);
}

TEST_F(ShardStoreStartupTest, HistoricalOneFileSnapshotCanStartACommittedStore) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", snapshotManifest(1, false, false));
    writeFile("shard_0/native_index/idx_000001.sst", std::string(3, 'x'));
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::MatchingShardCount);
}

TEST_F(ShardStoreStartupTest, MissingManifestReferencedSstableIsIncomplete) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", v2ManifestReferencing(7, 123));
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("idx_000007.sst"), std::string::npos);
    EXPECT_NE(result.detail.find("referenced by MANIFEST"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, ManifestReferencedSstableMustBeARegularFileOfTheCommittedSize) {
    writeFile("shard_count.meta", "1\n");
    createCompleteShards(1);
    writeFile("shard_0/native_index/MANIFEST", v2ManifestReferencing(7, 123));
    fs::create_directories(testDir / "shard_0/native_index/idx_000007.sst");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    auto result = inspect(startup, 1);
    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("not a regular file"), std::string::npos);

    fs::remove_all(testDir / "shard_0/native_index/idx_000007.sst");
    writeFile("shard_0/native_index/idx_000007.sst", std::string(122, 'x'));
    result = inspect(startup, 1);
    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_NE(result.detail.find("size does not match"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, MalformedShardCountMetadataIsRejected) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const std::vector<std::string> invalidContents = {
        "",      "0\n",          "-1\n",
        "two\n", "2 trailing\n", incrementDecimal(std::to_string(std::numeric_limits<unsigned>::max())) + "\n",
    };

    for (const auto& contents : invalidContents) {
        SCOPED_TRACE(contents);
        writeFile("shard_count.meta", contents);
        const auto result = inspect(startup, 2);
        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
        EXPECT_FALSE(result.canStart());
    }
}

TEST_F(ShardStoreStartupTest, OversizedControlMetadataIsRejected) {
    writeFile("shard_count.meta", std::string(5000, '1'));
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 2);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("4096-byte"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, SymlinkedControlMetadataIsRejected) {
    writeFile("count-target", "2\n");
    fs::create_symlink(testDir / "count-target", testDir / "shard_count.meta");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 2);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(result.canStart());
}

TEST_F(ShardStoreStartupTest, EveryLegacyRebalancePhaseBlocksStartup) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const std::vector<std::string> states = {
        "0 0 0\n",
        "1 2 4\n",
        "2 2 4\n",
        "3 2 4\n",
    };

    for (const auto& state : states) {
        SCOPED_TRACE(state);
        writeFile("rebalance.state", state);
        const auto result = inspect(startup, 4);
        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InterruptedLegacyRebalance);
        EXPECT_FALSE(result.canStart());
    }
}

TEST_F(ShardStoreStartupTest, MalformedLegacyRebalanceStateIsInvalidMetadata) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const std::vector<std::string> states = {
        "", "1 2\n", "4 2 4\n", "1 zero 4\n", "1 2 4 trailing\n",
    };

    for (const auto& state : states) {
        SCOPED_TRACE(state);
        writeFile("rebalance.state", state);
        const auto result = inspect(startup, 4);
        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
        EXPECT_FALSE(result.canStart());
    }
}

TEST_F(ShardStoreStartupTest, OrphanedLegacyArtifactsBlockStartup) {
    const std::vector<fs::path> artifacts = {
        "shard_0_new",
        "shard_0_old",
        "rebalance.state.tmp",
        "shard_count.meta.tmp",
    };

    for (const auto& artifact : artifacts) {
        SCOPED_TRACE(artifact.string());
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        if (artifact.extension() == ".tmp")
            writeFile(artifact, "partial");
        else
            fs::create_directories(testDir / artifact);

        timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
        const auto result = inspect(startup, 2);
        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InterruptedLegacyRebalance);
        EXPECT_FALSE(result.canStart());
    }
}

TEST_F(ShardStoreStartupTest, UnknownReservedArtifactsAreRejected) {
    const std::vector<fs::path> artifacts = {
        "shard_0.partial", "shard_bad", "shard_0_new.tmp", "shard_count.meta.partial", "rebalance.state.bak",
    };

    for (const auto& artifact : artifacts) {
        SCOPED_TRACE(artifact.string());
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        writeFile(artifact, "stranded");

        timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
        const auto result = inspect(startup, 2);
        EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
        EXPECT_NE(result.detail.find(artifact.string()), std::string::npos);
    }
}

TEST_F(ShardStoreStartupTest, SparseShardDirectoriesAreInvalid) {
    fs::create_directories(testDir / "shard_0/tsm");
    fs::create_directories(testDir / "shard_2/tsm");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 3);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("not contiguous"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, NonCanonicalShardDirectoryIsInvalid) {
    fs::create_directories(testDir / "shard_00/tsm");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("invalid shard directory name"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, SymlinkedShardDirectoryIsInvalid) {
    createCompleteShards(1);
    fs::rename(testDir / "shard_0", testDir / "real-shard");
    fs::create_directory_symlink(testDir / "real-shard", testDir / "shard_0");
    writeFile("shard_count.meta", "1\n");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("not a real directory"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, ShardPathMustBeADirectory) {
    writeFile("shard_0", "not-a-directory");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 1);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_NE(result.detail.find("not a real directory"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, MetadataMustAgreeWithActiveDirectories) {
    writeFile("shard_count.meta", "4\n");
    createCompleteShards(2);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 4);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::IncompleteStore);
    EXPECT_EQ(result.previousShardCount, 4u);
    EXPECT_NE(result.detail.find("requires 4"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, ZeroRequestedCountIsInvalid) {
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};

    const auto result = inspect(startup, 0);

    EXPECT_EQ(result.status, timestar::ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(result.canStart());
}

TEST_F(ShardStoreStartupTest, MismatchMessageNamesExactSafeCoreCount) {
    createCommittedStore(2);
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const auto result = inspect(startup, 4);

    const auto message = startup.failureMessage(result);

    EXPECT_NE(message.find(fs::absolute(testDir).string()), std::string::npos);
    EXPECT_NE(message.find("Requested --smp 4"), std::string::npos);
    EXPECT_NE(message.find("requires --smp 2"), std::string::npos);
    EXPECT_NE(message.find("No files were migrated"), std::string::npos);
    EXPECT_NE(message.find("future VShard format"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, InvalidAndInterruptedMessagesDoNotClaimAnExistingTool) {
    writeFile("shard_count.meta", "broken");
    timestar::ShardStoreStartup startup{timestar::StorageLayout(testDir)};
    const auto invalid = inspect(startup, 2);
    const auto invalidMessage = startup.failureMessage(invalid);
    EXPECT_NE(invalidMessage.find("No automatic metadata repair or migration tool"), std::string::npos);

    fs::remove(testDir / "shard_count.meta");
    writeFile("rebalance.state", "1 2 4\n");
    const auto interrupted = inspect(startup, 4);
    const auto interruptedMessage = startup.failureMessage(interrupted);
    EXPECT_NE(interruptedMessage.find("will not resume or delete"), std::string::npos);
    EXPECT_NE(interruptedMessage.find("no automatic recovery tool"), std::string::npos);
}

TEST_F(ShardStoreStartupTest, ServerUsesTheStatefulStartupSessionAroundEngineInitialization) {
#ifndef HTTP_SERVER_SOURCE_PATH
    GTEST_SKIP() << "HTTP_SERVER_SOURCE_PATH is not configured";
#else
    std::ifstream source(HTTP_SERVER_SOURCE_PATH);
    ASSERT_TRUE(source);
    const std::string text(std::istreambuf_iterator<char>(source), {});

    const auto layoutPosition = text.find("StorageLayout(timestar::dataRootPath()).anchored()");
    const auto sessionPosition = text.find("ShardStoreStartupSession shardStoreStartup(storageLayout,");
    const auto authorizePosition = text.find("shardStoreStartup.authorizeFirstStorageMutation");
    const auto placementWritePosition = text.find("savePlacement(storageLayout.placementFile().string())");
    const auto engineStartPosition = text.find("g_engine.start(storageLayout).get()");
    const auto engineInitPosition = text.find("engine.init()");
    const auto commitPosition = text.find("shardStoreStartup.commitEngineInitialization");

    ASSERT_NE(layoutPosition, std::string::npos);
    ASSERT_NE(sessionPosition, std::string::npos);
    ASSERT_NE(authorizePosition, std::string::npos);
    ASSERT_NE(placementWritePosition, std::string::npos);
    ASSERT_NE(engineStartPosition, std::string::npos);
    ASSERT_NE(engineInitPosition, std::string::npos);
    ASSERT_NE(commitPosition, std::string::npos);
    EXPECT_LT(layoutPosition, sessionPosition);
    EXPECT_LT(sessionPosition, authorizePosition);
    EXPECT_LT(authorizePosition, placementWritePosition);
    EXPECT_LT(placementWritePosition, engineStartPosition);
    EXPECT_LT(engineInitPosition, commitPosition);
    EXPECT_EQ(text.find("recoverIfNeeded"), std::string::npos);
    EXPECT_EQ(text.find("rebalancer.execute"), std::string::npos);
    EXPECT_EQ(text.find("ShardRebalancer"), std::string::npos);
#endif
}

}  // namespace

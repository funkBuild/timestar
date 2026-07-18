#include "../../../lib/cluster/worker_registry_store.hpp"

#include "../../../lib/storage/shard_store_startup.hpp"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using timestar::ShardStoreStartup;
using timestar::StorageLayout;
using timestar::cluster::createWorkerRegistry;
using timestar::cluster::encodeWorkerRegistryJson;
using timestar::cluster::reconcileWorkerRegistry;
using timestar::cluster::WorkerRegistry;
using timestar::cluster::WorkerRegistryCommitStage;
using timestar::cluster::WorkerRegistryStore;

constexpr int simulatedCrashExitCode = 73;

class WorkerRegistryStoreTest : public ::testing::Test {
protected:
    fs::path testDir;

    void SetUp() override {
        static std::atomic_uint64_t sequence{0};
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        testDir =
            fs::temp_directory_path() / (std::string("timestar_worker_registry_store_") + std::to_string(::getpid()) +
                                         "_" + std::to_string(sequence.fetch_add(1)) + "_" + info->name());
        fs::remove_all(testDir);
        fs::create_directories(testDir);
    }

    void TearDown() override { fs::remove_all(testDir); }

    static void writeFile(const fs::path& path, const std::string& contents) {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output) << path;
        output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        output.close();
        ASSERT_TRUE(output) << path;
    }

    static std::string readFile(const fs::path& path) {
        std::ifstream input(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), {});
    }

    static void crashInstallAt(const StorageLayout& layout, const WorkerRegistry& registry,
                               WorkerRegistryCommitStage crashStage) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            try {
                ShardStoreStartup childStartup(layout);
                auto childLock = childStartup.acquireExclusiveLock();
                WorkerRegistryStore crashingStore(layout, [crashStage](WorkerRegistryCommitStage stage) {
                    if (stage == crashStage)
                        ::_exit(simulatedCrashExitCode);
                });
                crashingStore.install(registry, childLock);
                ::_exit(0);
            } catch (...) {
                ::_exit(91);
            }
        }

        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), simulatedCrashExitCode);
    }

    static void crashRecoveryAt(const StorageLayout& layout, WorkerRegistryCommitStage crashStage) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            try {
                ShardStoreStartup childStartup(layout);
                auto childLock = childStartup.acquireExclusiveLock();
                WorkerRegistryStore crashingStore(layout, [crashStage](WorkerRegistryCommitStage stage) {
                    if (stage == crashStage)
                        ::_exit(simulatedCrashExitCode);
                });
                const auto ignored = crashingStore.loadAndRecover(childLock);
                (void)ignored;
                ::_exit(0);
            } catch (...) {
                ::_exit(91);
            }
        }

        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), simulatedCrashExitCode);
    }
};

TEST_F(WorkerRegistryStoreTest, InitialInstallIsCanonicalDurableAndReloadable) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(3);

    EXPECT_FALSE(store.loadAndRecover(lock).has_value());
    store.install(initial, lock);

    ASSERT_TRUE(store.loadAndRecover(lock).has_value());
    EXPECT_EQ(*store.loadAndRecover(lock), initial);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, UpdateValidatesTransitionAndNoOpPreservesInode) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    store.install(initial, lock);

    struct stat before{};
    ASSERT_EQ(::stat(layout.workerRegistryFile().c_str(), &before), 0);
    store.install(initial, lock);
    struct stat afterNoOp{};
    ASSERT_EQ(::stat(layout.workerRegistryFile().c_str(), &afterNoOp), 0);
    EXPECT_EQ(before.st_ino, afterNoOp.st_ino);

    store.install(next, lock);
    EXPECT_EQ(*store.loadAndRecover(lock), next);
    EXPECT_THROW(store.install(initial, lock), std::invalid_argument);
    EXPECT_EQ(*store.loadAndRecover(lock), next);
}

TEST_F(WorkerRegistryStoreTest, FreshRootRejectsNonInitialGenerationWithoutArtifacts) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto nonInitial = reconcileWorkerRegistry(createWorkerRegistry(2), 3);

    EXPECT_THROW(store.install(nonInitial, lock), std::invalid_argument);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, RecoveryRollsForwardOnlyAnExactNewerTransition) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    store.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    EXPECT_EQ(*store.loadAndRecover(lock), next);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(next));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, RecoveryCleansStaleAndPartialScratchButPreservesAcceptedState) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    store.install(initial, lock);
    store.install(next, lock);

    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(initial));
    EXPECT_EQ(*store.loadAndRecover(lock), next);
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));

    writeFile(layout.workerRegistryTemporaryFile(), "partial");
    EXPECT_EQ(*store.loadAndRecover(lock), next);
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, InvalidAuthoritativeStateIsNeverMaskedByTemporaryState) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    store.install(initial, lock);
    writeFile(layout.workerRegistryFile(), "corrupt");
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(fs::exists(layout.workerRegistryFile()));
    EXPECT_TRUE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, SameGenerationDivergenceFailsClosedAndPreservesBothFiles) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(3);
    auto divergent = initial;
    divergent.workers[2].state = timestar::cluster::StorageWorkerState::Draining;
    writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(initial));
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(divergent));

    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(fs::exists(layout.workerRegistryFile()));
    EXPECT_TRUE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, MissingFinalPromotesOnlyExactInitialTemporaryRegistry) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(initial));

    EXPECT_EQ(*store.loadAndRecover(lock), initial);
    EXPECT_TRUE(fs::exists(layout.workerRegistryFile()));

    fs::remove(layout.workerRegistryFile());
    const auto nonInitial = reconcileWorkerRegistry(initial, 3);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(nonInitial));
    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_TRUE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, MissingFinalPreservesInvalidTemporaryEvidence) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    writeFile(layout.workerRegistryTemporaryFile(), "partial");

    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), "partial");

    EXPECT_THROW(store.install(createWorkerRegistry(2), lock), std::runtime_error);
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), "partial");
}

TEST_F(WorkerRegistryStoreTest, DescriptorRelativeReadsNeverFollowFinalOrTemporarySymlinks) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);
    const auto initial = createWorkerRegistry(2);
    const auto outside = testDir.parent_path() / (testDir.filename().string() + "_outside");
    writeFile(outside, "sentinel");

    fs::create_symlink(outside, layout.workerRegistryFile());
    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_EQ(readFile(outside), "sentinel");

    fs::remove(layout.workerRegistryFile());
    store.install(initial, lock);
    fs::create_symlink(outside, layout.workerRegistryTemporaryFile());
    EXPECT_EQ(*store.loadAndRecover(lock), initial);
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
    EXPECT_EQ(readFile(outside), "sentinel");
    fs::remove(outside);
}

TEST_F(WorkerRegistryStoreTest, ReservedFifoCannotBlockRecovery) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore store(layout);

    ASSERT_EQ(::mkfifo(layout.workerRegistryTemporaryFile().c_str(), 0600), 0);
    EXPECT_THROW(
        {
            const auto ignored = store.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(fs::exists(layout.workerRegistryTemporaryFile()));

    fs::remove(layout.workerRegistryTemporaryFile());
    const auto initial = createWorkerRegistry(2);
    store.install(initial, lock);
    ASSERT_EQ(::mkfifo(layout.workerRegistryTemporaryFile().c_str(), 0600), 0);
    EXPECT_EQ(*store.loadAndRecover(lock), initial);
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, LockMustBelongToTheSameUnreplacedRoot) {
    const StorageLayout firstLayout(testDir / "first");
    const StorageLayout secondLayout(testDir / "second");
    ShardStoreStartup firstStartup(firstLayout);
    auto firstLock = firstStartup.acquireExclusiveLock();
    WorkerRegistryStore secondStore(secondLayout);

    EXPECT_THROW(
        {
            const auto ignored = secondStore.loadAndRecover(firstLock);
            (void)ignored;
        },
        std::invalid_argument);

    WorkerRegistryStore firstStore(firstLayout);
    const auto moved = testDir / "moved";
    fs::rename(firstLayout.root(), moved);
    fs::create_directories(firstLayout.root());
    EXPECT_THROW(
        {
            const auto ignored = firstStore.loadAndRecover(firstLock);
            (void)ignored;
        },
        std::runtime_error);
}

TEST_F(WorkerRegistryStoreTest, NormalCommitReportsEveryDurabilityBoundaryInOrder) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    std::vector<WorkerRegistryCommitStage> observed;
    WorkerRegistryStore store(layout, [&](WorkerRegistryCommitStage stage) { observed.push_back(stage); });

    store.install(createWorkerRegistry(2), lock);

    const std::vector<WorkerRegistryCommitStage> expected{
        WorkerRegistryCommitStage::TemporaryCreated, WorkerRegistryCommitStage::TemporaryWritten,
        WorkerRegistryCommitStage::TemporarySynced,  WorkerRegistryCommitStage::TemporaryClosed,
        WorkerRegistryCommitStage::Renamed,          WorkerRegistryCommitStage::DirectorySynced,
    };
    EXPECT_EQ(observed, expected);
}

TEST_F(WorkerRegistryStoreTest, ExclusiveCreateCollisionNeverDeletesForeignTemporaryArtifact) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool inserted = false;
    WorkerRegistryStore racingInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!inserted && stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            writeFile(layout.workerRegistryTemporaryFile(), "foreign-evidence");
            inserted = true;
        }
    });

    EXPECT_THROW(racingInstall.install(next, lock), std::system_error);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), "foreign-evidence");
    EXPECT_EQ(*setup.loadAndRecover(lock), initial);
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, InstallRenameRequiresTheCreatedTemporaryInode) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool replaced = false;
    WorkerRegistryStore attackedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!replaced && stage == WorkerRegistryCommitStage::TemporaryClosed) {
            fs::remove(layout.workerRegistryTemporaryFile());
            writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));
            replaced = true;
        }
    });

    EXPECT_THROW(attackedInstall.install(next, lock), std::runtime_error);
    EXPECT_TRUE(replaced);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, RecoveryRenameRequiresTheValidatedTemporaryInode) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    bool replaced = false;
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!replaced && stage == WorkerRegistryCommitStage::RecoveryTemporarySynced) {
            fs::remove(layout.workerRegistryTemporaryFile());
            writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));
            replaced = true;
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(replaced);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, InstallRenameRequiresTheValidatedTemporaryContents) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool rewritten = false;
    WorkerRegistryStore attackedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::TemporaryClosed) {
            writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(initial));
            rewritten = true;
        }
    });

    EXPECT_THROW(attackedInstall.install(next, lock), std::runtime_error);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, RecoveryRenameRequiresTheValidatedTemporaryContents) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    bool rewritten = false;
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::RecoveryTemporarySynced) {
            writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(initial));
            rewritten = true;
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), encodeWorkerRegistryJson(initial));
}

TEST_F(WorkerRegistryStoreTest, InstallPreservesASubstitutedAuthoritativeFinal) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    const auto substituted = createWorkerRegistry(4);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool replaced = false;
    WorkerRegistryStore attackedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!replaced && stage == WorkerRegistryCommitStage::TemporaryClosed) {
            fs::remove(layout.workerRegistryFile());
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(substituted));
            replaced = true;
        }
    });

    EXPECT_THROW(attackedInstall.install(next, lock), std::runtime_error);
    EXPECT_TRUE(replaced);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(substituted));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, RecoveryPreservesASubstitutedAuthoritativeFinal) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    const auto substituted = createWorkerRegistry(4);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    bool replaced = false;
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!replaced && stage == WorkerRegistryCommitStage::RecoveryTemporarySynced) {
            fs::remove(layout.workerRegistryFile());
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(substituted));
            replaced = true;
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(replaced);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(substituted));
    EXPECT_EQ(readFile(layout.workerRegistryTemporaryFile()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, InstallRevalidatesCommittedContentsAfterTheLastObserverBoundary) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool rewritten = false;
    WorkerRegistryStore attackedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::DirectorySynced) {
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(initial));
            rewritten = true;
        }
    });

    EXPECT_THROW(attackedInstall.install(next, lock), std::runtime_error);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, RecoveryRevalidatesCommittedContentsAfterTheLastObserverBoundary) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    bool rewritten = false;
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(initial));
            rewritten = true;
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(readFile(layout.workerRegistryFile()), encodeWorkerRegistryJson(initial));
    EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
}

TEST_F(WorkerRegistryStoreTest, InstallRestabilizesIdenticalBytesWrittenByTheLastObserver) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool rewritten = false;
    WorkerRegistryStore interruptedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::DirectorySynced) {
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(next));
            rewritten = true;
        }
    });

    EXPECT_NO_THROW(interruptedInstall.install(next, lock));
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(*setup.loadAndRecover(lock), next);
}

TEST_F(WorkerRegistryStoreTest, RecoveryPromotionRestabilizesIdenticalBytesWrittenByTheLastObserver) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    bool rewritten = false;
    WorkerRegistryStore interruptedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(next));
            rewritten = true;
        }
    });

    EXPECT_EQ(*interruptedRecovery.loadAndRecover(lock), next);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(*setup.loadAndRecover(lock), next);
}

TEST_F(WorkerRegistryStoreTest, AcceptedFinalRestabilizesIdenticalBytesWrittenByTheLastObserver) {
    const StorageLayout layout(testDir);
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    bool rewritten = false;
    WorkerRegistryStore interruptedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (!rewritten && stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            writeFile(layout.workerRegistryFile(), encodeWorkerRegistryJson(initial));
            rewritten = true;
        }
    });

    EXPECT_EQ(*interruptedRecovery.loadAndRecover(lock), initial);
    EXPECT_TRUE(rewritten);
    EXPECT_EQ(*setup.loadAndRecover(lock), initial);
}

TEST_F(WorkerRegistryStoreTest, InstallRejectsRootReplacementByTheLastObserver) {
    const StorageLayout layout(testDir / "root");
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    const auto moved = testDir / "moved";
    WorkerRegistryStore attackedInstall(layout, [&](WorkerRegistryCommitStage stage) {
        if (stage == WorkerRegistryCommitStage::DirectorySynced) {
            fs::rename(layout.root(), moved);
            fs::create_directories(layout.root());
        }
    });

    EXPECT_THROW(attackedInstall.install(next, lock), std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_EQ(readFile(moved / layout.workerRegistryFile().filename()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, RecoveryPromotionRejectsRootReplacementByTheLastObserver) {
    const StorageLayout layout(testDir / "root");
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    const auto moved = testDir / "moved";
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            fs::rename(layout.root(), moved);
            fs::create_directories(layout.root());
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_EQ(readFile(moved / layout.workerRegistryFile().filename()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, AcceptedFinalRejectsRootReplacementByTheLastObserver) {
    const StorageLayout layout(testDir / "root");
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto initial = createWorkerRegistry(2);
    WorkerRegistryStore setup(layout);
    setup.install(initial, lock);

    const auto moved = testDir / "moved";
    WorkerRegistryStore attackedRecovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (stage == WorkerRegistryCommitStage::RecoveryDirectorySynced) {
            fs::rename(layout.root(), moved);
            fs::create_directories(layout.root());
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = attackedRecovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_EQ(readFile(moved / layout.workerRegistryFile().filename()), encodeWorkerRegistryJson(initial));
}

TEST_F(WorkerRegistryStoreTest, RootReplacementImmediatelyBeforeRenameFailsClosed) {
    const StorageLayout layout(testDir / "root");
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    const auto moved = testDir / "moved";
    WorkerRegistryStore store(layout, [&](WorkerRegistryCommitStage stage) {
        if (stage == WorkerRegistryCommitStage::TemporaryClosed) {
            fs::rename(layout.root(), moved);
            fs::create_directories(layout.root());
        }
    });

    EXPECT_THROW(store.install(createWorkerRegistry(2), lock), std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_TRUE(fs::exists(moved / layout.workerRegistryTemporaryFile().filename()));
}

TEST_F(WorkerRegistryStoreTest, RootReplacementImmediatelyBeforeRecoveryRenameFailsClosed) {
    const StorageLayout layout(testDir / "root");
    ShardStoreStartup startup(layout);
    auto lock = startup.acquireExclusiveLock();
    WorkerRegistryStore setup(layout);
    const auto initial = createWorkerRegistry(2);
    const auto next = reconcileWorkerRegistry(initial, 3);
    setup.install(initial, lock);
    writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));

    const auto moved = testDir / "moved";
    WorkerRegistryStore recovery(layout, [&](WorkerRegistryCommitStage stage) {
        if (stage == WorkerRegistryCommitStage::RecoveryTemporarySynced) {
            fs::rename(layout.root(), moved);
            fs::create_directories(layout.root());
        }
    });

    EXPECT_THROW(
        {
            const auto ignored = recovery.loadAndRecover(lock);
            (void)ignored;
        },
        std::runtime_error);
    EXPECT_FALSE(fs::exists(layout.workerRegistryFile()));
    EXPECT_EQ(readFile(moved / layout.workerRegistryFile().filename()), encodeWorkerRegistryJson(initial));
    EXPECT_EQ(readFile(moved / layout.workerRegistryTemporaryFile().filename()), encodeWorkerRegistryJson(next));
}

TEST_F(WorkerRegistryStoreTest, InitialCreationRecoversOrFailsClosedAtEveryProcessCrashBoundary) {
    const std::array stages{
        WorkerRegistryCommitStage::TemporaryCreated, WorkerRegistryCommitStage::TemporaryWritten,
        WorkerRegistryCommitStage::TemporarySynced,  WorkerRegistryCommitStage::TemporaryClosed,
        WorkerRegistryCommitStage::Renamed,          WorkerRegistryCommitStage::DirectorySynced,
    };

    for (size_t index = 0; index < stages.size(); ++index) {
        SCOPED_TRACE(index);
        const StorageLayout layout(testDir / std::to_string(index));
        ShardStoreStartup startup(layout);
        const auto initial = createWorkerRegistry(2);
        crashInstallAt(layout, initial, stages[index]);

        auto lock = startup.acquireExclusiveLock();
        WorkerRegistryStore recovery(layout);
        if (stages[index] == WorkerRegistryCommitStage::TemporaryCreated) {
            EXPECT_THROW(
                {
                    const auto ignored = recovery.loadAndRecover(lock);
                    (void)ignored;
                },
                std::runtime_error);
            EXPECT_TRUE(fs::exists(layout.workerRegistryTemporaryFile()));
            fs::remove(layout.workerRegistryTemporaryFile());
            recovery.install(initial, lock);
        } else {
            const auto recovered = recovery.loadAndRecover(lock);
            ASSERT_TRUE(recovered.has_value());
            EXPECT_EQ(*recovered, initial);
        }
        EXPECT_EQ(*recovery.loadAndRecover(lock), initial);
        EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
    }
}

TEST_F(WorkerRegistryStoreTest, RecoveryPromotionSurvivesEveryProcessCrashBoundary) {
    const std::array stages{
        WorkerRegistryCommitStage::RecoveryTemporarySynced,
        WorkerRegistryCommitStage::RecoveryRenamed,
        WorkerRegistryCommitStage::RecoveryDirectorySynced,
    };

    for (size_t index = 0; index < stages.size(); ++index) {
        SCOPED_TRACE(index);
        const StorageLayout layout(testDir / std::to_string(index));
        ShardStoreStartup startup(layout);
        WorkerRegistryStore store(layout);
        const auto initial = createWorkerRegistry(2);
        const auto next = reconcileWorkerRegistry(initial, 3);
        {
            auto setupLock = startup.acquireExclusiveLock();
            store.install(initial, setupLock);
            writeFile(layout.workerRegistryTemporaryFile(), encodeWorkerRegistryJson(next));
        }

        crashRecoveryAt(layout, stages[index]);

        auto recoveryLock = startup.acquireExclusiveLock();
        EXPECT_EQ(*store.loadAndRecover(recoveryLock), next);
        EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
    }
}

TEST_F(WorkerRegistryStoreTest, RecoveryCleanupSurvivesProcessCrashBeforeDirectoryFsync) {
    const std::array stages{
        WorkerRegistryCommitStage::RecoveryTemporaryRemoved,
        WorkerRegistryCommitStage::RecoveryDirectorySynced,
    };

    for (size_t index = 0; index < stages.size(); ++index) {
        SCOPED_TRACE(index);
        const StorageLayout layout(testDir / std::to_string(index));
        ShardStoreStartup startup(layout);
        WorkerRegistryStore store(layout);
        const auto initial = createWorkerRegistry(2);
        {
            auto setupLock = startup.acquireExclusiveLock();
            store.install(initial, setupLock);
            writeFile(layout.workerRegistryTemporaryFile(), "partial");
        }

        crashRecoveryAt(layout, stages[index]);

        auto recoveryLock = startup.acquireExclusiveLock();
        EXPECT_EQ(*store.loadAndRecover(recoveryLock), initial);
        EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
    }
}

TEST_F(WorkerRegistryStoreTest, FinalOnlyRecoveryReacquiresLockAndSurvivesEveryFsyncBoundary) {
    const std::array stages{
        WorkerRegistryCommitStage::RecoveryFinalSynced,
        WorkerRegistryCommitStage::RecoveryDirectorySynced,
    };

    for (size_t index = 0; index < stages.size(); ++index) {
        SCOPED_TRACE(index);
        const StorageLayout layout(testDir / std::to_string(index));
        ShardStoreStartup startup(layout);
        WorkerRegistryStore store(layout);
        const auto initial = createWorkerRegistry(2);
        {
            auto setupLock = startup.acquireExclusiveLock();
            store.install(initial, setupLock);
        }

        crashRecoveryAt(layout, stages[index]);

        auto recoveryLock = startup.acquireExclusiveLock();
        EXPECT_EQ(*store.loadAndRecover(recoveryLock), initial);
    }
}

TEST_F(WorkerRegistryStoreTest, UpdateRecoversOneCompleteGenerationAtEveryProcessCrashBoundary) {
    const std::array stages{
        WorkerRegistryCommitStage::TemporaryCreated, WorkerRegistryCommitStage::TemporaryWritten,
        WorkerRegistryCommitStage::TemporarySynced,  WorkerRegistryCommitStage::TemporaryClosed,
        WorkerRegistryCommitStage::Renamed,          WorkerRegistryCommitStage::DirectorySynced,
    };

    for (size_t index = 0; index < stages.size(); ++index) {
        SCOPED_TRACE(index);
        const StorageLayout layout(testDir / std::to_string(index));
        ShardStoreStartup startup(layout);
        WorkerRegistryStore store(layout);
        const auto initial = createWorkerRegistry(2);
        const auto next = reconcileWorkerRegistry(initial, 3);
        {
            auto setupLock = startup.acquireExclusiveLock();
            store.install(initial, setupLock);
        }
        crashInstallAt(layout, next, stages[index]);

        auto recoveryLock = startup.acquireExclusiveLock();
        const auto recovered = store.loadAndRecover(recoveryLock);
        ASSERT_TRUE(recovered.has_value());
        const auto expected = stages[index] == WorkerRegistryCommitStage::TemporaryCreated ? initial : next;
        EXPECT_EQ(*recovered, expected);
        EXPECT_FALSE(fs::exists(layout.workerRegistryTemporaryFile()));
    }
}

}  // namespace

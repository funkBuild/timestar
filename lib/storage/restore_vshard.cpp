#include "restore_vshard.hpp"

#include "vshard_snapshot_read.hpp"

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>

namespace fs = std::filesystem;

namespace timestar {

seastar::future<bool> restoreVShardSnapshot(const VShardSnapshotManifest& manifest,
                                            std::vector<seastar::shared_ptr<::TSM>> sourceFiles,
                                            std::vector<std::string> targetPaths) {
    if (sourceFiles.size() != targetPaths.size())
        throw std::invalid_argument("restoreVShardSnapshot: sourceFiles and targetPaths size mismatch");

    // VERIFY before installing anything: the files must reproduce the snapshot's
    // resolved logical view exactly, or the restore is rejected.
    const bool verified = co_await verifyVShardSnapshot(manifest, sourceFiles);
    if (!verified)
        co_return false;

    // Install: copy each source's backing file to its target path. Blocking fs
    // copy runs off the reactor. Copy to a temp then rename so a crash never
    // leaves a partially-written file registered under its final name.
    std::vector<std::string> sources;
    sources.reserve(sourceFiles.size());
    for (const auto& f : sourceFiles)
        sources.push_back(f->getFilePath());

    co_await seastar::async([&sources, &targetPaths] {
        for (size_t i = 0; i < sources.size(); ++i) {
            const fs::path target = targetPaths[i];
            const fs::path tmp = fs::path(targetPaths[i] + ".restore-tmp");
            if (target.has_parent_path())
                fs::create_directories(target.parent_path());
            fs::copy_file(sources[i], tmp, fs::copy_options::overwrite_existing);
            fs::rename(tmp, target);  // atomic publish
        }
    });

    co_return true;
}

}  // namespace timestar

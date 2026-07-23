#include "restore_vshard.hpp"

#include "vshard_snapshot_read.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace timestar {

namespace {

// fsync a file (or directory) by path (blocking; call from seastar::async).
void fsyncPath(const std::string& path, int flags) {
    int fd = ::open(path.c_str(), flags);
    if (fd < 0)
        return;  // best-effort
    ::fsync(fd);
    ::close(fd);
}

}  // namespace

seastar::future<bool> restoreVShardSnapshot(const VShardSnapshotManifest& manifest,
                                            std::vector<seastar::shared_ptr<::TSM>> sourceFiles,
                                            std::vector<std::string> targetPaths) {
    if (sourceFiles.size() != targetPaths.size())
        throw std::invalid_argument("restoreVShardSnapshot: sourceFiles and targetPaths size mismatch");

    // Cross-check the manifest watermark: a source file carrying a revision beyond
    // the snapshot's snapshotRevision is not the data this manifest describes.
    for (const auto& f : sourceFiles) {
        if (f && f->maxRevision() > manifest.snapshotRevision)
            co_return false;
    }

    // VERIFY before installing anything: the files must reproduce the snapshot's
    // resolved logical view exactly, or the restore is rejected.
    const bool verified = co_await verifyVShardSnapshot(manifest, sourceFiles);
    if (!verified)
        co_return false;

    std::vector<std::string> sources;
    sources.reserve(sourceFiles.size());
    for (const auto& f : sourceFiles)
        sources.push_back(f->getFilePath());

    // Install all-or-nothing: copy EVERY file to a unique temp (fsync'd) FIRST;
    // only once all copies succeed do we rename them into place. A copy failure
    // (e.g. disk full) removes the temps and installs nothing, honouring the
    // "installs NOTHING on failure" contract. Blocking fs runs off the reactor.
    co_await seastar::async([&sources, &targetPaths] {
        const std::string tag = "." + std::to_string(::getpid()) + ".restore-tmp";
        std::vector<std::string> temps(sources.size());
        try {
            // Phase 1: stage all copies to temp, data-durable.
            for (size_t i = 0; i < sources.size(); ++i) {
                const fs::path target = targetPaths[i];
                if (target.has_parent_path())
                    fs::create_directories(target.parent_path());
                temps[i] = targetPaths[i] + tag + std::to_string(i);
                fs::copy_file(sources[i], temps[i], fs::copy_options::overwrite_existing);
                fsyncPath(temps[i], O_RDONLY);  // temp data durable before publish
            }
            // Phase 2: publish by rename (metadata-only), then fsync parent dirs.
            std::set<std::string> dirs;
            for (size_t i = 0; i < temps.size(); ++i) {
                fs::rename(temps[i], targetPaths[i]);
                const fs::path t = targetPaths[i];
                dirs.insert(t.has_parent_path() ? t.parent_path().string() : ".");
            }
            for (const auto& d : dirs)
                fsyncPath(d, O_RDONLY | O_DIRECTORY);
        } catch (...) {
            // Roll back any staged temps (not yet renamed). Files already renamed
            // in Phase 2 stay; Phase 2 is metadata-only and fails only pathologically.
            for (const auto& t : temps) {
                if (!t.empty()) {
                    std::error_code ec;
                    fs::remove(t, ec);
                }
            }
            throw;
        }
    });

    co_return true;
}

}  // namespace timestar

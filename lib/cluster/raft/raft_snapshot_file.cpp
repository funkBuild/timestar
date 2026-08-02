#include "raft_types.hpp"

#include <system_error>

namespace timestar::raft {

SnapshotFile::~SnapshotFile() {
    if (!removeOnDestroy || path.empty())
        return;
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

}  // namespace timestar::raft

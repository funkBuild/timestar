#pragma once

#include "raft_types.hpp"  // Config lives here (Snapshot embeds it)

#include <string>

namespace timestar::raft {

// Serialize / parse a Config into a ConfigChange entry's opaque `data`. Compact
// text ("v=1,2;o=3;l=4") so the deterministic core needs no external codec; the
// Seastar/journal driver may re-encode for the wire.
std::string encodeConfig(const Config& c);
Config decodeConfig(const std::string& s);

}  // namespace timestar::raft

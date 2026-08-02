#include "control_command.hpp"

#include <limits>
#include <stdexcept>

namespace timestar::control {

namespace {

constexpr uint8_t kNodeCapabilityFrameTag = 1;
constexpr size_t kMaxNodeCapabilityFrameBytes = 4096;
constexpr uint8_t kControlJoinRequestFrameTag = 1;
constexpr uint8_t kControlJoinResultFrameTag = 1;

bool canonicalHex128(const std::string& value) {
    if (value.size() != 32)
        return false;
    for (unsigned char c : value)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
            return false;
    return true;
}

bool validNodeCapability(const NodeCapabilityAdvertisement& capability) {
    return canonicalHex128(capability.clusterUuid) && capability.record.raftId != raft::kNoNode &&
           canonicalHex128(capability.record.uuid) && !capability.record.address.empty() &&
           capability.record.address.size() <= 1024 && !capability.record.failureDomain.empty() &&
           capability.record.failureDomain.size() <= 1024 && capability.formats.min != 0 &&
           capability.formats.min <= capability.formats.max;
}

bool validControlJoinRequest(const ControlJoinRequest& request) {
    return canonicalHex128(request.clusterUuid) && request.record.raftId != raft::kNoNode &&
           canonicalHex128(request.record.uuid) && !request.record.address.empty() &&
           request.record.address.size() <= 1024 && !request.record.failureDomain.empty() &&
           request.record.failureDomain.size() <= 1024 && request.record.state == NodeState::Joining &&
           validJoinToken(request.token);
}

bool validControlJoinResult(const ControlJoinResult& result) {
    if (result.status > ControlJoinStatus::Active)
        return false;
    return (result.status == ControlJoinStatus::NotLeader || result.status == ControlJoinStatus::Rejected) ||
           result.leader != raft::kNoNode;
}

struct Writer {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u16(uint16_t v) {
        u8(v & 0xff);
        u8((v >> 8) & 0xff);
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void str(const std::string& s) {
        u64(s.size());
        out.append(s);
    }
    void ids(const std::vector<NodeId>& v) {
        u64(v.size());
        for (NodeId n : v)
            u64(n);
    }
};

struct Reader {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint8_t u8() {
        if (!avail(1)) {
            ok = false;
            return 0;
        }
        return static_cast<uint8_t>(*p++);
    }
    uint16_t u16() {
        uint16_t a = u8(), b = u8();
        return static_cast<uint16_t>(a | (b << 8));
    }
    uint64_t u64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(u8()) << (8 * i);
        return v;
    }
    uint32_t u32() {
        const uint64_t v = u64();
        if (v > std::numeric_limits<uint32_t>::max()) {
            ok = false;
            return 0;
        }
        return static_cast<uint32_t>(v);
    }
    bool boolean() {
        const uint8_t v = u8();
        if (v > 1) {
            ok = false;
            return false;
        }
        return v != 0;
    }
    NodeState nodeState() {
        const auto state = static_cast<NodeState>(u8());
        if (!isValidNodeState(state))
            ok = false;
        return state;
    }
    std::string str() {
        uint64_t n = u64();
        if (!ok || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, p + n);
        p += n;
        return s;
    }
    std::vector<NodeId> ids() {
        uint64_t n = u64();
        std::vector<NodeId> v;
        // Non-wrapping bounds check: n*8 could overflow, so compare against the
        // remaining byte budget divided by 8 instead of multiplying.
        if (!ok || n > static_cast<uint64_t>(end - p) / 8) {
            ok = false;
            return v;
        }
        v.reserve(n);
        for (uint64_t i = 0; i < n; ++i)
            v.push_back(u64());
        return v;
    }
};

enum : uint8_t {
    kInitCluster = 1,
    kUpsertNode = 2,
    kSetNodeState = 3,
    kSetDesiredPlacement = 4,
    kSetMetaVoters = 5,
    kCasPolicy = 6,
    kSetControllerTerm = 7,
    kUpsertJob = 8,
    kMintJoinToken = 9,
    kAdmitWithToken = 10,
    kSetActiveVersion = 11,
    kSetInitialServingMap = 12,
    kSetActiveVersionCovered = 13,
    kStoreFrozenDeletePlan = 14,
};

void writeNode(Writer& w, const NodeRecord& r) {
    w.u64(r.raftId);
    w.str(r.uuid);
    w.str(r.address);
    w.str(r.failureDomain);
    w.u8(static_cast<uint8_t>(r.state));
}

NodeRecord readNode(Reader& r) {
    NodeRecord n;
    n.raftId = r.u64();
    n.uuid = r.str();
    n.address = r.str();
    n.failureDomain = r.str();
    n.state = r.nodeState();
    return n;
}

void writeControlMap(Writer& w, const ControlMap& map) {
    w.u64(map.epoch);
    w.u64(map.placement.size());
    for (const auto& [vshard, replicas] : map.placement) {
        w.u16(vshard);
        w.ids(replicas);
    }
    w.u64(map.groups.size());
    for (const auto& [vshard, group] : map.groups) {
        w.u16(vshard);
        w.u16(group);
    }
}

ControlMap readControlMap(Reader& r) {
    ControlMap map;
    map.epoch = r.u64();
    const uint64_t placements = r.u64();
    if (!r.ok || placements > static_cast<uint64_t>(r.end - r.p) / 10) {
        r.ok = false;
        return map;
    }
    for (uint64_t i = 0; i < placements && r.ok; ++i) {
        const uint16_t vshard = r.u16();
        auto replicas = r.ids();
        if (r.ok && !map.placement.emplace(vshard, std::move(replicas)).second)
            r.ok = false;
    }
    const uint64_t groups = r.u64();
    if (!r.ok || groups > static_cast<uint64_t>(r.end - r.p) / 4) {
        r.ok = false;
        return map;
    }
    for (uint64_t i = 0; i < groups && r.ok; ++i) {
        const uint16_t vshard = r.u16();
        const uint16_t group = r.u16();
        if (!map.groups.emplace(vshard, group).second)
            r.ok = false;
    }
    return map;
}

void writeFrozenDeletePlan(Writer& w, const FrozenDeletePlan& plan) {
    w.str(plan.requestId);
    w.str(plan.requestFingerprint);
    w.u64(plan.issuedAtMs);
    w.u64(plan.targets.size());
    for (const auto& target : plan.targets) {
        w.str(target.seriesKey);
        w.u64(target.startTime);
        w.u64(target.endTime);
    }
}

FrozenDeletePlan readFrozenDeletePlan(Reader& r) {
    FrozenDeletePlan plan;
    plan.requestId = r.str();
    plan.requestFingerprint = r.str();
    plan.issuedAtMs = r.u64();
    const uint64_t count = r.u64();
    constexpr uint64_t kMinimumTargetBytes = sizeof(uint64_t) * 3;
    if (!r.ok || count > kMaxFrozenDeletePlanTargets ||
        count > static_cast<uint64_t>(r.end - r.p) / kMinimumTargetBytes) {
        r.ok = false;
        return plan;
    }
    plan.targets.reserve(count);
    for (uint64_t i = 0; i < count && r.ok; ++i)
        plan.targets.push_back(FrozenDeleteTarget{r.str(), r.u64(), r.u64()});
    return plan;
}

}  // namespace

std::string encodeCommand(const ControlCommand& cmd) {
    if (const auto* mint = std::get_if<MintJoinToken>(&cmd); mint && !validJoinToken(mint->token))
        throw std::invalid_argument("invalid group-0 join token");
    if (const auto* admission = std::get_if<AdmitWithToken>(&cmd); admission && !validJoinToken(admission->token))
        throw std::invalid_argument("invalid group-0 admission token");
    Writer w;
    std::visit(
        [&](const auto& c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, InitCluster>) {
                w.u8(kInitCluster);
                w.str(c.clusterUuid);
            } else if constexpr (std::is_same_v<T, UpsertNode>) {
                w.u8(kUpsertNode);
                writeNode(w, c.record);
            } else if constexpr (std::is_same_v<T, SetNodeState>) {
                w.u8(kSetNodeState);
                w.u64(c.raftId);
                w.u8(static_cast<uint8_t>(c.state));
            } else if constexpr (std::is_same_v<T, SetDesiredPlacement>) {
                w.u8(kSetDesiredPlacement);
                w.u16(c.vshard);
                w.ids(c.replicas);
            } else if constexpr (std::is_same_v<T, SetMetaVoters>) {
                w.u8(kSetMetaVoters);
                w.ids(c.voters);
            } else if constexpr (std::is_same_v<T, CasPolicy>) {
                w.u8(kCasPolicy);
                w.str(c.key);
                w.u64(c.expectedVersion);
                w.str(c.value);
            } else if constexpr (std::is_same_v<T, SetControllerTerm>) {
                w.u8(kSetControllerTerm);
                w.u64(c.term);
                w.u64(c.leader);
            } else if constexpr (std::is_same_v<T, UpsertJob>) {
                w.u8(kUpsertJob);
                w.str(c.jobId);
                w.u64(c.step);
                w.u8(c.done ? 1 : 0);
                w.str(c.payload);
            } else if constexpr (std::is_same_v<T, MintJoinToken>) {
                w.u8(kMintJoinToken);
                w.str(c.token);
            } else if constexpr (std::is_same_v<T, AdmitWithToken>) {
                w.u8(kAdmitWithToken);
                writeNode(w, c.record);
                w.str(c.token);
            } else if constexpr (std::is_same_v<T, StoreFrozenDeletePlan>) {
                w.u8(kStoreFrozenDeletePlan);
                writeFrozenDeletePlan(w, c.plan);
            } else if constexpr (std::is_same_v<T, SetActiveVersion>) {
                w.u8(kSetActiveVersionCovered);
                w.u64(c.version);
                w.ids(c.coveredVoters);
            } else if constexpr (std::is_same_v<T, SetInitialServingMap>) {
                w.u8(kSetInitialServingMap);
                writeControlMap(w, c.map);
            }
        },
        cmd);
    return std::move(w.out);
}

std::optional<ControlCommand> decodeCommand(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    const uint8_t tag = r.u8();
    ControlCommand cmd;
    switch (tag) {
        case kInitCluster: {
            InitCluster c;
            c.clusterUuid = r.str();
            cmd = std::move(c);
            break;
        }
        case kUpsertNode: {
            UpsertNode c;
            c.record = readNode(r);
            cmd = std::move(c);
            break;
        }
        case kSetNodeState: {
            SetNodeState c;
            c.raftId = r.u64();
            c.state = r.nodeState();
            cmd = c;
            break;
        }
        case kSetDesiredPlacement: {
            SetDesiredPlacement c;
            c.vshard = r.u16();
            c.replicas = r.ids();
            cmd = std::move(c);
            break;
        }
        case kSetMetaVoters: {
            SetMetaVoters c;
            c.voters = r.ids();
            cmd = std::move(c);
            break;
        }
        case kCasPolicy: {
            CasPolicy c;
            c.key = r.str();
            c.expectedVersion = r.u64();
            c.value = r.str();
            cmd = std::move(c);
            break;
        }
        case kSetControllerTerm: {
            SetControllerTerm c;
            c.term = r.u64();
            c.leader = r.u64();
            cmd = c;
            break;
        }
        case kUpsertJob: {
            UpsertJob c;
            c.jobId = r.str();
            c.step = r.u32();
            c.done = r.boolean();
            c.payload = r.str();
            cmd = std::move(c);
            break;
        }
        case kMintJoinToken: {
            MintJoinToken c;
            c.token = r.str();
            cmd = std::move(c);
            break;
        }
        case kAdmitWithToken: {
            AdmitWithToken c;
            c.record = readNode(r);
            c.token = r.str();
            cmd = std::move(c);
            break;
        }
        case kStoreFrozenDeletePlan: {
            StoreFrozenDeletePlan c;
            c.plan = readFrozenDeletePlan(r);
            cmd = std::move(c);
            break;
        }
        case kSetActiveVersion: {
            SetActiveVersion c;
            c.version = r.u32();
            // Backward-compatible decode of the pre-coverage command. Its empty
            // proof is rejected by production apply. The covered form has a new
            // tag so a truncated current command cannot masquerade as this one.
            cmd = std::move(c);
            break;
        }
        case kSetActiveVersionCovered: {
            SetActiveVersion c;
            c.version = r.u32();
            c.coveredVoters = r.ids();
            cmd = std::move(c);
            break;
        }
        case kSetInitialServingMap: {
            SetInitialServingMap c;
            c.map = readControlMap(r);
            cmd = std::move(c);
            break;
        }
        default:
            return std::nullopt;
    }
    // A command occupies the WHOLE Raft entry. Accepting an otherwise-valid
    // prefix and ignoring a suffix makes format skew especially dangerous: an
    // older binary could appear to understand a newer command while applying
    // only its old prefix, permanently diverging group-0 state.
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    if (const auto* mint = std::get_if<MintJoinToken>(&cmd); mint && !validJoinToken(mint->token))
        return std::nullopt;
    if (const auto* admission = std::get_if<AdmitWithToken>(&cmd);
        admission && !validJoinToken(admission->token))
        return std::nullopt;
    return cmd;
}

std::string encodeFrozenDeletePlanRpcRequest(const FrozenDeletePlanRpcRequest& request) {
    if (!validFrozenDeletePlan(request.plan) ||
        (request.operation == FrozenDeletePlanRpcOperation::Lookup && !request.plan.targets.empty()) ||
        (request.operation != FrozenDeletePlanRpcOperation::Lookup &&
         request.operation != FrozenDeletePlanRpcOperation::Freeze))
        throw std::invalid_argument("invalid frozen delete-plan RPC request");
    Writer w;
    w.u8(static_cast<uint8_t>(request.operation));
    writeFrozenDeletePlan(w, request.plan);
    return std::move(w.out);
}

std::optional<FrozenDeletePlanRpcRequest> decodeFrozenDeletePlanRpcRequest(const std::string& bytes) {
    // Reject an over-budget frame before Reader materialises attacker-sized
    // strings. The RPC admission bound protects the process globally; this
    // tighter protocol bound protects this one 512-KiB control operation.
    if (bytes.size() > kMaxFrozenDeletePlanBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    const uint8_t operation = r.u8();
    FrozenDeletePlanRpcRequest request;
    if (operation > static_cast<uint8_t>(FrozenDeletePlanRpcOperation::Freeze))
        return std::nullopt;
    request.operation = static_cast<FrozenDeletePlanRpcOperation>(operation);
    request.plan = readFrozenDeletePlan(r);
    if (!r.ok || r.p != r.end || !validFrozenDeletePlan(request.plan) ||
        (request.operation == FrozenDeletePlanRpcOperation::Lookup && !request.plan.targets.empty()))
        return std::nullopt;
    return request;
}

std::string encodeFrozenDeletePlanRpcResult(const FreezeDeletePlanResult& result) {
    const bool carriesPlan = result.status == FreezeDeletePlanStatus::Stored ||
                             result.status == FreezeDeletePlanStatus::Conflict;
    if (result.status > FreezeDeletePlanStatus::Invalid ||
        (carriesPlan ? !validFrozenDeletePlan(result.plan) : result.plan != FrozenDeletePlan{}))
        throw std::invalid_argument("invalid frozen delete-plan RPC result");
    Writer w;
    w.u8(static_cast<uint8_t>(result.status));
    if (carriesPlan)
        writeFrozenDeletePlan(w, result.plan);
    return std::move(w.out);
}

std::optional<FreezeDeletePlanResult> decodeFrozenDeletePlanRpcResult(const std::string& bytes) {
    if (bytes.size() > kMaxFrozenDeletePlanBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    const uint8_t status = r.u8();
    if (status > static_cast<uint8_t>(FreezeDeletePlanStatus::Invalid))
        return std::nullopt;
    FreezeDeletePlanResult result;
    result.status = static_cast<FreezeDeletePlanStatus>(status);
    const bool carriesPlan = result.status == FreezeDeletePlanStatus::Stored ||
                             result.status == FreezeDeletePlanStatus::Conflict;
    if (carriesPlan)
        result.plan = readFrozenDeletePlan(r);
    if (!r.ok || r.p != r.end || (carriesPlan && !validFrozenDeletePlan(result.plan)))
        return std::nullopt;
    return result;
}

std::string encodeNodeCapabilityAdvertisement(const NodeCapabilityAdvertisement& capability) {
    if (!validNodeCapability(capability))
        throw std::invalid_argument("invalid node capability advertisement");
    Writer w;
    w.u8(kNodeCapabilityFrameTag);
    w.str(capability.clusterUuid);
    writeNode(w, capability.record);
    w.u64(capability.formats.min);
    w.u64(capability.formats.max);
    if (w.out.size() > kMaxNodeCapabilityFrameBytes)
        throw std::invalid_argument("node capability advertisement exceeds its wire bound");
    return std::move(w.out);
}

std::optional<NodeCapabilityAdvertisement> decodeNodeCapabilityAdvertisement(const std::string& bytes) {
    if (bytes.size() > kMaxNodeCapabilityFrameBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kNodeCapabilityFrameTag)
        return std::nullopt;
    NodeCapabilityAdvertisement capability;
    capability.clusterUuid = r.str();
    capability.record = readNode(r);
    capability.formats.min = r.u32();
    capability.formats.max = r.u32();
    if (!r.ok || r.p != r.end || !validNodeCapability(capability))
        return std::nullopt;
    return capability;
}

std::string encodeControlJoinRequest(const ControlJoinRequest& request) {
    if (!validControlJoinRequest(request))
        throw std::invalid_argument("invalid control join request");
    Writer w;
    w.u8(kControlJoinRequestFrameTag);
    w.str(request.clusterUuid);
    writeNode(w, request.record);
    w.str(request.token);
    if (w.out.size() > kMaxControlJoinFrameBytes)
        throw std::invalid_argument("control join request exceeds its wire bound");
    return std::move(w.out);
}

std::optional<ControlJoinRequest> decodeControlJoinRequest(const std::string& bytes) {
    if (bytes.size() > kMaxControlJoinFrameBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kControlJoinRequestFrameTag)
        return std::nullopt;
    ControlJoinRequest request;
    request.clusterUuid = r.str();
    request.record = readNode(r);
    request.token = r.str();
    if (!r.ok || r.p != r.end || !validControlJoinRequest(request))
        return std::nullopt;
    return request;
}

std::string encodeControlJoinResult(const ControlJoinResult& result) {
    if (!validControlJoinResult(result))
        throw std::invalid_argument("invalid control join result");
    Writer w;
    w.u8(kControlJoinResultFrameTag);
    w.u8(static_cast<uint8_t>(result.status));
    w.u64(result.leader);
    return std::move(w.out);
}

std::optional<ControlJoinResult> decodeControlJoinResult(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kControlJoinResultFrameTag)
        return std::nullopt;
    const uint8_t status = r.u8();
    if (status > static_cast<uint8_t>(ControlJoinStatus::Active))
        return std::nullopt;
    ControlJoinResult result{static_cast<ControlJoinStatus>(status), r.u64()};
    if (!r.ok || r.p != r.end || !validControlJoinResult(result))
        return std::nullopt;
    return result;
}

}  // namespace timestar::control

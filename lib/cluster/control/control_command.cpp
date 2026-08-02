#include "control_command.hpp"

#include "../../core/vshard.hpp"

#include <cstring>
#include <limits>
#include <stdexcept>

namespace timestar::control {

namespace {

constexpr char kCommandMagic[4] = {'T', 'C', 'C', '1'};
constexpr uint8_t kFrozenDeletePlanFrameVersion = 1;
constexpr uint8_t kControlJoinRequestFrameTag = 1;
constexpr uint8_t kControlJoinResultFrameTag = 1;
constexpr uint8_t kEnsureMoveDestinationRequestFrameTag = 1;
constexpr uint8_t kEnsureMoveDestinationResultFrameTag = 1;
constexpr uint8_t kActuateMoveResultFrameTag = 1;

bool canonicalHex128(const std::string& value) {
    if (value.size() != 32)
        return false;
    for (unsigned char c : value)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
            return false;
    return true;
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

bool validEnsureMoveDestinationRequest(const EnsureMoveDestinationRequest& request) {
    return canonicalHex128(request.clusterUuid) && validControlJobId(request.jobId) &&
           request.controllerTerm != raft::kNoTerm && request.controllerLeader != raft::kNoNode;
}

bool validActuateMoveResult(const ActuateMoveResult& result) {
    if (result.status > ActuateMoveStatus::Unavailable)
        return false;
    if (result.status != ActuateMoveStatus::Advanced)
        return result.job == Job{};
    if (result.leader == raft::kNoNode || !validControlJobId(result.job.id))
        return false;
    auto move = movement::MoveJob::decode(result.job.payload);
    return move && result.job.step == static_cast<uint32_t>(move->step()) && result.job.done == move->done();
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
    std::string str(size_t maxBytes = std::numeric_limits<size_t>::max()) {
        uint64_t n = u64();
        if (!ok || n > maxBytes || !avail(n)) {
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
    kPlanVShardMove = 4,
    kSetMetaVoters = 5,
    kCasPolicy = 6,
    kSetControllerTerm = 7,
    kUpsertJob = 8,
    kMintJoinToken = 9,
    kAdmitWithToken = 10,
    kPublishServingMap = 11,
    kStoreFrozenDeletePlan = 12,
    kStartRetentionSweep = 13,
    kAdvanceRetentionSweep = 14,
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
    if (const auto* policy = std::get_if<CasPolicy>(&cmd);
        policy &&
        (policy->key.empty() || policy->key.size() > kMaxPolicyKeyBytes || policy->value.size() > kMaxPolicyValueBytes))
        throw std::invalid_argument("invalid group-0 policy cell");
    if (const auto* sweep = std::get_if<StartRetentionSweep>(&cmd);
        sweep && (sweep->sweepId == 0 || !validRetentionMeasurement(sweep->measurement) || sweep->policyVersion == 0 ||
                  sweep->issuedAtNanos == 0 || sweep->cutoffTime == 0))
        throw std::invalid_argument("invalid group-0 retention sweep");
    if (const auto* advance = std::get_if<AdvanceRetentionSweep>(&cmd);
        advance &&
        (advance->sweepId == 0 || !validRetentionMeasurement(advance->measurement) || advance->policyVersion == 0 ||
         advance->cutoffTime == 0 || advance->nextVShard > timestar::VIRTUAL_SHARD_COUNT))
        throw std::invalid_argument("invalid group-0 retention advance");
    Writer w;
    w.out.append(kCommandMagic, sizeof(kCommandMagic));
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
            } else if constexpr (std::is_same_v<T, PlanVShardMove>) {
                w.u8(kPlanVShardMove);
                w.str(c.jobId);
                w.str(movement::MoveJob(c.plan).encode());
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
            } else if constexpr (std::is_same_v<T, PublishServingMap>) {
                w.u8(kPublishServingMap);
                w.str(c.completedJobId);
                writeControlMap(w, c.map);
            } else if constexpr (std::is_same_v<T, StartRetentionSweep>) {
                w.u8(kStartRetentionSweep);
                w.u64(c.sweepId);
                w.str(c.measurement);
                w.u64(c.policyVersion);
                w.u64(c.issuedAtNanos);
                w.u64(c.cutoffTime);
            } else if constexpr (std::is_same_v<T, AdvanceRetentionSweep>) {
                w.u8(kAdvanceRetentionSweep);
                w.u64(c.sweepId);
                w.str(c.measurement);
                w.u64(c.policyVersion);
                w.u64(c.cutoffTime);
                w.u64(c.nextVShard);
            }
        },
        cmd);
    return std::move(w.out);
}

std::optional<ControlCommand> decodeCommand(const std::string& bytes) {
    if (bytes.size() < sizeof(kCommandMagic) || std::memcmp(bytes.data(), kCommandMagic, sizeof(kCommandMagic)) != 0)
        return std::nullopt;
    Reader r{bytes.data() + sizeof(kCommandMagic), bytes.data() + bytes.size()};
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
        case kPlanVShardMove: {
            PlanVShardMove c;
            c.jobId = r.str();
            auto job = movement::MoveJob::decode(r.str());
            if (!job || job->step() != movement::MoveStep::Planned)
                r.ok = false;
            else
                c.plan = job->plan();
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
            c.key = r.str(kMaxPolicyKeyBytes);
            c.expectedVersion = r.u64();
            c.value = r.str(kMaxPolicyValueBytes);
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
        case kPublishServingMap: {
            PublishServingMap c;
            c.completedJobId = r.str();
            c.map = readControlMap(r);
            cmd = std::move(c);
            break;
        }
        case kStartRetentionSweep: {
            StartRetentionSweep c;
            c.sweepId = r.u64();
            c.measurement = r.str(kMaxRetentionMeasurementBytes);
            c.policyVersion = r.u64();
            c.issuedAtNanos = r.u64();
            c.cutoffTime = r.u64();
            cmd = std::move(c);
            break;
        }
        case kAdvanceRetentionSweep: {
            AdvanceRetentionSweep c;
            c.sweepId = r.u64();
            c.measurement = r.str(kMaxRetentionMeasurementBytes);
            c.policyVersion = r.u64();
            c.cutoffTime = r.u64();
            c.nextVShard = r.u32();
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
    if (const auto* admission = std::get_if<AdmitWithToken>(&cmd); admission && !validJoinToken(admission->token))
        return std::nullopt;
    if (const auto* sweep = std::get_if<StartRetentionSweep>(&cmd);
        sweep && (sweep->sweepId == 0 || !validRetentionMeasurement(sweep->measurement) || sweep->policyVersion == 0 ||
                  sweep->issuedAtNanos == 0 || sweep->cutoffTime == 0))
        return std::nullopt;
    if (const auto* advance = std::get_if<AdvanceRetentionSweep>(&cmd);
        advance &&
        (advance->sweepId == 0 || !validRetentionMeasurement(advance->measurement) || advance->policyVersion == 0 ||
         advance->cutoffTime == 0 || advance->nextVShard > timestar::VIRTUAL_SHARD_COUNT))
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
    w.u8(kFrozenDeletePlanFrameVersion);
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
    if (r.u8() != kFrozenDeletePlanFrameVersion)
        return std::nullopt;
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
    const bool carriesPlan =
        result.status == FreezeDeletePlanStatus::Stored || result.status == FreezeDeletePlanStatus::Conflict;
    if (result.status > FreezeDeletePlanStatus::Invalid ||
        (carriesPlan ? !validFrozenDeletePlan(result.plan) : result.plan != FrozenDeletePlan{}))
        throw std::invalid_argument("invalid frozen delete-plan RPC result");
    Writer w;
    w.u8(kFrozenDeletePlanFrameVersion);
    w.u8(static_cast<uint8_t>(result.status));
    if (carriesPlan)
        writeFrozenDeletePlan(w, result.plan);
    return std::move(w.out);
}

std::optional<FreezeDeletePlanResult> decodeFrozenDeletePlanRpcResult(const std::string& bytes) {
    if (bytes.size() > kMaxFrozenDeletePlanBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kFrozenDeletePlanFrameVersion)
        return std::nullopt;
    const uint8_t status = r.u8();
    if (status > static_cast<uint8_t>(FreezeDeletePlanStatus::Invalid))
        return std::nullopt;
    FreezeDeletePlanResult result;
    result.status = static_cast<FreezeDeletePlanStatus>(status);
    const bool carriesPlan =
        result.status == FreezeDeletePlanStatus::Stored || result.status == FreezeDeletePlanStatus::Conflict;
    if (carriesPlan)
        result.plan = readFrozenDeletePlan(r);
    if (!r.ok || r.p != r.end || (carriesPlan && !validFrozenDeletePlan(result.plan)))
        return std::nullopt;
    return result;
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

std::string encodeEnsureMoveDestinationRequest(const EnsureMoveDestinationRequest& request) {
    if (!validEnsureMoveDestinationRequest(request))
        throw std::invalid_argument("invalid ensure-move-destination request");
    Writer w;
    w.u8(kEnsureMoveDestinationRequestFrameTag);
    w.str(request.clusterUuid);
    w.str(request.jobId);
    w.u64(request.controllerTerm);
    w.u64(request.controllerLeader);
    if (w.out.size() > kMaxEnsureMoveDestinationFrameBytes)
        throw std::invalid_argument("ensure-move-destination request exceeds its wire bound");
    return std::move(w.out);
}

std::optional<EnsureMoveDestinationRequest> decodeEnsureMoveDestinationRequest(const std::string& bytes) {
    if (bytes.size() > kMaxEnsureMoveDestinationFrameBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kEnsureMoveDestinationRequestFrameTag)
        return std::nullopt;
    EnsureMoveDestinationRequest request;
    request.clusterUuid = r.str();
    request.jobId = r.str();
    request.controllerTerm = r.u64();
    request.controllerLeader = r.u64();
    if (!r.ok || r.p != r.end || !validEnsureMoveDestinationRequest(request))
        return std::nullopt;
    return request;
}

std::string encodeEnsureMoveDestinationResult(const EnsureMoveDestinationResult& result) {
    if (result.status > EnsureMoveDestinationStatus::Unavailable)
        throw std::invalid_argument("invalid ensure-move-destination result");
    Writer w;
    w.u8(kEnsureMoveDestinationResultFrameTag);
    w.u8(static_cast<uint8_t>(result.status));
    return std::move(w.out);
}

std::optional<EnsureMoveDestinationResult> decodeEnsureMoveDestinationResult(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kEnsureMoveDestinationResultFrameTag)
        return std::nullopt;
    const uint8_t status = r.u8();
    if (!r.ok || r.p != r.end || status > static_cast<uint8_t>(EnsureMoveDestinationStatus::Unavailable))
        return std::nullopt;
    return EnsureMoveDestinationResult{static_cast<EnsureMoveDestinationStatus>(status)};
}

std::string encodeActuateMoveResult(const ActuateMoveResult& result) {
    if (!validActuateMoveResult(result))
        throw std::invalid_argument("invalid actuate-move result");
    Writer w;
    w.u8(kActuateMoveResultFrameTag);
    w.u8(static_cast<uint8_t>(result.status));
    w.u64(result.leader);
    if (result.status == ActuateMoveStatus::Advanced) {
        w.str(result.job.id);
        w.u64(result.job.step);
        w.u8(result.job.done ? 1 : 0);
        w.str(result.job.payload);
    }
    if (w.out.size() > kMaxActuateMoveFrameBytes)
        throw std::invalid_argument("actuate-move result exceeds its wire bound");
    return std::move(w.out);
}

std::optional<ActuateMoveResult> decodeActuateMoveResult(const std::string& bytes) {
    if (bytes.size() > kMaxActuateMoveFrameBytes)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    if (r.u8() != kActuateMoveResultFrameTag)
        return std::nullopt;
    const uint8_t status = r.u8();
    if (status > static_cast<uint8_t>(ActuateMoveStatus::Unavailable))
        return std::nullopt;
    ActuateMoveResult result;
    result.status = static_cast<ActuateMoveStatus>(status);
    result.leader = r.u64();
    if (result.status == ActuateMoveStatus::Advanced) {
        result.job.id = r.str();
        result.job.step = r.u32();
        result.job.done = r.boolean();
        result.job.payload = r.str();
    }
    if (!r.ok || r.p != r.end || !validActuateMoveResult(result))
        return std::nullopt;
    return result;
}

}  // namespace timestar::control

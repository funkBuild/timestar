#pragma once

#include <cstdint>
#include <string>

namespace timestar::control {

// Detects group-0 state REGRESSION (a restore from backup or a disaster rebuild)
// and freezes control-plane actuation until an operator supersession ceremony --
// the plan's "control-plane state restored from a backup is a regression, not an
// outage." Every node persists the highest group-0 applied index and map epoch
// it has ACTED ON; if it later observes group-0 state below that watermark, it
// must not actuate the (stale) control plane (the data plane continues as during
// an outage). Group 0 is the external epoch witness that makes this detectable.
class EpochRegressionGuard {
public:
    // Observe the current group-0 (appliedIndex, mapEpoch) before acting on it.
    // Returns true (safe to actuate) if the state is at or ahead of our
    // watermark, advancing the watermark. Returns false (FREEZE) if it regressed
    // below the watermark -- and latches frozen until supersede().
    bool observe(uint64_t appliedIndex, uint64_t mapEpoch) {
        if (frozen_)
            return false;
        if (appliedIndex < highestApplied_ || mapEpoch < highestEpoch_) {
            frozen_ = true;  // regression: latch the freeze
            return false;
        }
        highestApplied_ = appliedIndex;
        highestEpoch_ = mapEpoch;
        return true;
    }

    bool frozen() const { return frozen_; }
    uint64_t highestApplied() const { return highestApplied_; }
    uint64_t highestEpoch() const { return highestEpoch_; }

    // Operator supersession ceremony: accept a (possibly restored) state as the
    // new authoritative watermark and clear the freeze. The operator asserts the
    // restored control plane has been reconciled against data-plane truth.
    void supersede(uint64_t appliedIndex, uint64_t mapEpoch) {
        highestApplied_ = appliedIndex;
        highestEpoch_ = mapEpoch;
        frozen_ = false;
    }

    // Durable state: [applied u64][epoch u64][frozen u8].
    std::string serialize() const {
        std::string out;
        auto put64 = [&](uint64_t v) {
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
        };
        put64(highestApplied_);
        put64(highestEpoch_);
        out.push_back(frozen_ ? 1 : 0);
        return out;
    }
    bool load(const std::string& s) {
        if (s.size() < 17)
            return false;
        auto get64 = [&](const char* p) {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
            return v;
        };
        highestApplied_ = get64(s.data());
        highestEpoch_ = get64(s.data() + 8);
        frozen_ = s[16] != 0;
        return true;
    }

private:
    uint64_t highestApplied_ = 0;
    uint64_t highestEpoch_ = 0;
    bool frozen_ = false;
};

}  // namespace timestar::control

#pragma once

#include <cstdint>

namespace timestar::movement {

// Foreground health sampled from the reactor / query / write path. Rebalance and
// repair run BELOW foreground work and must yield to it (docs/clustering.md
// §"Rebalance resource protection").
struct ForegroundSignals {
    double p99LatencyMs = 0;
    double errorRate = 0;       // [0,1]
    double reactorStallMs = 0;  // worst recent reactor stall
    uint32_t queueDepth = 0;
    double diskLatencyMs = 0;
};

// Budgets above which movement must automatically pause.
struct SloBudgets {
    double maxP99LatencyMs = 500;
    double maxErrorRate = 0.01;
    double maxReactorStallMs = 100;
    uint32_t maxQueueDepth = 1000;
    double maxDiskLatencyMs = 50;
};

// Decides whether rebalance/repair movement may proceed right now. It AUTO-PAUSES
// the instant any foreground budget is exceeded, and only auto-resumes after the
// signals have stayed healthy for a cool-down (hysteresis) so it does not flap on
// a single good sample. Manual pause and cancel override and are sticky (a
// cancelled job never proceeds again). Foreground always wins.
class MovementThrottle {
public:
    explicit MovementThrottle(SloBudgets budgets, uint32_t resumeHysteresisTicks = 3)
        : budgets_(budgets), hysteresis_(resumeHysteresisTicks == 0 ? 1 : resumeHysteresisTicks) {}

    void manualPause() { manualPaused_ = true; }
    void manualResume() { manualPaused_ = false; }
    void cancel() { cancelled_ = true; }
    bool cancelled() const { return cancelled_; }

    // Feed a fresh foreground sample; returns whether movement may proceed now.
    bool update(const ForegroundSignals& s) {
        if (exceeds(s)) {
            healthyStreak_ = 0;
            autoPaused_ = true;
        } else if (autoPaused_ && ++healthyStreak_ >= hysteresis_) {
            autoPaused_ = false;
        }
        return mayProceed();
    }

    bool mayProceed() const { return !cancelled_ && !manualPaused_ && !autoPaused_; }
    bool autoPaused() const { return autoPaused_; }

private:
    bool exceeds(const ForegroundSignals& s) const {
        return s.p99LatencyMs > budgets_.maxP99LatencyMs || s.errorRate > budgets_.maxErrorRate ||
               s.reactorStallMs > budgets_.maxReactorStallMs || s.queueDepth > budgets_.maxQueueDepth ||
               s.diskLatencyMs > budgets_.maxDiskLatencyMs;
    }

    SloBudgets budgets_;
    uint32_t hysteresis_;
    uint32_t healthyStreak_ = 0;
    bool autoPaused_ = false;
    bool manualPaused_ = false;
    bool cancelled_ = false;
};

}  // namespace timestar::movement

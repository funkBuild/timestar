#pragma once

// T-Digest approximate quantile estimator (Dunning & Ertl, 2019).
//
// This is the "merging" variant: incoming values are buffered, then sorted
// and merged into a set of centroids when the buffer is full.  The merge
// uses the scale function k1 = (delta/2) * arcsin(2q - 1) / pi, which
// gives higher accuracy at the tails (q near 0 or 1).
//
// Key properties:
//   - Compression factor delta=100 (default): ~200 max centroids, ~3.2KB
//   - Approximate median with ~0.01% relative error for typical data
//   - Mergeable: two TDigests can be combined in O(centroids) time
//   - No external dependencies (header-only, standard C++ only)
//
// Reference: https://arxiv.org/abs/1902.04023

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace timestar {

class TDigest {
public:
    // A centroid represents a cluster of values with a mean and a count.
    struct Centroid {
        double mean;
        double weight;  // number of values in this centroid

        bool operator<(const Centroid& other) const { return mean < other.mean; }
    };

    // Default compression factor.  Higher = more centroids = more accuracy.
    // delta=100 gives ~200 centroids max and ~0.01% error at the median.
    static constexpr double DEFAULT_DELTA = 100.0;

    // Buffer size before triggering a merge.  Larger buffers amortize the sort
    // cost but use more memory.  The internal buffer capacity is set to
    // roughly the max number of centroids to keep memory bounded.
    static constexpr size_t DEFAULT_BUFFER_CAPACITY = 500;

    explicit TDigest(double delta = DEFAULT_DELTA) : delta_(delta) {
        centroids_.reserve(static_cast<size_t>(delta) * 2 + DEFAULT_BUFFER_CAPACITY);
        buffer_.reserve(DEFAULT_BUFFER_CAPACITY);
    }

    // Add a single value.
    void add(double value, double weight = 1.0) {
        if (std::isnan(value))
            return;
        buffer_.push_back({value, weight});
        totalWeight_ += weight;
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        if (buffer_.size() >= DEFAULT_BUFFER_CAPACITY) {
            compress();
        }
    }

    // Merge another TDigest into this one.
    void merge(const TDigest& other) {
        if (other.empty())
            return;

        // Flush both buffers into centroids before merging.
        compress();

        // Combine all centroids from both digests.
        std::vector<Centroid> all;
        all.reserve(centroids_.size() + other.centroids_.size() + other.buffer_.size());
        all.insert(all.end(), centroids_.begin(), centroids_.end());
        all.insert(all.end(), other.centroids_.begin(), other.centroids_.end());
        all.insert(all.end(), other.buffer_.begin(), other.buffer_.end());

        totalWeight_ += other.totalWeight_;
        min_ = std::min(min_, other.min_);
        max_ = std::max(max_, other.max_);

        // Sort all centroids by mean.
        std::sort(all.begin(), all.end());

        // Merge into new centroid set using the scale function.
        centroids_.clear();
        mergeCentroids(all);
    }

    // Estimate the value at quantile q (0.0 to 1.0).
    // Returns NaN if the digest is empty.
    [[nodiscard]] double quantile(double q) const {
        if (empty())
            return std::numeric_limits<double>::quiet_NaN();
        if (q <= 0.0)
            return min_;
        if (q >= 1.0)
            return max_;

        // Need to flush buffer conceptually — work on a merged copy.
        // For const correctness, merge into a temporary if buffer is non-empty.
        if (!buffer_.empty()) {
            TDigest copy = *this;
            copy.compress();
            return copy.quantile(q);
        }

        if (centroids_.size() == 1) {
            return centroids_[0].mean;
        }

        // Build cumulative weight array.
        // Each centroid is conceptually centered at its cumulative midpoint.
        double target = q * totalWeight_;

        // Walk through centroids, interpolating.
        double cumWeight = 0.0;

        for (size_t i = 0; i < centroids_.size(); ++i) {
            double halfW = centroids_[i].weight / 2.0;
            double midpoint = cumWeight + halfW;

            if (target < midpoint) {
                // Target falls before or at this centroid's midpoint.
                if (i == 0) {
                    // Interpolate between min and first centroid mean.
                    double rightMid = midpoint;
                    if (rightMid <= 0.0)
                        return centroids_[0].mean;
                    double ratio = target / rightMid;
                    return min_ + ratio * (centroids_[0].mean - min_);
                }
                // Interpolate between previous centroid's midpoint and this one's.
                double prevHalfW = centroids_[i - 1].weight / 2.0;
                double prevMid = cumWeight - prevHalfW;
                double span = midpoint - prevMid;
                if (span <= 0.0)
                    return centroids_[i].mean;
                double ratio = (target - prevMid) / span;
                return centroids_[i - 1].mean + ratio * (centroids_[i].mean - centroids_[i - 1].mean);
            }

            cumWeight += centroids_[i].weight;
        }

        // Target is past the last centroid midpoint — interpolate to max.
        double lastHalfW = centroids_.back().weight / 2.0;
        double lastMid = totalWeight_ - lastHalfW;
        double span = totalWeight_ - lastMid;
        if (span <= 0.0)
            return max_;
        double ratio = (target - lastMid) / span;
        return centroids_.back().mean + ratio * (max_ - centroids_.back().mean);
    }

    // Convenience: median = quantile(0.5).
    [[nodiscard]] double median() const { return quantile(0.5); }

    // Returns true if no values have been added.
    [[nodiscard]] bool empty() const { return totalWeight_ == 0.0; }

    // Number of centroids (after compression).
    [[nodiscard]] size_t centroidCount() const { return centroids_.size() + buffer_.size(); }

    // Total number of values added.
    [[nodiscard]] double totalCount() const { return totalWeight_; }

private:
    // Flush the buffer: sort buffered values, combine with existing centroids,
    // and re-merge using the scale function.
    void compress() {
        if (buffer_.empty())
            return;

        // Combine buffer with existing centroids.
        std::vector<Centroid> all;
        all.reserve(centroids_.size() + buffer_.size());
        all.insert(all.end(), centroids_.begin(), centroids_.end());
        all.insert(all.end(), buffer_.begin(), buffer_.end());
        buffer_.clear();

        // Sort by mean.
        std::sort(all.begin(), all.end());

        // Re-merge using the scale function constraint.
        centroids_.clear();
        mergeCentroids(all);
    }

    // Merge sorted centroids into centroids_ using the k1 scale function.
    // This is the core of the "merging" t-digest algorithm.
    void mergeCentroids(const std::vector<Centroid>& sorted) {
        if (sorted.empty())
            return;

        centroids_.clear();
        centroids_.push_back(sorted[0]);

        double weightSoFar = sorted[0].weight;
        // k_limit: the maximum quantile position the current centroid can extend to.
        double qLimitLeft = 0.0;
        double qLimitRight = kInverse(kForward(qLimitLeft) + 1.0);

        for (size_t i = 1; i < sorted.size(); ++i) {
            double projectedWeight = centroids_.back().weight + sorted[i].weight;
            double qProject = (weightSoFar + sorted[i].weight) / totalWeight_;

            // Can we merge this centroid into the current one?
            // The constraint is that the merged centroid must not span more
            // than one unit of the scale function.
            if (qProject <= qLimitRight && projectedWeight <= totalWeight_ * (qLimitRight - qLimitLeft)) {
                // Merge into current centroid (weighted mean update).
                auto& c = centroids_.back();
                double newWeight = c.weight + sorted[i].weight;
                c.mean = (c.mean * c.weight + sorted[i].mean * sorted[i].weight) / newWeight;
                c.weight = newWeight;
            } else {
                // Start a new centroid.
                qLimitLeft = weightSoFar / totalWeight_;
                qLimitRight = kInverse(kForward(qLimitLeft) + 1.0);
                centroids_.push_back(sorted[i]);
            }

            weightSoFar += sorted[i].weight;
        }
    }

    // Scale function k1: maps quantile q to a scale value.
    // k1(q) = (delta / (2 * pi)) * arcsin(2q - 1)
    // This gives higher resolution (smaller centroids) near q=0 and q=1.
    [[nodiscard]] double kForward(double q) const {
        // Clamp to [0, 1] for safety.
        q = std::clamp(q, 0.0, 1.0);
        return (delta_ / (2.0 * M_PI)) * std::asin(2.0 * q - 1.0);
    }

    // Inverse of the scale function.
    // k1^{-1}(k) = (sin(2 * pi * k / delta) + 1) / 2
    [[nodiscard]] double kInverse(double k) const {
        double result = (std::sin(2.0 * M_PI * k / delta_) + 1.0) / 2.0;
        return std::clamp(result, 0.0, 1.0);
    }

    double delta_;
    double totalWeight_ = 0.0;
    double min_ = std::numeric_limits<double>::max();
    double max_ = std::numeric_limits<double>::lowest();
    std::vector<Centroid> centroids_;
    std::vector<Centroid> buffer_;
};

}  // namespace timestar

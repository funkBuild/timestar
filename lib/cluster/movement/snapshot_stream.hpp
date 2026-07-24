#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::movement {

// A streamed-snapshot integrity failure: a chunk hash or the whole-snapshot hash
// did not verify. Fail closed -- a corrupt transfer is never installed.
class SnapshotVerifyError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {
inline uint64_t fnv1a(const char* p, size_t n, uint64_t seed = 1469598103934665603ull) {
    uint64_t h = seed;
    for (size_t i = 0; i < n; ++i) {
        h ^= static_cast<uint8_t>(p[i]);
        h *= 1099511628211ull;
    }
    return h;
}
}  // namespace detail

struct SnapshotChunk {
    uint32_t index = 0;
    std::string data;
    uint64_t hash = 0;  // per-chunk integrity hash
};

// The sending side of a pinned snapshot: splits the payload into fixed-size chunks
// each carrying its own hash, plus a whole-snapshot verification hash. Resumable:
// the receiver asks for any chunk index, so a failed transfer restarts from the
// missing chunk rather than the beginning (docs/clustering.md §"Adding one
// machine": "streams them with resumable chunk hashes").
class SnapshotStreamSource {
public:
    SnapshotStreamSource(std::string payload, size_t chunkSize)
        : payload_(std::move(payload)), chunkSize_(chunkSize == 0 ? 1 : chunkSize) {}

    size_t chunkCount() const { return (payload_.size() + chunkSize_ - 1) / chunkSize_; }
    uint64_t wholeHash() const { return detail::fnv1a(payload_.data(), payload_.size()); }

    SnapshotChunk chunk(uint32_t index) const {
        // Bound the index BEFORE the multiply so an adversarial index cannot
        // overflow `off` and wrap below payload_.size() into a wrong chunk. Empty
        // payload has one empty chunk 0.
        if (index >= chunkCount() && !(payload_.empty() && index == 0))
            throw std::out_of_range("snapshot chunk index out of range");
        const size_t off = static_cast<size_t>(index) * chunkSize_;
        const size_t len = std::min(chunkSize_, payload_.size() - std::min(off, payload_.size()));
        SnapshotChunk c;
        c.index = index;
        c.data = payload_.substr(off, len);
        c.hash = detail::fnv1a(c.data.data(), c.data.size());
        return c;
    }

private:
    std::string payload_;
    size_t chunkSize_;
};

// The receiving side: accepts chunks in any order, verifying each chunk's hash on
// arrival (throwing on mismatch), and reassembles the payload. Idempotent/
// resumable -- a re-sent already-accepted chunk is a no-op. finish() returns the
// payload only when every chunk has arrived AND the whole-snapshot hash matches;
// otherwise it throws, so a truncated or corrupt snapshot is never installed.
class SnapshotStreamSink {
public:
    SnapshotStreamSink(size_t chunkCount, uint64_t wholeHash)
        : chunks_(chunkCount), have_(chunkCount, false), wholeHash_(wholeHash) {}

    // Accept a chunk; returns the next MISSING chunk index (== chunkCount when
    // complete). Throws SnapshotVerifyError on a per-chunk hash mismatch or an
    // out-of-range index.
    uint32_t accept(const SnapshotChunk& c) {
        if (c.index >= chunks_.size())
            throw SnapshotVerifyError("snapshot chunk index out of range");
        if (detail::fnv1a(c.data.data(), c.data.size()) != c.hash)
            throw SnapshotVerifyError("snapshot chunk hash mismatch");
        if (!have_[c.index]) {
            chunks_[c.index] = c.data;
            have_[c.index] = true;
        }
        return nextMissing();
    }

    bool complete() const {
        for (bool h : have_)
            if (!h)
                return false;
        return true;
    }
    uint32_t nextMissing() const {
        for (uint32_t i = 0; i < have_.size(); ++i)
            if (!have_[i])
                return i;
        return static_cast<uint32_t>(have_.size());
    }

    std::string finish() const {
        if (!complete())
            throw SnapshotVerifyError("snapshot incomplete");
        std::string out;
        for (const auto& c : chunks_)
            out += c;
        if (detail::fnv1a(out.data(), out.size()) != wholeHash_)
            throw SnapshotVerifyError("whole-snapshot hash mismatch");
        return out;
    }

private:
    std::vector<std::string> chunks_;
    std::vector<bool> have_;
    uint64_t wholeHash_;
};

}  // namespace timestar::movement

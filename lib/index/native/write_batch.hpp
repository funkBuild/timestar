#ifndef NATIVE_INDEX_WRITE_BATCH_H_INCLUDED
#define NATIVE_INDEX_WRITE_BATCH_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

class MemTable;

// Buffers Put/Delete operations and applies them atomically to a MemTable.
// Also supports serialization for WAL persistence.
class IndexWriteBatch {
public:
    enum class OpType : uint8_t { Put = 0, Delete = 1 };

    struct Op {
        OpType type;
        std::string key;
        std::string value;  // Empty for Delete
    };

    void put(std::string_view key, std::string_view value);
    void remove(std::string_view key);
    void clear();

    size_t count() const { return ops_.size(); }
    bool empty() const { return ops_.empty(); }

    // Approximate memory usage of the batch
    size_t approximateSize() const { return approxSize_; }

    const std::vector<Op>& ops() const { return ops_; }

    // Apply all operations to a MemTable atomically.
    void applyTo(MemTable& memtable) const;

    // Serialization for WAL persistence.
    // Format: [op_count (4 bytes LE)] [per op: type(1) key_len(4) key value_len(4) value]
    void serializeTo(std::string& output) const;
    static IndexWriteBatch deserializeFrom(std::string_view data);

private:
    std::vector<Op> ops_;
    size_t approxSize_ = 0;
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_WRITE_BATCH_H_INCLUDED

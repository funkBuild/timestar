#include "write_batch.hpp"

#include "memtable.hpp"

#include <cstring>
#include <stdexcept>

namespace timestar::index {

void IndexWriteBatch::put(std::string_view key, std::string_view value) {
    approxSize_ += key.size() + value.size() + sizeof(Op);
    ops_.push_back({OpType::Put, std::string(key), std::string(value)});
}

void IndexWriteBatch::remove(std::string_view key) {
    approxSize_ += key.size() + sizeof(Op);
    ops_.push_back({OpType::Delete, std::string(key), {}});
}

void IndexWriteBatch::clear() {
    ops_.clear();
    approxSize_ = 0;
}

void IndexWriteBatch::applyTo(MemTable& memtable) const {
    for (const auto& op : ops_) {
        if (op.type == OpType::Put) {
            memtable.put(op.key, op.value);
        } else {
            memtable.remove(op.key);
        }
    }
}

static void appendFixed32(std::string& out, uint32_t v) {
    out.push_back(static_cast<char>(v & 0xff));
    out.push_back(static_cast<char>((v >> 8) & 0xff));
    out.push_back(static_cast<char>((v >> 16) & 0xff));
    out.push_back(static_cast<char>((v >> 24) & 0xff));
}

static uint32_t readFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<uint8_t>(p[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
}

void IndexWriteBatch::serializeTo(std::string& output) const {
    appendFixed32(output, static_cast<uint32_t>(ops_.size()));

    for (const auto& op : ops_) {
        output.push_back(static_cast<char>(op.type));
        appendFixed32(output, static_cast<uint32_t>(op.key.size()));
        output.append(op.key);
        if (op.type == OpType::Put) {
            appendFixed32(output, static_cast<uint32_t>(op.value.size()));
            output.append(op.value);
        }
    }
}

IndexWriteBatch IndexWriteBatch::deserializeFrom(std::string_view data) {
    IndexWriteBatch batch;
    if (data.size() < 4) {
        throw std::runtime_error("WriteBatch: data too short for op count");
    }

    const char* p = data.data();
    const char* end = data.data() + data.size();

    uint32_t count = readFixed32(p);
    p += 4;

    for (uint32_t i = 0; i < count; ++i) {
        if (p >= end) throw std::runtime_error("WriteBatch: truncated at op type");

        auto type = static_cast<IndexWriteBatch::OpType>(*p);
        ++p;

        if (p + 4 > end) throw std::runtime_error("WriteBatch: truncated at key length");
        uint32_t keyLen = readFixed32(p);
        p += 4;

        if (p + keyLen > end) throw std::runtime_error("WriteBatch: truncated at key data");
        std::string key(p, keyLen);
        p += keyLen;

        if (type == OpType::Put) {
            if (p + 4 > end) throw std::runtime_error("WriteBatch: truncated at value length");
            uint32_t valLen = readFixed32(p);
            p += 4;
            if (p + valLen > end) throw std::runtime_error("WriteBatch: truncated at value data");
            batch.put(key, std::string_view(p, valLen));
            p += valLen;
        } else {
            batch.remove(key);
        }
    }

    return batch;
}

}  // namespace timestar::index

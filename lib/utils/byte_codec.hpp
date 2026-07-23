#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

// Little-endian byte encode/decode primitives shared by the cluster on-disk
// formats (journal records, series catalog, ...). The Reader is a
// bounds-checked cursor: every read that would run past the end sets a sticky
// failure flag and returns a zero/empty value rather than over-reading, so a
// decoder over untrusted bytes is safe by construction -- callers check ok() at
// the end instead of hand-tracing offsets.
namespace timestar::codec {

inline void putU8(std::string& out, uint8_t v) {
    out.push_back(static_cast<char>(v));
}

inline void putU16(std::string& out, uint16_t v) {
    out.push_back(static_cast<char>(v & 0xff));
    out.push_back(static_cast<char>((v >> 8) & 0xff));
}

inline void putU32(std::string& out, uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

inline void putU64(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

// Length-prefixed (u32) string.
inline void putStr(std::string& out, std::string_view s) {
    putU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}

class Reader {
public:
    explicit Reader(std::span<const char> data) : data_(data) {}

    [[nodiscard]] bool ok() const noexcept { return ok_; }
    [[nodiscard]] size_t consumed() const noexcept { return pos_; }
    [[nodiscard]] size_t remaining() const noexcept { return ok_ ? data_.size() - pos_ : 0; }
    // True once all input has been consumed with no failure.
    [[nodiscard]] bool exhausted() const noexcept { return ok_ && pos_ == data_.size(); }

    uint8_t u8() {
        const char* p = read(1);
        return p ? static_cast<uint8_t>(p[0]) : 0;
    }
    uint16_t u16() {
        const char* p = read(2);
        return p ? (static_cast<uint16_t>(static_cast<uint8_t>(p[0])) |
                    static_cast<uint16_t>(static_cast<uint8_t>(p[1]) << 8))
                 : 0;
    }
    uint32_t u32() {
        const char* p = read(4);
        if (!p)
            return 0;
        uint32_t r = 0;
        for (int i = 0; i < 4; ++i)
            r |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (i * 8);
        return r;
    }
    uint64_t u64() {
        const char* p = read(8);
        if (!p)
            return 0;
        uint64_t r = 0;
        for (int i = 0; i < 8; ++i)
            r |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
        return r;
    }

    // Length-prefixed (u32) string. A length exceeding the remaining input fails
    // the reader rather than allocating from a corrupt size.
    std::string str() {
        const uint32_t n = u32();
        if (!ok_)
            return {};
        const char* p = read(n);
        return p ? std::string(p, n) : std::string{};
    }

    // Exactly n raw bytes.
    std::string bytes(size_t n) {
        const char* p = read(n);
        return p ? std::string(p, n) : std::string{};
    }

private:
    const char* read(size_t n) {
        if (!ok_ || data_.size() - pos_ < n) {
            ok_ = false;
            return nullptr;
        }
        const char* p = data_.data() + pos_;
        pos_ += n;
        return p;
    }

    std::span<const char> data_;
    size_t pos_ = 0;
    bool ok_ = true;
};

}  // namespace timestar::codec

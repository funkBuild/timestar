#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

class Simple8BException : public std::runtime_error {
public:
    Simple8BException(const std::string& message) : std::runtime_error(message) {}
};

class Simple8BValueTooLargeException : public Simple8BException {
private:
    uint64_t value_;
    size_t offset_;
    static constexpr uint64_t MAX_ENCODABLE_VALUE = (1ULL << 60) - 1;

public:
    Simple8BValueTooLargeException(uint64_t value, size_t offset)
        : Simple8BException(createMessage(value, offset)), value_(value), offset_(offset) {}

    uint64_t getValue() const { return value_; }
    size_t getOffset() const { return offset_; }
    static uint64_t getMaxEncodableValue() { return MAX_ENCODABLE_VALUE; }

private:
    static std::string createMessage(uint64_t value, size_t offset) {
        return "Simple8B encoding failed: Value " + std::to_string(value) + " at offset " + std::to_string(offset) +
               " exceeds maximum encodable value of " + std::to_string(MAX_ENCODABLE_VALUE) + " (60-bit limit)";
    }
};

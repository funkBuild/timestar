#include "simple16.hpp"
#include "util.hpp"
#include <iostream>
#include <stdexcept>

AlignedBuffer Simple16::encode(std::vector<uint64_t> &values){
    size_t offset = 0;
    AlignedBuffer buffer;

    while(offset < values.size()){
        // Try each packing scheme from most values to least
        // Schemes with more values for the same bit width are tried first
        // Scheme 13: 30 values of 2 bits (strictly better than scheme 0's 28x2bit)
        if(canPack<30, 2>(values, offset)) {
            uint64_t val = pack<13, 30, 2>(values, offset);
            buffer.write(val);
        }
        // Scheme 0: 28 values of 2 bits each
        else if(canPack<28, 2>(values, offset)) {
            uint64_t val = pack<0, 28, 2>(values, offset);
            buffer.write(val);
        }
        // Scheme 1: 20 values of 3 bits each
        else if(canPack<20, 3>(values, offset)) {
            uint64_t val = pack<1, 20, 3>(values, offset);
            buffer.write(val);
        }
        // Scheme 2: 15 values of 4 bits each
        else if(canPack<15, 4>(values, offset)) {
            uint64_t val = pack<2, 15, 4>(values, offset);
            buffer.write(val);
        }
        // Scheme 14: 14 values of 4 bits (alternative to scheme 2)
        else if(canPack<14, 4>(values, offset)) {
            uint64_t val = pack<14, 14, 4>(values, offset);
            buffer.write(val);
        }
        // Scheme 3: 12 values of 5 bits each
        else if(canPack<12, 5>(values, offset)) {
            uint64_t val = pack<3, 12, 5>(values, offset);
            buffer.write(val);
        }
        // Scheme 4: 10 values of 6 bits each
        else if(canPack<10, 6>(values, offset)) {
            uint64_t val = pack<4, 10, 6>(values, offset);
            buffer.write(val);
        }
        // Scheme 5: 8 values of 7 bits each (8*7=56, fits in 60 bits)
        else if(canPack<8, 7>(values, offset)) {
            uint64_t val = pack<5, 8, 7>(values, offset);
            buffer.write(val);
        }
        // Scheme 6: 7 values of 8 bits each (7*8=56)
        else if(canPack<7, 8>(values, offset)) {
            uint64_t val = pack<6, 7, 8>(values, offset);
            buffer.write(val);
        }
        // Scheme 7: 6 values of 10 bits each
        else if(canPack<6, 10>(values, offset)) {
            uint64_t val = pack<7, 6, 10>(values, offset);
            buffer.write(val);
        }
        // Scheme 8: 5 values of 12 bits each
        else if(canPack<5, 12>(values, offset)) {
            uint64_t val = pack<8, 5, 12>(values, offset);
            buffer.write(val);
        }
        // Scheme 9: 4 values of 15 bits each
        else if(canPack<4, 15>(values, offset)) {
            uint64_t val = pack<9, 4, 15>(values, offset);
            buffer.write(val);
        }
        // Scheme 10: 3 values of 20 bits each
        else if(canPack<3, 20>(values, offset)) {
            uint64_t val = pack<10, 3, 20>(values, offset);
            buffer.write(val);
        }
        // Scheme 11: 2 values of 30 bits each
        else if(canPack<2, 30>(values, offset)) {
            uint64_t val = pack<11, 2, 30>(values, offset);
            buffer.write(val);
        }
        // Scheme 12: 1 value of 60 bits
        else if(canPack<1, 60>(values, offset)) {
            uint64_t val = pack<12, 1, 60>(values, offset);
            buffer.write(val);
        }
        // Scheme 15: Store one 64-bit value uncompressed (using two words)
        else {
            packLarge(values, offset, buffer);
        }
    }

    return buffer;
}

std::vector<uint64_t> Simple16::decode(Slice &encoded, unsigned int size){
    std::vector<uint64_t> values;
    values.reserve(size);

    while(encoded.length<uint64_t>() > 0 && values.size() < size){
        uint64_t packedValue = encoded.read<uint64_t>();
        uint64_t selector = packedValue >> 60;

        switch(selector){
            case 0:
                unpack<28, 2>(packedValue, values);
                break;
            case 1:
                unpack<20, 3>(packedValue, values);
                break;
            case 2:
                unpack<15, 4>(packedValue, values);
                break;
            case 3:
                unpack<12, 5>(packedValue, values);
                break;
            case 4:
                unpack<10, 6>(packedValue, values);
                break;
            case 5:
                unpack<8, 7>(packedValue, values);
                break;
            case 6:
                unpack<7, 8>(packedValue, values);
                break;
            case 7:
                unpack<6, 10>(packedValue, values);
                break;
            case 8:
                unpack<5, 12>(packedValue, values);
                break;
            case 9:
                unpack<4, 15>(packedValue, values);
                break;
            case 10:
                unpack<3, 20>(packedValue, values);
                break;
            case 11:
                unpack<2, 30>(packedValue, values);
                break;
            case 12:
                unpack<1, 60>(packedValue, values);
                break;
            case 13:
                unpack<30, 2>(packedValue, values);
                break;
            case 14:
                unpack<14, 4>(packedValue, values);
                break;
            case 15:
                // Special case: full 64-bit value follows
                unpackLarge(encoded, values);
                break;
        }
    }

    return values;
}

template<uint64_t n, uint64_t bits>
bool Simple16::canPack(std::vector<uint64_t> &values, size_t offset){
    if(offset > values.size()) return false;
    size_t remaining = values.size() - offset;
    if(remaining < n)
        return false;

    uint64_t max = (1ull << bits) - 1;

    for(size_t i = offset; i < (offset+n); i++){
        if(values[i] > max)
            return false;
    }

    return true;
}

template<uint64_t selector, uint64_t n, uint64_t bits>
uint64_t Simple16::pack(std::vector<uint64_t> &values, size_t &offset){
    uint64_t out = selector << 60;
    constexpr uint64_t mask = (bits == 0) ? 0ULL : ((1ULL << bits) - 1);

    for(unsigned int i = 0; i < n; i++){
        out |= (values[offset + i] & mask) << (i*bits);
    }

    offset += n;

    return out;
}

template<uint64_t n, uint64_t bits>
void Simple16::unpack(uint64_t value, std::vector<uint64_t> &out){
    const uint64_t mask = (1ull << bits) - 1;

    loop<int, n>([&out, value, mask] (auto i) {
        constexpr int shiftAmount = i * bits;
        uint64_t v = (value >> shiftAmount) & mask;
        out.push_back(v);
    });
}

// Special handling for 64-bit values
void Simple16::packLarge(std::vector<uint64_t> &values, size_t &offset, AlignedBuffer &buffer) {
    // Selector 15 indicates a full 64-bit value follows
    // For simplicity, we use two words:
    // First word: selector 15 (in top 4 bits) + zeros in remaining 60 bits
    // Second word: the full 64-bit value
    uint64_t firstWord = 15ULL << 60;  // Just the selector
    buffer.write(firstWord);
    
    // Second word: the full original value
    buffer.write(values[offset]);
    
    offset++;
}

void Simple16::unpackLarge(Slice &encoded, std::vector<uint64_t> &out) {
    // When we see selector 15, the next word contains the full 64-bit value
    if (encoded.length<uint64_t>() == 0) {
        throw std::runtime_error("Simple16 decode: truncated stream at large value (selector 15)");
    }
    uint64_t fullValue = encoded.read<uint64_t>();
    out.push_back(fullValue);
}

size_t Simple16::encodeInto(std::vector<uint64_t> &values, AlignedBuffer &target) {
    const size_t startPos = target.size();
    size_t offset = 0;

    while (offset < values.size()) {
        if (canPack<30, 2>(values, offset)) {
            uint64_t val = pack<13, 30, 2>(values, offset);
            target.write(val);
        } else if (canPack<28, 2>(values, offset)) {
            uint64_t val = pack<0, 28, 2>(values, offset);
            target.write(val);
        } else if (canPack<20, 3>(values, offset)) {
            uint64_t val = pack<1, 20, 3>(values, offset);
            target.write(val);
        } else if (canPack<15, 4>(values, offset)) {
            uint64_t val = pack<2, 15, 4>(values, offset);
            target.write(val);
        } else if (canPack<14, 4>(values, offset)) {
            uint64_t val = pack<14, 14, 4>(values, offset);
            target.write(val);
        } else if (canPack<12, 5>(values, offset)) {
            uint64_t val = pack<3, 12, 5>(values, offset);
            target.write(val);
        } else if (canPack<10, 6>(values, offset)) {
            uint64_t val = pack<4, 10, 6>(values, offset);
            target.write(val);
        } else if (canPack<8, 7>(values, offset)) {
            uint64_t val = pack<5, 8, 7>(values, offset);
            target.write(val);
        } else if (canPack<7, 8>(values, offset)) {
            uint64_t val = pack<6, 7, 8>(values, offset);
            target.write(val);
        } else if (canPack<6, 10>(values, offset)) {
            uint64_t val = pack<7, 6, 10>(values, offset);
            target.write(val);
        } else if (canPack<5, 12>(values, offset)) {
            uint64_t val = pack<8, 5, 12>(values, offset);
            target.write(val);
        } else if (canPack<4, 15>(values, offset)) {
            uint64_t val = pack<9, 4, 15>(values, offset);
            target.write(val);
        } else if (canPack<3, 20>(values, offset)) {
            uint64_t val = pack<10, 3, 20>(values, offset);
            target.write(val);
        } else if (canPack<2, 30>(values, offset)) {
            uint64_t val = pack<11, 2, 30>(values, offset);
            target.write(val);
        } else if (canPack<1, 60>(values, offset)) {
            uint64_t val = pack<12, 1, 60>(values, offset);
            target.write(val);
        } else {
            packLarge(values, offset, target);
        }
    }

    return target.size() - startPos;
}
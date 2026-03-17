#pragma once

#include "compressed_buffer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iosfwd>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

// Allocator that:
//  1. Aligns every allocation to DMA_ALIGNMENT (4096) so the buffer can be
//     passed directly to Seastar's dma_write() without an intermediate copy.
//  2. Default-initializes (i.e. leaves memory uninitialized) on resize,
//     avoiding the implicit memset from std::vector<uint8_t>::resize().
//     Every AlignedBuffer caller immediately overwrites newly-allocated bytes
//     via memcpy, so value-initialization is pure waste.
template <typename T, std::size_t Alignment = 4096>
struct dma_default_init_allocator {
    using value_type = T;

    // Required by std::allocator_traits for rebinding (e.g. inside std::vector
    // internals).  Preserves the Alignment parameter across type rebinds.
    template <typename U>
    struct rebind {
        using other = dma_default_init_allocator<U, Alignment>;
    };

    dma_default_init_allocator() noexcept = default;

    template <typename U, std::size_t A>
    dma_default_init_allocator(const dma_default_init_allocator<U, A>&) noexcept {}

    T* allocate(std::size_t n) {
        const std::size_t bytes = n * sizeof(T);
        // std::aligned_alloc requires size to be a multiple of alignment.
        const std::size_t padded = (bytes + Alignment - 1) & ~(Alignment - 1);
        void* p = std::aligned_alloc(Alignment, padded);
        if (!p)
            throw std::bad_alloc();
        return static_cast<T*>(p);
    }

    void deallocate(T* p, std::size_t) noexcept { std::free(p); }

    // Default construction: leave memory uninitialized (default-init for
    // trivial types like uint8_t is a no-op).
    void construct(T* p) noexcept(std::is_nothrow_default_constructible_v<T>) {
        ::new (static_cast<void*>(p)) T;  // default-init, NOT value-init
    }

    // Non-default construction: forward arguments as usual.
    template <typename... Args>
    void construct(T* p, Args&&... args) {
        ::new (static_cast<void*>(p)) T(std::forward<Args>(args)...);
    }

    template <typename U, std::size_t A>
    bool operator==(const dma_default_init_allocator<U, A>&) const noexcept {
        return true;
    }
};

class AlignedBuffer {
private:
    static constexpr size_t INITIAL_CAPACITY = 4096;
    static constexpr size_t GROWTH_FACTOR = 2;

    size_t current_size = 0;

    // Ensure we have enough capacity
    void ensure_capacity(size_t required);

public:
    // DMA alignment used by the underlying allocator.  Matches Seastar's
    // default memory_dma_alignment (4096).  The buffer's base address is
    // always aligned to this value, so closeDMA() can write directly
    // from data.data() without an intermediate memcpy.
    static constexpr size_t DMA_ALIGNMENT = 4096;

    std::vector<uint8_t, dma_default_init_allocator<uint8_t, DMA_ALIGNMENT>> data;

    explicit AlignedBuffer(size_t initialSize = 0) {
        if (initialSize > 0) {
            data.reserve(std::max(initialSize, INITIAL_CAPACITY));
            data.resize(initialSize);
            current_size = initialSize;
        } else {
            data.reserve(INITIAL_CAPACITY);
        }
    };

    template <class T>
    void write(T value);
    void write(const std::string& value);
    void write(const CompressedBuffer& value);
    void write(const AlignedBuffer& value);
    void write(std::_Bit_reference value);

    // Bulk write raw bytes (for compressed data, etc.)
    void write_bytes(const char* bytes, size_t count);

    // Bulk write for arrays
    template <typename T>
    void write_array(const T* values, size_t count) {
        // Guard against overflow: sizeof(T) * count and the subsequent add
        if (count > 0 && count > SIZE_MAX / sizeof(T)) [[unlikely]] {
            throw std::runtime_error("AlignedBuffer::write_array overflow");
        }
        const size_t bytesToAdd = sizeof(T) * count;
        const size_t new_size = current_size + bytesToAdd;
        if (new_size < current_size) [[unlikely]] {
            throw std::runtime_error("AlignedBuffer::write_array overflow");
        }

        ensure_capacity(new_size);

        std::memcpy(data.data() + current_size, values, bytesToAdd);
        current_size = new_size;
    }

    // Read 8 bytes as uint64_t using memcpy to avoid strict aliasing violation
    uint64_t read64(size_t offset) const {
        if (offset + sizeof(uint64_t) > current_size) {
            throw std::runtime_error("AlignedBuffer::read64 out of bounds: offset=" + std::to_string(offset) +
                                     " size=" + std::to_string(current_size));
        }
        uint64_t val;
        std::memcpy(&val, data.data() + offset, sizeof(uint64_t));
        return val;
    }

    // Read a single byte
    uint8_t read8(size_t offset) const {
        if (offset >= current_size) {
            throw std::runtime_error("AlignedBuffer::read8 out of bounds: offset=" + std::to_string(offset) +
                                     " size=" + std::to_string(current_size));
        }
        return data[offset];
    }

    // Write a value at a specific offset (for backpatching headers).
    // The target region must already be within the buffer's logical size.
    template <class T>
    void writeAt(size_t offset, T value) {
        if (offset + sizeof(T) > current_size) [[unlikely]] {
            throw std::runtime_error("AlignedBuffer::writeAt out of bounds: offset=" + std::to_string(offset) +
                                     " sizeof(T)=" + std::to_string(sizeof(T)) +
                                     " size=" + std::to_string(current_size));
        }
        std::memcpy(data.data() + offset, &value, sizeof(T));
    }

    // Grow the buffer by `extra_bytes` and return a pointer to the start
    // of the newly-available region. The new bytes are left uninitialized
    // (the caller must fill them).
    uint8_t* grow_uninit(size_t extra_bytes) {
        const size_t new_size = current_size + extra_bytes;
        ensure_capacity(new_size);
        uint8_t* ptr = data.data() + current_size;
        current_size = new_size;
        return ptr;
    }

    size_t size() const { return current_size; }
    size_t capacity() const { return data.capacity(); }

    // Reserve capacity upfront when size is known
    void reserve(size_t capacity) { data.reserve(capacity); }

    // Clear the buffer but keep allocated memory
    void clear() { current_size = 0; }

    // Shrink to fit after operations
    void shrink_to_fit() {
        data.resize(current_size);
        data.shrink_to_fit();
    }
};

std::ofstream& operator<<(std::ofstream& os, const AlignedBuffer& buf);

#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

template <typename T>
inline size_t getLeadingZeroBitsUnsafe(T x) {
    assert(x != 0);
    return static_cast<size_t>(std::countl_zero(static_cast<std::make_unsigned_t<T>>(x)));
}

template <typename T>
inline size_t getLeadingZeroBits(T x) {
    return static_cast<size_t>(std::countl_zero(static_cast<std::make_unsigned_t<T>>(x)));
}

template <typename T>
inline size_t getTrailingZeroBitsUnsafe(T x) {
    assert(x != 0);
    return static_cast<size_t>(std::countr_zero(static_cast<std::make_unsigned_t<T>>(x)));
}

template <typename T>
inline size_t getTrailingZeroBits(T x) {
    return static_cast<size_t>(std::countr_zero(static_cast<std::make_unsigned_t<T>>(x)));
}

inline bool endsWith(std::string const& value, std::string const& ending) {
    if (ending.size() > value.size())
        return false;
    return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

template <class T, T... inds, class F>
constexpr void loop(std::integer_sequence<T, inds...>, F&& f) {
    (f(std::integral_constant<T, inds>{}), ...);  // C++17 fold expression
}

template <class T, T count, class F>
constexpr void loop(F&& f) {
    loop(std::make_integer_sequence<T, count>{}, std::forward<F>(f));
}
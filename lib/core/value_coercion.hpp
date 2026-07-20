#pragma once

// Lossless value coercion between the four TimeStar value types.
//
// A series' id is a hash of measurement+tags+field only (series_id.hpp), so the
// same id can be written as two different types. Ingest binds a series to the
// type of its first write and holds it there; a later write of a different type
// is converted into the bound type when — and only when — that conversion
// cannot lose information and has exactly one sensible reading.
//
// Everything else fails, and the caller rejects the write rather than storing a
// value the writer did not mean. `"abcd"` does not become NaN or 0, and 10.5
// does not become 10.
//
// Returning std::nullopt means "not coercible"; it is never a value.

#include "tsm.hpp"  // TSMValueType

#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

namespace timestar {

namespace detail {

// Shortest representation that reads back as the same double.
inline std::string doubleToString(double v) {
    char buf[40];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    if (ec != std::errc{})
        return std::to_string(v);
    return std::string(buf, ptr);
}

// Strict: the whole string must be consumed, so "12abc" and "" both fail.
template <class N>
inline std::optional<N> parseWhole(std::string_view s) {
    if (s.empty())
        return std::nullopt;
    N out{};
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    if (ec != std::errc{} || ptr != s.data() + s.size())
        return std::nullopt;
    return out;
}

}  // namespace detail

// coerceValue<To>(from) — std::nullopt when the conversion would lose
// information or is ambiguous.
template <class To, class From>
std::optional<To> coerceValue(const From& v);

// ---- to double ----
template <>
inline std::optional<double> coerceValue<double, bool>(const bool& v) {
    return v ? 1.0 : 0.0;
}
template <>
inline std::optional<double> coerceValue<double, int64_t>(const int64_t& v) {
    // Beyond 2^53 an int64 no longer round-trips through a double.
    auto d = static_cast<double>(v);
    if (static_cast<int64_t>(d) != v)
        return std::nullopt;
    return d;
}
template <>
inline std::optional<double> coerceValue<double, std::string>(const std::string& v) {
    return detail::parseWhole<double>(v);
}

// ---- to bool ----
// Only exactly 0 or 1 map back to a boolean; 5 would discard the 5.
template <>
inline std::optional<bool> coerceValue<bool, double>(const double& v) {
    if (v == 0.0)
        return false;
    if (v == 1.0)
        return true;
    return std::nullopt;
}
template <>
inline std::optional<bool> coerceValue<bool, int64_t>(const int64_t& v) {
    if (v == 0)
        return false;
    if (v == 1)
        return true;
    return std::nullopt;
}
template <>
inline std::optional<bool> coerceValue<bool, std::string>(const std::string& v) {
    if (v == "true")
        return true;
    if (v == "false")
        return false;
    return std::nullopt;
}

// ---- to string ----
template <>
inline std::optional<std::string> coerceValue<std::string, double>(const double& v) {
    return detail::doubleToString(v);
}
template <>
inline std::optional<std::string> coerceValue<std::string, bool>(const bool& v) {
    return std::string(v ? "true" : "false");
}
template <>
inline std::optional<std::string> coerceValue<std::string, int64_t>(const int64_t& v) {
    return std::to_string(v);
}

// ---- to int64 ----
template <>
inline std::optional<int64_t> coerceValue<int64_t, double>(const double& v) {
    // Integral values only: 10.0 -> 10 is the JSON-serialiser case and is
    // lossless; 10.5 -> 10 would destroy data.
    if (!std::isfinite(v) || v != std::trunc(v))
        return std::nullopt;
    if (v < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
        v > static_cast<double>(std::numeric_limits<int64_t>::max()))
        return std::nullopt;
    return static_cast<int64_t>(v);
}
template <>
inline std::optional<int64_t> coerceValue<int64_t, bool>(const bool& v) {
    return v ? int64_t{1} : int64_t{0};
}
template <>
inline std::optional<int64_t> coerceValue<int64_t, std::string>(const std::string& v) {
    return detail::parseWhole<int64_t>(v);
}

// Identity overloads — a batch already matching its binding never reaches the
// conversion path, but the template must still instantiate for every pair.
template <>
inline std::optional<double> coerceValue<double, double>(const double& v) {
    return v;
}
template <>
inline std::optional<bool> coerceValue<bool, bool>(const bool& v) {
    return v;
}
template <>
inline std::optional<std::string> coerceValue<std::string, std::string>(const std::string& v) {
    return v;
}
template <>
inline std::optional<int64_t> coerceValue<int64_t, int64_t>(const int64_t& v) {
    return v;
}

// Human-readable form of a value, for the rejection message.
template <class T>
inline std::string describeValue(const T& v) {
    if constexpr (std::is_same_v<T, std::string>)
        return "\"" + v + "\"";
    else if constexpr (std::is_same_v<T, bool>)
        return v ? "true" : "false";
    else if constexpr (std::is_same_v<T, double>)
        return detail::doubleToString(v);
    else
        return std::to_string(v);
}

}  // namespace timestar

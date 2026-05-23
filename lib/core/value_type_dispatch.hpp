#pragma once

// Compile- and runtime-dispatch primitives over the four TimeStar value
// types (double, bool, std::string, int64_t).
//
// The codebase has many 4-arm switch/if-constexpr blocks that branch on
// TSMValueType (a runtime tag) or std::is_same_v<T, X> (a compile-time
// tag), one branch per supported type. These templates fold those blocks
// into a single line, eliminating drift between parallel arms and making
// it trivial to add or remove a supported type in one place.
//
// Usage (C++23 generic lambda with explicit template params):
//
//   auto result = timestar::dispatchValueType(seriesType, [&]<class T>() {
//       return doSomething<T>();
//   });
//
//   timestar::forEachValueType([&]<class T>() { register<T>(); });
//
// The dispatch order matches both `enum class TSMValueType` numeric values
// and the variant ordering in InMemorySeries' std::variant, so that
// `static_cast<TSMValueType>(variant.index())` round-trips.

#include "tsm.hpp"  // TSMValueType (Float=0, Boolean, String, Integer)

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace timestar {

// Canonical lowercase name for each value type. Used in metadata API
// (/fields response: "float" | "boolean" | "string" | "integer") and in
// the field-type cache.
inline std::string_view valueTypeName(TSMValueType t) noexcept {
    switch (t) {
        case TSMValueType::Float:
            return "float";
        case TSMValueType::Boolean:
            return "boolean";
        case TSMValueType::String:
            return "string";
        case TSMValueType::Integer:
            return "integer";
    }
    return "";
}

// Compile-time TSMValueType → C++ type mapping.
template <TSMValueType T>
struct ValueTypeCpp;
template <>
struct ValueTypeCpp<TSMValueType::Float> {
    using type = double;
};
template <>
struct ValueTypeCpp<TSMValueType::Boolean> {
    using type = bool;
};
template <>
struct ValueTypeCpp<TSMValueType::String> {
    using type = std::string;
};
template <>
struct ValueTypeCpp<TSMValueType::Integer> {
    using type = int64_t;
};

template <TSMValueType T>
using value_type_cpp_t = typename ValueTypeCpp<T>::type;

// Inverse mapping: C++ type -> TSMValueType (compile-time).
template <class T>
constexpr TSMValueType valueTypeOf();
template <>
constexpr TSMValueType valueTypeOf<double>() {
    return TSMValueType::Float;
}
template <>
constexpr TSMValueType valueTypeOf<bool>() {
    return TSMValueType::Boolean;
}
template <>
constexpr TSMValueType valueTypeOf<std::string>() {
    return TSMValueType::String;
}
template <>
constexpr TSMValueType valueTypeOf<int64_t>() {
    return TSMValueType::Integer;
}

// Invoke `f.template operator()<T>()` (or `f.operator()<T>()` via a C++23
// generic lambda) for the C++ type matching the runtime TSMValueType.
// Forwards the callable's return value. Throws on unknown enum values.
template <class F>
decltype(auto) dispatchValueType(TSMValueType t, F&& f) {
    switch (t) {
        case TSMValueType::Float:
            return f.template operator()<double>();
        case TSMValueType::Boolean:
            return f.template operator()<bool>();
        case TSMValueType::String:
            return f.template operator()<std::string>();
        case TSMValueType::Integer:
            return f.template operator()<int64_t>();
    }
    throw std::runtime_error("dispatchValueType: unknown TSMValueType");
}

// Invoke `f.template operator()<T>()` once for every value type, in the
// canonical enum order. Use for things like template instantiations,
// per-type bookkeeping in tests, or fanning out per-type storage.
template <class F>
void forEachValueType(F&& f) {
    f.template operator()<double>();
    f.template operator()<bool>();
    f.template operator()<std::string>();
    f.template operator()<int64_t>();
}

}  // namespace timestar

// Macro to instantiate `FN(T)` for each TimeStar value type. Use in .cpp
// files to fan out explicit template instantiations (a place where the
// dispatchValueType template can't help, since instantiation is a parse-time
// concern). Example:
//
//   #define INST(T) template void Foo::bar<T>(T x);
//   TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST)
//   #undef INST
#define TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(FN) \
    FN(double)                                   \
    FN(bool)                                     \
    FN(std::string)                              \
    FN(int64_t)

#pragma once

#include <glaze/glaze.hpp>

#include <optional>
#include <string>

struct DownsamplePolicy {
    std::string after;  // Duration string, e.g. "30d"
    uint64_t afterNanos = 0;
    std::string interval;  // Duration string, e.g. "5m"
    uint64_t intervalNanos = 0;
    std::string method;  // "avg", "min", "max", "sum", "latest"
};

template <>
struct glz::meta<DownsamplePolicy> {
    using T = DownsamplePolicy;
    static constexpr auto value = object("after", &T::after, "afterNanos", &T::afterNanos, "interval", &T::interval,
                                         "intervalNanos", &T::intervalNanos, "method", &T::method);
};

struct RetentionPolicy {
    std::string measurement;
    std::string ttl;  // Duration string, e.g. "90d"
    uint64_t ttlNanos = 0;
    std::optional<DownsamplePolicy> downsample;
};

template <>
struct glz::meta<RetentionPolicy> {
    using T = RetentionPolicy;
    static constexpr auto value =
        object("measurement", &T::measurement, "ttl", &T::ttl, "ttlNanos", &T::ttlNanos, "downsample", &T::downsample);
};

// Request structure for PUT /retention (ttl and downsample are optional)
struct RetentionPolicyRequest {
    std::string measurement;
    std::optional<std::string> ttl;
    std::optional<DownsamplePolicy> downsample;
};

template <>
struct glz::meta<RetentionPolicyRequest> {
    using T = RetentionPolicyRequest;
    static constexpr auto value = object("measurement", &T::measurement, "ttl", &T::ttl, "downsample", &T::downsample);
};

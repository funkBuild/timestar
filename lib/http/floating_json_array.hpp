#pragma once

#include <glaze/json.hpp>

#include <vector>

namespace timestar::http {

// Only arrays whose first JSON token is floating-point are eligible for the
// double write fast path. Classification must precede conversion: 0.0 is a
// float, whereas 9007199254740993 is an integer that a double cannot preserve.
struct FloatingJsonArray {
    std::vector<double> values;
};

}  // namespace timestar::http

template <>
struct glz::from<glz::JSON, timestar::http::FloatingJsonArray> {
    template <auto Opts>
    static void op(auto& value, auto& ctx, auto& it, auto end) {
        auto token = it;
        auto skipWhitespace = [&] {
            while (token != end && (*token == ' ' || *token == '\n' || *token == '\r' || *token == '\t'))
                ++token;
        };
        skipWhitespace();
        if (token == end || *token++ != '[') {
            ctx.error = glz::error_code::syntax_error;
            return;
        }
        skipWhitespace();
        bool floating = false;
        while (token != end) {
            const char c = *token;
            if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
                ++token;
            } else if (c == '.' || c == 'e' || c == 'E') {
                floating = true;
                ++token;
            } else {
                break;
            }
        }
        if (!floating) {
            ctx.error = glz::error_code::syntax_error;
            return;
        }
        // Glaze validates the complete array, including the token inspected
        // above; the lookahead alone never accepts a malformed number.
        glz::from<glz::JSON, std::vector<double>>::template op<Opts>(value.values, ctx, it, end);
        // The generic reader can report end_reached after a trailing comma;
        // top-level read_json treats that sentinel as success. Require the
        // closing bracket even when this adapter is used as a root value.
        if (it == end && *(it - 1) != ']')
            ctx.error = glz::error_code::expected_bracket;
    }
};

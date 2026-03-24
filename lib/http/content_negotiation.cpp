#include "content_negotiation.hpp"

#include <algorithm>
#include <string_view>

namespace timestar::http {

namespace {

/// Case-insensitive comparison of two ASCII string_views.
bool ciEquals(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i) {
        auto ca = static_cast<unsigned char>(a[i]);
        auto cb = static_cast<unsigned char>(b[i]);
        if (ca != cb && std::tolower(ca) != std::tolower(cb))
            return false;
    }
    return true;
}

/// Strip leading/trailing whitespace from a string_view.
std::string_view trimWhitespace(std::string_view sv) noexcept {
    while (!sv.empty() && sv.front() == ' ')
        sv.remove_prefix(1);
    while (!sv.empty() && sv.back() == ' ')
        sv.remove_suffix(1);
    return sv;
}

}  // anonymous namespace

WireFormat parseMediaType(std::string_view headerValue) {
    // Strip parameters after ';' (e.g., "application/json; charset=utf-8")
    auto semi = headerValue.find(';');
    auto mediaType = trimWhitespace(headerValue.substr(0, semi));

    if (mediaType.empty()) {
        return WireFormat::Json;
    }

    if (ciEquals(mediaType, "application/x-protobuf") || ciEquals(mediaType, "application/protobuf")) {
        return WireFormat::Protobuf;
    }

    return WireFormat::Json;
}

WireFormat requestFormat(const seastar::http::request& req) {
    auto ct = req.get_header("Content-Type");
    return parseMediaType(std::string_view{ct.data(), ct.size()});
}

WireFormat responseFormat(const seastar::http::request& req) {
    auto accept = req.get_header("Accept");
    std::string_view acceptSv{accept.data(), accept.size()};

    if (!acceptSv.empty()) {
        // Accept header can be a comma-separated list; check each entry.
        // NOTE: Uses first-match-wins (ignoring quality values beyond q=0).
        // Full RFC 7231 quality-based selection is not implemented.
        size_t pos = 0;
        while (pos < acceptSv.size()) {
            auto comma = acceptSv.find(',', pos);
            auto entry = trimWhitespace(acceptSv.substr(pos, comma == std::string_view::npos ? comma : comma - pos));

            // Strip quality parameter (e.g., ";q=0.9") before matching
            auto semi = entry.find(';');
            auto mediaType = trimWhitespace(entry.substr(0, semi));

            // Skip entries with explicit zero quality (q=0, q=0.0, q=0.00, q=0.000)
            if (semi != std::string_view::npos) {
                auto params = entry.substr(semi);
                auto qPos = params.find(";q=0");
                if (qPos != std::string_view::npos) {
                    auto afterQ = params.substr(qPos + 4);
                    // Accept "q=0", "q=0.", "q=0.0", "q=0.00", "q=0.000" as zero
                    if (afterQ.empty() || afterQ[0] == ',' || afterQ[0] == ';' || afterQ[0] == ' ' ||
                        (afterQ[0] == '.' && [&]() {
                            if (afterQ.size() == 1)
                                return true;
                            auto nonZero = afterQ.find_first_not_of('0', 1);
                            return nonZero == std::string_view::npos || afterQ[nonZero] == ',' ||
                                   afterQ[nonZero] == ';' || afterQ[nonZero] == ' ';
                        }())) {
                        if (comma == std::string_view::npos)
                            break;
                        pos = comma + 1;
                        continue;
                    }
                }
            }

            if (ciEquals(mediaType, "application/x-protobuf") || ciEquals(mediaType, "application/protobuf")) {
                return WireFormat::Protobuf;
            }

            if (comma == std::string_view::npos)
                break;
            pos = comma + 1;
        }
        // Accept was set but didn't contain protobuf -> Json
        return WireFormat::Json;
    }

    // Accept not set -> echo request format
    return requestFormat(req);
}

void setContentType(seastar::http::reply& rep, WireFormat fmt) {
    if (fmt == WireFormat::Protobuf) {
        rep.set_mime_type("application/x-protobuf");
    } else {
        rep.set_mime_type("application/json");
    }
}

}  // namespace timestar::http

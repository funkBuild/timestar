#include "line_parser.hpp"

#include <stdexcept>

// Inverse of timestar::buildSeriesKey (lib/utils/series_key.hpp).
//
// The canonical key is "measurement,tag=val field" where ',', '=', ' ', '"' and
// '\\' are structural.  buildSeriesKey backslash-escapes those characters inside
// each component, so here a '\\' makes the following character literal (never a
// delimiter), and the extracted components are un-escaped.  This makes
// SeriesKeyParser(buildSeriesKey(m, t, f)) == (m, t, f) for any names — including
// spaces and commas — so WAL replay and delete-by-key reconstruct the exact
// original measurement/tags/field.
//
// A legacy double-quote quoting form ("...") is still honoured for keys that were
// hand-constructed rather than produced by buildSeriesKey: an unescaped '"'
// toggles quoting, and delimiters inside quotes are literal (the quotes are kept
// verbatim in the value).

// Remove one level of backslash-escaping: "\\X" -> "X".  A trailing lone '\\' is
// kept as-is (defensive; buildSeriesKey never emits one).
static std::string unescapeComponent(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            out += s[++i];
        } else {
            out += s[i];
        }
    }
    return out;
}

SeriesKeyParser::SeriesKeyParser(std::string_view seriesKey) {
    if (seriesKey.empty()) {
        throw std::invalid_argument("SeriesKeyParser: empty series key");
    }

    const size_t length = seriesKey.length();

    size_t startIndex = 0;
    State state = State::measurement;
    bool insideQuote = false;

    for (size_t i = 0; i < length; i++) {
        const char c = seriesKey[i];

        // Escape: the next character is literal — skip both so it is never seen
        // as a delimiter or quote toggle.  Un-escaping happens per-component below.
        if (c == '\\') {
            if (i + 1 < length)
                ++i;
            continue;
        }

        if (c == '"') {
            insideQuote = !insideQuote;
            continue;
        }

        if (insideQuote)
            continue;

        switch (c) {
            case ',': {
                switch (state) {
                    case State::measurement:
                        measurement = unescapeComponent(seriesKey.substr(startIndex, i - startIndex));
                        state = State::tags;
                        startIndex = i + 1;
                        break;
                    case State::tags:
                        parseKeypair(seriesKey.substr(startIndex, i - startIndex), tags);
                        startIndex = i + 1;
                        break;
                    case State::field:
                        throw std::invalid_argument("Multiple fields not supported in line protocol");
                }
            } break;
            case ' ': {
                switch (state) {
                    case State::measurement:
                        measurement = unescapeComponent(seriesKey.substr(startIndex, i - startIndex));
                        state = State::field;
                        startIndex = i + 1;
                        break;
                    case State::tags:
                        parseKeypair(seriesKey.substr(startIndex, i - startIndex), tags);
                        state = State::field;
                        startIndex = i + 1;
                        break;
                    case State::field:
                        throw std::runtime_error("Malformed series key: unexpected space in field");
                }
            } break;
        }
    }

    if (insideQuote) {
        throw std::invalid_argument("SeriesKeyParser: unclosed quote in series key");
    }

    field = unescapeComponent(seriesKey.substr(startIndex, length - startIndex));
}

void SeriesKeyParser::parseKeypair(std::string_view keypair, std::map<std::string, std::string>& map) {
    // Split on the first UNESCAPED '=' — subsequent '=' are part of the value.
    size_t delim = std::string_view::npos;
    for (size_t i = 0; i < keypair.size(); ++i) {
        if (keypair[i] == '\\') {
            ++i;  // skip the escaped character
            continue;
        }
        if (keypair[i] == '=') {
            delim = i;
            break;
        }
    }
    if (delim == std::string_view::npos)
        return;  // skip malformed keypairs

    std::string key = unescapeComponent(keypair.substr(0, delim));
    std::string value = unescapeComponent(keypair.substr(delim + 1));

    map.insert({std::move(key), std::move(value)});
}

#include "raft_config.hpp"

#include <charconv>

namespace timestar::raft {

namespace {

void appendList(std::string& out, char key, const std::vector<NodeId>& ids) {
    out.push_back(key);
    out.push_back('=');
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i)
            out.push_back(',');
        out += std::to_string(ids[i]);
    }
    out.push_back(';');
}

std::vector<NodeId> parseList(const std::string& body) {
    std::vector<NodeId> ids;
    size_t i = 0;
    while (i < body.size()) {
        size_t j = body.find(',', i);
        if (j == std::string::npos)
            j = body.size();
        if (j > i) {
            NodeId v = 0;
            std::from_chars(body.data() + i, body.data() + j, v);
            ids.push_back(v);
        }
        i = j + 1;
    }
    return ids;
}

}  // namespace

std::string encodeConfig(const Config& c) {
    std::string out;
    appendList(out, 'v', c.voters);
    appendList(out, 'o', c.votersOutgoing);
    appendList(out, 'l', c.learners);
    return out;
}

Config decodeConfig(const std::string& s) {
    Config c;
    size_t i = 0;
    while (i < s.size()) {
        const char key = s[i];
        // expect key '=' body ';'
        size_t eq = s.find('=', i);
        if (eq == std::string::npos)
            break;
        size_t semi = s.find(';', eq + 1);
        if (semi == std::string::npos)
            semi = s.size();
        const std::string body = s.substr(eq + 1, semi - eq - 1);
        std::vector<NodeId> ids = parseList(body);
        if (key == 'v')
            c.voters = std::move(ids);
        else if (key == 'o')
            c.votersOutgoing = std::move(ids);
        else if (key == 'l')
            c.learners = std::move(ids);
        i = semi + 1;
    }
    return c;
}

}  // namespace timestar::raft

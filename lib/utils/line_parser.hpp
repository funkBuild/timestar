#pragma once

#include <map>
#include <string>

class SeriesKeyParser {
private:
    enum class State { measurement, tags, field };

public:
    std::string measurement;
    std::string field;
    std::map<std::string, std::string> tags;

    SeriesKeyParser(std::string_view seriesKey);
    void parseKeypair(std::string_view keypair, std::map<std::string, std::string>& map);
};

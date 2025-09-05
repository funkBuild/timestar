#ifndef __LINE_PARSER_H_INCLUDED__
#define __LINE_PARSER_H_INCLUDED__

#include <map>
#include <string>

class SeriesKeyParser {
private:
  enum class State { measurement, tags, field };

public:
  std::string measurement;
  std::string field;
  std::map<std::string_view, std::string_view> tags;

  SeriesKeyParser(std::string_view seriesKey);
  void parseKeypair(std::string_view keypair, std::map<std::string_view, std::string_view> &map);
};


#endif

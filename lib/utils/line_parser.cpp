#include "line_parser.hpp"

#include <iostream>

SeriesKeyParser::SeriesKeyParser(std::string_view seriesKey){
  const size_t length = seriesKey.length();

  size_t startIndex = 0;
  State state = State::measurement;
  bool insideQuote = false;

  for(size_t i=0; i < length; i++){
    //std::cout << seriesKey[i] << std::endl;
    switch(seriesKey[i]){
      case ',': {
        if(insideQuote)
          continue;

        switch(state){
          case State::measurement:
            measurement = seriesKey.substr(startIndex, i - startIndex);
            
            state = State::tags;
            startIndex = i + 1;

            break;
          case State::tags: {
            std::string_view keypair = seriesKey.substr(startIndex, i - startIndex);
            parseKeypair(keypair, tags);

            startIndex = i + 1;
            break;
          }
          case State::field: {
            // Throw invalid
            field = seriesKey.substr(startIndex, i - startIndex);

            break;
          }
        }
      }
      break;
    case '"': {
        insideQuote = !insideQuote;
      };
      break;
    case ' ': {
        if(insideQuote)
          continue;

        switch(state){
          case State::measurement: {
            measurement = seriesKey.substr(startIndex, i - startIndex);

            state = State::field;
            break;
          }
          case State::tags: {
            std::string_view keypair = seriesKey.substr(startIndex, i - startIndex);
            parseKeypair(keypair, tags);

            state = State::field;
            break;
          }
          case State::field: {
            // TODO: Should be throw here for invalid seriesKeys?
            field = seriesKey.substr(startIndex, i - startIndex);

            break;
          }
        }

        startIndex = i+1;
      };
      break;
    }

  }

  field = seriesKey.substr(startIndex, length - startIndex);
}

void SeriesKeyParser::parseKeypair(std::string_view keypair, std::map<std::string_view, std::string_view> &map){
  auto delim = keypair.find('=');
  if (delim == std::string_view::npos) return;  // skip malformed keypairs
  std::string_view key = keypair.substr(0, delim);
  std::string_view value = keypair.substr(delim + 1);

  map.insert({key, value});
}

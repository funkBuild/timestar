#pragma once

#include <map>
#include <vector>

template <class T>
class TSDBInsert {
private:
public:
  std::string name;
  std::map<std::string, std::string> tags;

  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  TSDBInsert(std::string _name) : name(_name) {}

  void addTag(std::string key, std::string value){
    tags.insert({key, value});
  }

  void addValue(uint64_t timestamp, T value){
    timestamps.push_back(timestamp);
    values.push_back(value);
  }

  std::string seriesKey(){
    // measurement, tag set, field key
    // h2o_level, location=santa_monica, host=my-host-1 h2o_feet

    return name; //TODO: Return (and memoize?) the series key with tags attached
  }
};

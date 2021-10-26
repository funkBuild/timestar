#ifndef __MEMORY_STORE_H_INCLUDED__
#define __MEMORY_STORE_H_INCLUDED__

#include <unordered_map>
#include <vector>
#include <memory>
#include <iostream>
#include <variant>

#include "tsdb_value.hpp"
#include "wal.hpp"

class WAL;

template <class T>
class InMemorySeries {
public:
  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  void insert(TSDBInsert<T> &insertRequest);
  void sort();
};

using VariantInMemorySeries = std::variant<InMemorySeries<double>, InMemorySeries<bool>>;

class MemoryStore {
private:
  std::unique_ptr<WAL> wal;
  bool closed = false;

public:
  const unsigned int sequenceNumber;
  std::unordered_map<std::string, VariantInMemorySeries> series;

  MemoryStore(unsigned int walSequenceNumber) : sequenceNumber(walSequenceNumber) {
    std::cout << "Memory store " << sequenceNumber << " created" << std::endl;

    wal = std::make_unique<WAL>(this, walSequenceNumber);
  };
  ~MemoryStore() { std::cout << "Memory store " << sequenceNumber << " removed" << std::endl; };

  void close();
  template <class T>
  void insert(TSDBInsert<T> &insertRequest);
  bool isFull() { return true; /* TODO: Return true is size is about certain amount */ }
  bool isClosed() { return closed; }
};

#endif

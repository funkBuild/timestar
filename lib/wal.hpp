#ifndef __WAL_H_INCLUDED__
#define __WAL_H_INCLUDED__

#include "tsdb_value.hpp"
#include "memory_store.hpp"

#include <memory>
#include <fstream>

class MemoryStore;

enum class WALType { Write = 0, Delete, DeleteRange, Close };
enum class WALValueType { Float, Boolean };


class WAL {
private:
  unsigned int sequenceNumber;
  std::unique_ptr<std::ofstream> walFile;

public:
  WAL(MemoryStore *store, unsigned int _sequenceNumber);

  template <class T>
  void insert(TSDBInsert<T> &insertRequest);
  void close();

  static std::string sequenceNumberToFilename(unsigned int sequenceNumber);
  static void remove(unsigned int sequenceNumber);
};

class WALReader {
private:
  std::unique_ptr<std::ifstream> walFile;
  size_t length = 0;

  template <class T>
  TSDBInsert<T> readSeries(std::ifstream *file, std::string &seriesId);
public:
  WALReader(std::string filename);
  void readAll(MemoryStore *store);
};

#endif

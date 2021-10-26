#include "memory_store.hpp"
#include "util.hpp"

#include <iostream>
#include <stdexcept>

template <class T>
void InMemorySeries<T>::insert(TSDBInsert<T> &insertRequest){
  timestamps.insert(timestamps.end(), insertRequest.timestamps.begin(), insertRequest.timestamps.end());
  values.insert(values.end(), insertRequest.values.begin(), insertRequest.values.end());
}

template <class T>
void InMemorySeries<T>::sort(){
  auto p = sort_permutation(timestamps,
    [](uint64_t const& a, uint64_t const& b){ return a < b; });

  timestamps = apply_permutation(timestamps, p);
  values = apply_permutation(values, p);
}

void MemoryStore::close(){
  closed = true;

  if(wal){
    std::cout << "Closing WAL" << std::endl;
    wal->close();
  }
}

template <class T>
void MemoryStore::insert(TSDBInsert<T> &insertRequest){
  std::cout << "Insert into MemoryStore " << sequenceNumber << std::endl;

  if(closed){
    throw std::runtime_error("MemoryStore is closed");
  }

  // WAL Insert
  if(wal)
    wal->insert(insertRequest);

  // In-memory insert
  auto it = series.find(insertRequest.seriesKey());

  if(it == series.end()){
    InMemorySeries<T> newSeries;
    it = series.insert({insertRequest.seriesKey(), std::move(newSeries)}).first;
  }

  std::get<InMemorySeries<T>>(it->second).insert(insertRequest);
}

template void InMemorySeries<double>::insert(TSDBInsert<double> &insertRequest);
template void InMemorySeries<double>::sort();
template void InMemorySeries<bool>::insert(TSDBInsert<bool> &insertRequest);
template void InMemorySeries<bool>::sort();

template void MemoryStore::insert<double>(TSDBInsert<double> &insertRequest);
template void MemoryStore::insert<bool>(TSDBInsert<bool> &insertRequest);

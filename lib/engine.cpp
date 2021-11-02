#include "engine.hpp"
#include "util.hpp"
#include "tsm_writer.hpp"
#include "query_runner.hpp"

#include <iostream>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

Engine::Engine(){};


seastar::future<> Engine::init(){
  co_await fileManager.init();

  // Search for existing WAL's
  std::string path = "./"; //TODO: Get WAL path from config
  std::vector<std::string> walFiles;

  for (const auto &entry : fs::directory_iterator(path))
  {
    if (endsWith(entry.path(), ".wal"))
      walFiles.push_back(entry.path());
  }

  // Convert them to TSM's if they exist and are closed
  for (const auto &walFilename : walFiles)
  {
    size_t filenameEndIndex = walFilename.find_last_of(".");
    size_t filenameStartIndex = walFilename.find_last_of("/") + 1;

    int seqNum = std::stoi(walFilename.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex));

    if (seqNum > currentWalSequenceNumber)
      currentWalSequenceNumber = seqNum;

    memoryStore = std::make_shared<MemoryStore>(seqNum);

    if (memoryStore->isClosed())
    {
      std::cout << "Write MemoryStore to TSM: " << seqNum << std::endl;

      // Write to TSM
      fileManager.writeMemstore(memoryStore);
      memoryStore.reset();
    }
    else
    {
      // TODO: Freak out if we have two open WAL's
      std::cout << "Got open MemoryStore: " << seqNum << std::endl;
      break;
    }
  }

  if (!memoryStore)
    memoryStore = std::make_shared<MemoryStore>(++currentWalSequenceNumber);

  std::cout << "Boot done" << std::endl
            << std::endl;
};

template <class T>
void Engine::insert(TSDBInsert<T> insertRequest)
{
  memoryStore->insert(insertRequest);

  if (memoryStore->isFull())
  {
    rolloverMemoryStore();
  }
}

void Engine::rolloverMemoryStore()
{
  std::cout << "Memory store full" << std::endl;

  memoryStore->close();
  pendingMemoryStores.push_back(memoryStore);

  memoryStore = std::make_shared<MemoryStore>(++currentWalSequenceNumber);
}


seastar::future<VariantQueryResult> Engine::query(std::string series, uint64_t startTime, uint64_t endTime){
  // TODO: Series id lookup
  // TODO: Return and enforce the type of each series
  
  // assert startTime < endTime
  
  QueryRunner runner(&fileManager);
  auto result = co_await runner.runQuery(series, startTime, endTime);

  co_return result;
}

template void Engine::insert<bool>(TSDBInsert<bool> insertRequest);
template void Engine::insert<double>(TSDBInsert<double> insertRequest);
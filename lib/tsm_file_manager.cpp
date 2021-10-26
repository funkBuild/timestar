#include "tsm_file_manager.hpp"
#include "tsm_writer.hpp"
#include "util.hpp"

#include <thread>
#include <filesystem>

namespace fs = std::filesystem;

TSMFileManager::TSMFileManager(){
  init();
}

void TSMFileManager::init(){
  std::cout << "TSMFileManager init" << std::endl;
  // TODO: Scan the TSM folder for files

  std::string path = "./"; //TODO: Get TSM path from config

  for (const auto &entry : fs::directory_iterator(path))
  {
    if (endsWith(entry.path(), ".tsm")){
      std::string absolutePath = fs::canonical(fs::absolute(entry.path()));
      openTsmFile(absolutePath);
    }
  }

  // TODO: Load each file into a TSM object and store
  // TODO: Keep track of levels and sequence id's
}

void TSMFileManager::openTsmFile(std::string path){
  std::cout << "Open TSM " << path << std::endl;

  try {
    std::shared_ptr<TSM> tsmFile = std::make_shared<TSM>(path);
    tsmFiles.push_back(tsmFile);

    // TODO: Handle conflicting sequence number, or fail loudly
    sequencedTsmFiles.insert({ tsmFile.get()->seqNum, tsmFile });
  } catch(const std::runtime_error&) {
    return;
  }
}

void TSMFileManager::queueMemstoreCompaction(std::shared_ptr<MemoryStore> memStore){
  std::thread t([&] {
    writeMemstore(memStore);
  });

  t.detach();
}

void TSMFileManager::writeMemstore(std::shared_ptr<MemoryStore> memStore){
  auto seqNum = sequenceId++;
  std::string filename = "0_" + std::to_string(seqNum) + ".tsm";
  TSMWriter::run(memStore, filename);
  WAL::remove(memStore.get()->sequenceNumber);
}
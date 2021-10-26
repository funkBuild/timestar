#include "wal.hpp"
#include "aligned_buffer.hpp"
#include "tsm.hpp"

#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

// TODO: WAL directory should be set in config
WAL::WAL(MemoryStore *store, unsigned int _sequenceNumber) {
  sequenceNumber = _sequenceNumber;

  std::string filename = sequenceNumberToFilename(sequenceNumber);

  if(fs::exists(filename)){
    std::cout << "Wal file exists: " << filename << std::endl;

    WALReader reader(filename);
    reader.readAll(store);
  }

  if(!store->isClosed())
    walFile = std::make_unique<std::ofstream>(filename, std::ios::binary | std::ios::app);
};

std::string WAL::sequenceNumberToFilename(unsigned int sequenceNumber){
  std::string sequenceNumStr = std::to_string(sequenceNumber);
  std::string filename = std::string(10 - sequenceNumStr.length(), '0').append(sequenceNumStr).append(".wal");

  return std::move(filename);  
}

void WAL::close(){
  AlignedBuffer buffer;
  buffer.write((uint8_t)WALType::Close);
  *walFile << buffer;

  walFile->flush();
  walFile->close();
}

template <class T>
void WAL::insert(TSDBInsert<T> &insertRequest){
  AlignedBuffer buffer;

  buffer.write((uint8_t)WALType::Write);

  std::string seriesId = insertRequest.seriesKey();
  uint16_t seriesIdLength = seriesId.length();
  buffer.write(seriesIdLength);
  buffer.write(seriesId);

  // Value type
  buffer.write((uint8_t) TSM::getValueType<T>());

  const uint32_t entries = insertRequest.values.size();
  buffer.write(entries);

  for(unsigned int i=0; i < entries; i++){
    buffer.write(insertRequest.timestamps[i]);
    // Note: Bools are written as a single byte
    buffer.write(insertRequest.values[i]); 
  }

  *walFile << buffer;
};

void WAL::remove(unsigned int sequenceNumber){
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);

  try {
    if (std::filesystem::remove(filename))
       std::cout << "file " << filename << " deleted.\n";
    else
       std::cout << "file " << filename << " not found.\n";
  }
  catch(const std::filesystem::filesystem_error& err) {
     std::cout << "filesystem error: " << err.what() << '\n';
  }
}

// TODO: WAL directory should be set in config
WALReader::WALReader(std::string filename) {
  walFile = std::make_unique<std::ifstream>(filename, std::ios::binary);

  if(walFile->fail()){
    std::cout << "Failed to open " << filename << std::endl;
    // TODO: Throw exception
  }

  walFile->seekg(0, walFile->end);
  length = walFile->tellg();
  walFile->seekg(0, walFile->beg);
};

void WALReader::readAll(MemoryStore *store){
  std::ifstream *file = walFile.get();
  
  std::cout << "length= " << (unsigned int)length << std::endl;
  std::cout << "WAL pos= " << (unsigned int)file->tellg() << std::endl;

  while(file->tellg() < length){
    uint8_t type;
    file->read((char*)&type, 1);

    std::cout << "Got type " << (unsigned int)type << std::endl;

    switch(static_cast<WALType>(type)){
      case WALType::Write: {
        uint16_t seriesIdLength;
        file->read((char*)&seriesIdLength, 2);

        std::string seriesId(seriesIdLength, ' ');
        file->read(&seriesId[0], seriesIdLength);

        std::cout << "Got series " << seriesId << std::endl;

        uint8_t valueType = 0;
        file->read((char*)&valueType, 1);


        switch(static_cast<WALValueType>(valueType)){
          case WALValueType::Float: {
            TSDBInsert<double> insertReq = readSeries<double>(file, seriesId);
            store->insert(insertReq);
          }
          break;
          case WALValueType::Boolean: {
            TSDBInsert<bool> insertReq = readSeries<bool>(file, seriesId);
            store->insert(insertReq);
          }
          break;
        }

      }
      break;
      case WALType::Close: {
        std::cout << "Got WAL close" << std::endl;
        store->close();
      }
    }

    std::cout << "WAL pos= " << (unsigned int)file->tellg() << std::endl;

  }
}

template <class T>
TSDBInsert<T> WALReader::readSeries(std::ifstream *file, std::string &seriesId){
  // TODO: Correctly parse the seriesId back to measurement, tags, field

  TSDBInsert<T> insertReq(seriesId);

  uint32_t entries;
  file->read((char*)&entries, 4);

  std::cout << " >> Found " << entries << " entries" << std::endl;

  while(entries-- > 0){
    uint64_t timestamp;
    file->read((char*)&timestamp, sizeof(uint64_t));

    T value;
    size_t valueBytes = std::is_same<T, bool>::value ? 1 : sizeof(T);

    file->read((char*)&value, valueBytes); // TODO: Don't read a single value at a time, optimize to read the whole block

    insertReq.addValue(timestamp, value);
  }

  return std::move(insertReq);
}

template void WAL::insert<double>(TSDBInsert<double> &insertRequest);
template void WAL::insert<bool>(TSDBInsert<bool> &insertRequest);
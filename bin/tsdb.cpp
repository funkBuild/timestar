#include <iostream>
#include <vector>
#include <cstdint>
#include <chrono>


#include "integer_encoder.hpp"
#include "compressed_buffer.hpp"
#include "simple8b.hpp"
#include "zigzag.hpp"
#include "float_encoder.hpp"
#include "tsxor_encoder.hpp"
#include "tsm.hpp"

#include "engine.hpp"

#include <cmath>
#include <limits>
#include <random>
#include <variant>
#include <unistd.h>


typedef std::chrono::high_resolution_clock Clock;

bool AreSame(double a, double b) {
    return std::fabs(a - b) < std::numeric_limits<double>::epsilon();
}

float get_random()
{
    static std::default_random_engine e;
    static std::uniform_real_distribution<> dis(-1, 1); // rage 0 - 1
    return dis(e);
}


int main_bench(){
  //std::vector<int64_t> v = { 7, 5, 16, 8, 4, 6, 10, -15, 20, 40,7,61,39,80,23,4,99,16,19,63,79,31,1,89,90,1,64,42,51,28,86,33,72,52,35,15,47,19,79,13,63,25,59,5,3,68 };

  // auto encoded = IntegerEncoder::encode(v);
  // IntegerEncoder::decode(encoded);

  //CompressedBuffer buffer;
  //buffer.write(0xff, 8);
  //buffer.write(0x1234567890abcdef, 64);

/*
  std::vector<uint64_t> zzV = ZigZag::zigzagEncodeVector(v);

  std::vector<uint64_t> encoded = Simple8B::encode(zzV);

  std::cout << "Encoded size: " << encoded.size();
  std::cout << " Original size: " << v.size();
  std::cout << " ratio: " << (float)encoded.size() / v.size() << std::endl;
*/

  //std::vector<int64_t> decV = ZigZag::zigzagDecodeVector(zzV);
  const unsigned int test_length = 10;

  std::vector<double> v;
  double tmp = 10;
  for(unsigned int i=0; i < test_length; i++){
    tmp += 0.01; //get_random();
    v.push_back(tmp);
  }

  auto start_time = Clock::now();

  auto buf = TsxorEncoder::encode(v);

  auto end_time = Clock::now();
  uint64_t time_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  double iterations_per_sec = 1e9/time_diff;
  double bytes_per_sec = v.size() * sizeof(double) * iterations_per_sec;
  double mb_per_sec = bytes_per_sec / (1024*1024);

  std::cout << "Time difference: " << time_diff << " nanoseconds" << std::endl;
  std::cout << "iterations_per_sec: " << iterations_per_sec << " " << std::endl;
  std::cout << "bytes_per_sec: " << bytes_per_sec << " " << std::endl;
  std::cout << "mb_per_sec: " << mb_per_sec << " " << std::endl;
  std::cout << "compression ratio: " << ((float)v.size() / buf.size()) << " " << std::endl;

/*

  buf.rewind();

  {
    auto start_time = Clock::now();

    auto decoded = FloatEncoder::decode(buf, v.size());

    auto end_time = Clock::now();
    uint64_t time_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    double iterations_per_sec = 1e9/time_diff;
    double bytes_per_sec = v.size() * sizeof(double) * iterations_per_sec;
    double mb_per_sec = bytes_per_sec / (1024*1024);

    std::cout << "Time difference: " << time_diff << " nanoseconds" << std::endl;
    std::cout << "iterations_per_sec: " << iterations_per_sec << " " << std::endl;
    std::cout << "bytes_per_sec: " << bytes_per_sec << " " << std::endl;
    std::cout << "mb_per_sec: " << mb_per_sec << " " << std::endl;
    std::cout << "size: " << decoded.size() << " " << std::endl;


    for(unsigned int i=0; i < std::min(decoded.size(), v.size()); i++){
      if(!AreSame(v[i], decoded[i])){
        std::cout << "Invalid decode at " << i << " value=" << decoded[i] << " expected=" << v[i] << std::endl;
        break;
      }
    }
  }
*/
  return 0;
};
/*
int main(){
  Engine engine;
*/

/*
{
  TSDBInsert<double> insertVal("booltest");
  insertVal.addTag("tag", "tagValue");

  for(int i=0; i < 1000000; i++)
    insertVal.addValue(1000 * i, true);

  engine.insert(insertVal);
}
*/


#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>



int main(int argc, char** argv) {
  seastar::app_template app;
  app.run(argc, argv, [] () -> seastar::future<> {
    seastar::sharded<Engine> engineService;

    co_await engineService.start();


    co_await engineService.invoke_on(0, [] (Engine& engine) -> seastar::future<>{
        co_await engine.init();
    });




    co_await engineService.invoke_on(0, [] (Engine& engine) -> seastar::future<>{
      auto start_time = Clock::now();

      auto result = co_await engine.query("floattest", 500000, 52500000);

      auto end_time = Clock::now();

      uint64_t time_diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
      std::cout << "Time difference: " << time_diff/1000.0 << " milliseconds" << std::endl;

      std::visit([](auto&& arg) {
        std::cout << "results.timestamps.size()=" << arg.timestamps->size() << std::endl;
        std::cout << "results.values.size()=" << arg.values->size() << std::endl;
      }, result);
    });


  /*
    {
      TSDBInsert<double> insertVal("floattest");
      insertVal.addTag("tag", "tagValue");

      for(int i=0; i < 1000000; i++)
        insertVal.addValue(1000 * i, i*0.5);

      engine.insert(insertVal);
    }
  */


/*
    constexpr size_t aligned_size = 4096;
    auto wbuf = seastar::temporary_buffer<char>::aligned(aligned_size, aligned_size);

    auto file = co_await seastar::open_file_dma("test.file", seastar::open_flags::rw | seastar::open_flags::create);
    //co_await file.dma_write(0, wbuf.get(), aligned_size);

    auto stream = co_await seastar::make_file_output_stream(file, aligned_size);

    co_await stream.write(wbuf.get(), 512);

    co_await stream.flush();
    co_await stream.close();

    auto alignment = file.disk_write_dma_alignment();
    std::cout << "alignment=" << alignment << std::endl;
*/
    co_await engineService.stop();

  });
}
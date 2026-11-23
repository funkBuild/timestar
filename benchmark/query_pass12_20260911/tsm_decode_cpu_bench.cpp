#include "tsm.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder_rle.hpp"
#include "string_encoder.hpp"
#include "zigzag.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>

template<class T> void run(const char* type) {
    TSM tsm("0_999.tsm");
    for (size_t n : {3000, 65536}) {
        std::vector<uint64_t> timestamps(n);
        std::vector<T> values(n);
        for (size_t i = 0; i < n; ++i) {
            timestamps[i] = 1700000000000000000ULL + i * 1000;
            if constexpr (std::is_same_v<T, std::string>) values[i] = std::to_string(i);
            else values[i] = static_cast<T>(i % 127);
        }
        auto times = IntegerEncoder::encode(timestamps);
        AlignedBuffer block;
        block.write(static_cast<uint8_t>(TSM::getValueType<T>()));
        block.write(static_cast<uint32_t>(n));
        block.write(static_cast<uint32_t>(times.size()));
        block.write(times);
        if constexpr (std::is_same_v<T, double>) FloatEncoder::encodeInto(values, block);
        else if constexpr (std::is_same_v<T, bool>) BoolEncoderRLE::encodeInto(values, block);
        else if constexpr (std::is_same_v<T, std::string>) StringEncoder::encodeInto(values, block);
        else {
            std::vector<uint64_t> zigzag(n);
            for (size_t i = 0; i < n; ++i) zigzag[i] = ZigZag::zigzagEncode(values[i]);
            IntegerEncoder::encodeInto(zigzag, block);
        }
        for (std::string filter : {"full", "middle", "late", "none"}) {
            size_t first = filter == "middle" ? n/3 : filter == "late" ? n-128 : filter == "none" ? n : 0;
            size_t last = filter == "middle" ? 2*n/3+1 : n;
            uint64_t lo = first == n ? timestamps.back()+1 : filter == "full" ? 0 : timestamps[first];
            uint64_t hi = filter == "middle" ? timestamps[last-1] : UINT64_MAX;
            std::vector<double> samples;
            size_t tsBytes=0, valueBytes=0;
            const size_t loops=std::max(size_t(8),size_t(262144)/n);
            for(int rep=0;rep<7;++rep) {
                std::unique_ptr<TSMBlock<T>> result;
                auto start=std::chrono::steady_clock::now();
                for(size_t j=0;j<loops;++j) {
                    result.reset();
                    Slice slice(block.data.data(),block.size());
                    result=tsm.decodeBlock<T>(slice,block.size(),lo,hi);
                    if(!result || result->size()!=last-first)throw std::runtime_error("size mismatch");
                }
                double ns=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/loops;
                for(size_t i=0;i<result->size();++i)
                    if(result->timestamps[i]!=timestamps[first+i] || result->values[i]!=values[first+i])
                        throw std::runtime_error("value mismatch");
                tsBytes=result->timestamps.capacity()*8;
                if constexpr(std::is_same_v<T,bool>) valueBytes=(result->values.capacity()+7)/8;
                else valueBytes=result->values.capacity()*sizeof(T);
                if(rep)samples.push_back(ns);
            }
            std::sort(samples.begin(),samples.end());
            std::printf("type=%s n=%zu filter=%s median_ns=%.3f min_ns=%.3f max_ns=%.3f timestamp_capacity_bytes=%zu value_capacity_bytes=%zu\n",type,n,filter.c_str(),(samples[2]+samples[3])/2,samples.front(),samples.back(),tsBytes,valueBytes);
        }
    }
}
int main() { run<double>("float");run<int64_t>("integer");run<bool>("boolean");run<std::string>("string"); }

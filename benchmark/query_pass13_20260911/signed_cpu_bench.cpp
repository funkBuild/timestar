#include "integer_encoder.hpp"
#include "zigzag.hpp"
#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <string>

template<class T> void run(const char* type) {
    std::mt19937_64 rng(42);
    for(size_t n:{3000,65536})for(std::string shape:{"constant","ramp","cycle","random"}) {
        std::vector<int64_t> input(n);
        for(size_t i=0;i<n;++i)input[i]=shape=="constant"?-7:shape=="ramp"?static_cast<int64_t>(i):shape=="cycle"?static_cast<int64_t>(i%127)-63:std::bit_cast<int64_t>(rng());
        auto encoded=IntegerEncoder::encode(ZigZag::zigzagEncodeVector(input));
        for(std::string filter:{"full","middle","late","none"}) {
            size_t skip=filter=="middle"?n/3:filter=="late"?n-128:filter=="none"?n:0;
            size_t length=filter=="middle"?n/3+1:n-skip;
            std::vector<double> samples;
            size_t scratchCapacity=0,outputCapacity=0;
            size_t loops=std::max(size_t{4},size_t{262144}/n);
            for(int rep=0;rep<7;++rep) {
                std::vector<T> output;
                auto start=std::chrono::steady_clock::now();
                for(size_t j=0;j<loops;++j) {
                    std::vector<T>().swap(output);
                    // Both TSM callers already reserve the surviving value count.
                    output.reserve(length);
                    Slice slice(encoded.data.data(),encoded.size());
#ifdef BENCH_DIRECT_SIGNED
                    size_t count=IntegerEncoder::decodeSigned(slice,n,skip,length,output);
#else
                    std::vector<uint64_t> scratch;
                    IntegerEncoder::decode(slice,n,scratch);
                    scratchCapacity=scratch.capacity()*8;
                    for(size_t k=skip;k<skip+length;++k)output.push_back(static_cast<T>(ZigZag::zigzagDecode(scratch[k])));
                    size_t count=output.size();
#endif
                    if(count!=length || slice.offset!=encoded.size())throw std::runtime_error("count or cursor mismatch");
                }
                double ns=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/loops;
                for(size_t i=0;i<length;++i)if(output[i]!=static_cast<T>(input[skip+i]))throw std::runtime_error("value mismatch");
                outputCapacity=output.capacity()*sizeof(T);
                if(rep)samples.push_back(ns);
            }
            std::sort(samples.begin(),samples.end());
            std::printf("type=%s n=%zu shape=%s filter=%s median_ns=%.3f min_ns=%.3f max_ns=%.3f output_capacity_bytes=%zu scratch_capacity_bytes=%zu\n",type,n,shape.c_str(),filter.c_str(),(samples[2]+samples[3])/2,samples.front(),samples.back(),outputCapacity,scratchCapacity);
        }
    }
}
int main(){run<int64_t>("integer");run<double>("double");}

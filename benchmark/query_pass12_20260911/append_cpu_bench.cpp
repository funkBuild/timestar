#include "integer_encoder_ffor.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>
int main() {
    for (size_t points : {128, 3000}) {
        std::vector<uint64_t> input(points);
        for(size_t i=0;i<points;++i)input[i]=1700000000000000000ULL+i*1000;
        auto encoded=IntegerEncoderFFOR::encode(input);
        for(size_t blocks:{1,4,16,64,256}) {
            std::vector<double> times;
            size_t capacity=0,growths=0;
            for(int rep=0;rep<7;++rep) {
                std::vector<uint64_t> output;
                size_t lastCapacity=0;growths=0;
                const auto start=std::chrono::steady_clock::now();
                for(size_t j=0;j<blocks;++j) {
                    Slice slice(encoded.data.data(),encoded.size());
                    auto [skipped,added]=IntegerEncoderFFOR::decode(slice,points,output);
                    if(skipped || added!=points)throw std::runtime_error("count mismatch");
                    if(output.capacity()!=lastCapacity)++growths;
                    lastCapacity=output.capacity();
                }
                double ns=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count();
                if(output.size()!=blocks*points)throw std::runtime_error("size mismatch");
                for(size_t i=0;i<output.size();++i)if(output[i]!=input[i%points])throw std::runtime_error("value mismatch");
                capacity=output.capacity()*8;
                if(rep)times.push_back(ns);
            }
            std::sort(times.begin(),times.end());
            std::printf("points=%zu blocks=%zu median_ns=%.3f min_ns=%.3f max_ns=%.3f output_capacity_bytes=%zu growths=%zu\n",points,blocks,(times[2]+times[3])/2,times.front(),times.back(),capacity,growths);
        }
    }
}

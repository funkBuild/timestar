#include "integer_encoder_ffor.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
int main() {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    for (size_t n : {1024, 3000, 65537, 1048576}) {
        std::vector<uint64_t> input(n);
        for (size_t i = 0; i < n; ++i) input[i] = 1700000000000000000ULL + i * 1000000;
        auto encoded = IntegerEncoderFFOR::encode(input);
        for (std::string filter : {"full", "middle", "late", "none"}) {
            const uint64_t lo = filter == "middle" ? input[n/3] : filter == "late" ? input[n-129] : filter == "none" ? input.back()+1 : 0;
            const uint64_t hi = filter == "middle" ? input[2*n/3] : filter == "late" ? input[n-2] : UINT64_MAX;
            auto first=std::lower_bound(input.begin(),input.end(),lo),last=std::upper_bound(first,input.end(),hi);
            std::vector<uint64_t> expected(first,last);
            for (bool fresh : {false,true}) {
                std::vector<uint64_t> output;
                if (!fresh) output.reserve(n);
                size_t capacity=0;
                std::vector<double> times;
                const size_t loops=std::max(size_t(16),size_t(2097152)/n);
                for (int rep=0;rep<7;++rep) {
                    const auto start=std::chrono::steady_clock::now();
                    for(size_t j=0;j<loops;++j) {
                        if (fresh) std::vector<uint64_t>().swap(output);
                        else output.clear();
                        Slice slice(encoded.data.data(),encoded.size());
                        auto [skipped,added]=IntegerEncoderFFOR::decode(slice,n,output,lo,hi);
                        if(skipped!=size_t(first-input.begin()) || added!=expected.size())throw std::runtime_error("count mismatch");
                    }
                    double ns=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/loops;
                    if(output!=expected)throw std::runtime_error("values mismatch");
                    capacity=std::max(capacity,output.capacity()*sizeof(uint64_t));
                    if(rep)times.push_back(ns);
                }
                std::sort(times.begin(),times.end());
                std::printf("regular n=%zu filter=%s fresh=%d median_ns=%.3f min_ns=%.3f max_ns=%.3f output_capacity_bytes=%zu\n",n,filter.c_str(),fresh,(times[2]+times[3])/2,times.front(),times.back(),capacity);
            }
        }
    }
}

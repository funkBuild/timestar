#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

template <size_t Width>
void batched(const uint64_t* data, size_t n, const uint64_t* keys, size_t count, size_t* out) {
    size_t i = 0;
    for (; i + Width <= count; i += Width) {
        std::array<size_t, Width> base{};
        size_t len = n;
        while (len > 1) {
            const auto half = len / 2;
            for (size_t k = 0; k < Width; ++k)
                base[k] = data[base[k] + half] < keys[i + k] ? base[k] + half : base[k];
            len -= half;
        }
        for (size_t k = 0; k < Width; ++k)
            out[i+k] = base[k] + (data[base[k]] < keys[i+k]);
    }
    for (; i < count; ++i) out[i] = std::lower_bound(data, data+n, keys[i])-data;
}
void standard(const uint64_t* data, size_t n, const uint64_t* keys, size_t count, size_t* out) {
    for (size_t i = 0; i < count; ++i) out[i] = std::lower_bound(data, data+n, keys[i])-data;
}
int main() {
    std::mt19937_64 rng(42);
    using Fn=void(*)(const uint64_t*,size_t,const uint64_t*,size_t,size_t*);
    for (size_t n : {4096, 65536, 1048576}) {
        std::vector<uint64_t> data(n), keys(8192);
        for (size_t i=0;i<n;++i)data[i]=i*2;
        for(const char* shape: {"random", "sorted", "duplicates"}) {
            for(size_t i=0;i<keys.size();++i) keys[i]=(rng()%(n+2))*2 + (i%5==0);
            if(shape[0]=='s')std::sort(keys.begin(),keys.end());
            if(shape[0]=='d')for(size_t i=1;i<keys.size();i+=2)keys[i]=keys[i-1];
            std::vector<size_t> reference(keys.size()),out(keys.size());
            standard(data.data(),n,keys.data(),keys.size(),reference.data());
            for(auto [name,fn]: std::array<std::pair<const char*,Fn>,4>{{{"std",standard},{"one",batched<1>},{"four",batched<4>},{"eight",batched<8>}}}) {
                std::vector<double> times;
                for(int rep=0;rep<7;++rep) {
                    auto start=std::chrono::steady_clock::now();
                    for(int i=0;i<10;++i) fn(data.data(),n,keys.data(),keys.size(),out.data());
                    double ns=std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/(10*keys.size());
                    if(out!=reference)return 1;
                    if(rep)times.push_back(ns);
                }
                std::sort(times.begin(),times.end());
                std::printf("n=%zu shape=%s method=%s ns=%.3f\n",n,shape,name,(times[2]+times[3])/2);
            }
        }
    }
}

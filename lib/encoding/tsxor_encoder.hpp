#ifndef __TSXOR_ENCODER_H_INCLUDED__
#define __TSXOR_ENCODER_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <deque>
#include <algorithm>

#include "compressed_buffer.hpp"

#define WINDOW_SIZE 127

class Window
{
private:
    std::deque<uint64_t> buffer;
    std::vector<uint64_t> tmp;

public:
    Window(size_t dim = WINDOW_SIZE)
    {
        buffer = std::deque<uint64_t>(dim, 0);
        tmp = std::vector<uint64_t>(dim, 0);
    }

    inline void insert(uint64_t val)
    {
        //Remove tail
        buffer.pop_back();

        //Add the fresh value
        buffer.push_front(val);
    }

    inline bool contains(uint64_t val)
    {
        return std::find(buffer.begin(), buffer.end(), val) != buffer.end();
    }

    inline int getIndexOf(uint64_t val)
    {
        return std::distance(buffer.begin(), std::find(buffer.begin(), buffer.end(), val));
    }

    inline uint64_t get(int offset)
    {
        return buffer[offset];
    }

    inline uint64_t getLast()
    {
        return buffer.front();
    }

    inline uint64_t getCandidate(uint64_t val)
    {
        
        for (int i = 0; i < WINDOW_SIZE; i++)
        {
            tmp[i] = val ^ buffer[i];
        }

        std::transform(tmp.begin(),
                       tmp.end(),
                       tmp.begin(),
                       [&](uint64_t x) {
                           if (x != 0)
                           {
                               return __builtin_clzll(x) + __builtin_ctzll(x);
                           }
                           else
                           {
                               return 64;
                           }
                       });

        auto maxid = std::distance(tmp.begin(), std::max_element(tmp.begin(), tmp.end()));
        return buffer[maxid];
    }
};

class TsxorEncoder {
private:


public:
  TsxorEncoder(){};

  static CompressedBuffer encode(std::vector<double> &values);
  // static std::vector<double> decode(CompressedBuffer &encoded, unsigned int length);
};

#endif

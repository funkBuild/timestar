#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/encoding/simple8b.hpp"
#include "../../../lib/core/placement_table.hpp"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

float get_random() {
    static std::default_random_engine e;
    static std::uniform_real_distribution<> dis(0, 1);  // range 0 - 1
    return dis(e);
}

TEST(FloatTest, BasicAssertions) {
    const unsigned int test_length = 1000000;

    std::vector<double> v;
    double tmp = 0;

    for (unsigned int i = 0; i < test_length; i++) {
        tmp += get_random();
        v.push_back(tmp);
    }

    auto buf = FloatEncoder::encode(v);
    buf.rewind();
    CompressedSlice slice((const uint8_t*)buf.data.data(), buf.size());
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, v.size(), decoded);

    EXPECT_EQ(decoded.size(), v.size());

    for (unsigned int i = 0; i < std::min(decoded.size(), v.size()); i++) {
        // std::cout << i << " -- " << v[i] << " :: " << decoded[i] << std::endl;
        EXPECT_DOUBLE_EQ(v[i], decoded[i]);
    }
}

TEST(Simple8Test, BasicAssertions) {
    const unsigned int test_length = 1000000;

    std::vector<uint64_t> v;
    uint64_t tmp = 0;

    for (unsigned int i = 0; i < test_length; i++) {
        tmp += 10;
        v.push_back(tmp);
    }

    auto buf = Simple8B::encode(v);
    Slice slice(buf.data.data(), buf.size());
    auto decoded = Simple8B::decode(slice, v.size());

    EXPECT_EQ(decoded.size(), v.size());

    for (unsigned int i = 0; i < std::min(decoded.size(), v.size()); i++) {
        // std::cout << i << " -- " << v[i] << " :: " << decoded[i] << std::endl;
        EXPECT_EQ(v[i], decoded[i]);
    }
}

TEST(IntegerTest, BasicAssertions) {
    const unsigned int test_length = 1000000;

    std::vector<uint64_t> v;
    uint64_t tmp = 0;

    for (unsigned int i = 0; i < test_length; i++) {
        tmp += 10000 * get_random();
        v.push_back(tmp);
    }

    auto buf = IntegerEncoder::encode(v);
    Slice slice(buf.data.data(), buf.size());
    std::vector<uint64_t> decoded;
    IntegerEncoder::decode(slice, v.size(), decoded);

    EXPECT_EQ(decoded.size(), v.size());

    for (unsigned int i = 0; i < std::min(decoded.size(), v.size()); i++) {
        EXPECT_EQ(v[i], decoded[i]);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    seastar::app_template app;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(seastar::smp::count));
            return ::RUN_ALL_TESTS();
        });
    });
}

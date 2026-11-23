// BucketMap replaces std::unordered_map<uint64_t, State> on the bucketed
// aggregation path. Wherever the two are used the same way they must behave
// the same way, so the reference here is std::unordered_map itself, driven by
// the same operations — in the access patterns aggregation produces (in-order
// appends, in-order lookups from a second series) and in ones it does not.

#include "../../../lib/query/bucket_map.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

using timestar::BucketMap;

namespace {

struct Tracked {
    static inline int64_t alive = 0;
    int value = 0;
    std::vector<int> heap;  // non-trivial, like a state holding raw values
    Tracked() { ++alive; }
    explicit Tracked(int v) : value(v), heap(static_cast<size_t>(v % 5), v) { ++alive; }
    Tracked(const Tracked& o) : value(o.value), heap(o.heap) { ++alive; }
    Tracked(Tracked&& o) noexcept : value(o.value), heap(std::move(o.heap)) { ++alive; }
    Tracked& operator=(const Tracked&) = default;
    Tracked& operator=(Tracked&&) noexcept = default;
    ~Tracked() { --alive; }
};

template <class V, class Ref>
void expectSameContents(const BucketMap<V>& map, const Ref& ref) {
    ASSERT_EQ(map.size(), ref.size());
    ASSERT_EQ(map.empty(), ref.empty());
    size_t seen = 0;
    for (const auto& [key, value] : map) {
        auto it = ref.find(key);
        ASSERT_NE(it, ref.end()) << "unexpected key " << key;
        ASSERT_EQ(value.value, it->second.value) << "key " << key;
        ++seen;
    }
    ASSERT_EQ(seen, ref.size());
    for (const auto& [key, value] : ref) {
        auto it = map.find(key);
        ASSERT_NE(it, map.end()) << "missing key " << key;
        ASSERT_EQ(it->second.value, value.value);
        ASSERT_EQ(map.count(key), 1u);
        ASSERT_EQ(map.at(key).value, value.value);
    }
}

}  // namespace

TEST(BucketMapTest, InOrderAppendsStaySortedAndNeedNoIndex) {
    BucketMap<Tracked> map;
    EXPECT_TRUE(map.sortedByKey());
    for (int i = 0; i < 20000; ++i) {
        map[static_cast<uint64_t>(i) * 60].value = i;
    }
    EXPECT_TRUE(map.sortedByKey());
    ASSERT_EQ(map.size(), 20000u);
    uint64_t expectedKey = 0;
    int expectedValue = 0;
    for (const auto& [key, v] : map) {
        ASSERT_EQ(key, expectedKey);
        ASSERT_EQ(v.value, expectedValue);
        expectedKey += 60;
        ++expectedValue;
    }
    // Revisiting the latest bucket (a run of points in one bucket) hits it.
    map[19999 * 60].value += 1;
    EXPECT_EQ(map.size(), 20000u);
    EXPECT_EQ(map.at(19999 * 60).value, 20000);
}

TEST(BucketMapTest, FoldingASecondSeriesInOrderFindsEveryBucket) {
    BucketMap<Tracked> group;
    for (int i = 0; i < 5000; ++i) {
        group[static_cast<uint64_t>(i) * 10].value = 1;
    }
    // Second series: same buckets, a gap, then new ones past the end.
    for (int i = 0; i < 6000; ++i) {
        if (i % 7 == 3) {
            continue;
        }
        group[static_cast<uint64_t>(i) * 10].value += 10;
    }
    ASSERT_EQ(group.size(), 6000u - 143u /* i>=5000 with i%7==3 never created */);
    for (int i = 0; i < 5000; ++i) {
        ASSERT_EQ(group.at(static_cast<uint64_t>(i) * 10).value, (i % 7 == 3) ? 1 : 11) << i;
    }
    EXPECT_TRUE(group.sortedByKey());
    // A third series whose buckets fall between existing ones.
    for (int i = 0; i < 100; ++i) {
        group[static_cast<uint64_t>(i) * 10 + 5].value = 7;
    }
    EXPECT_FALSE(group.sortedByKey());
    EXPECT_EQ(group.at(55).value, 7);
    EXPECT_EQ(group.at(50).value, 11);
}

TEST(BucketMapTest, RandomOperationsMatchUnorderedMap) {
    std::mt19937_64 rng(20261122);
    for (int round = 0; round < 30; ++round) {
        BucketMap<Tracked> map;
        std::unordered_map<uint64_t, Tracked> ref;
        const uint64_t keySpace = (round % 3 == 0) ? 50 : (round % 3 == 1 ? 5000 : 1000000);
        const int ops = (round < 25) ? 4000 : 60000;
        uint64_t ascending = 0;
        for (int op = 0; op < ops; ++op) {
            uint64_t key = rng() % keySpace;
            switch (rng() % 12) {
                case 0:
                case 1:
                case 2: {  // in-order append, the common case
                    ascending += 1 + rng() % 3;
                    key = keySpace + ascending;
                    map[key].value = op;
                    ref[key].value = op;
                    break;
                }
                case 3:
                case 4:
                    map[key].value += 1;
                    ref[key].value += 1;
                    break;
                case 5: {
                    auto a = map.try_emplace(key, op);
                    auto b = ref.try_emplace(key, op);
                    ASSERT_EQ(a.second, b.second);
                    ASSERT_EQ(a.first->first, key);
                    ASSERT_EQ(a.first->second.value, b.first->second.value);
                    break;
                }
                case 6: {
                    auto a = map.emplace(key, Tracked(op));
                    auto b = ref.emplace(key, Tracked(op));
                    ASSERT_EQ(a.second, b.second);
                    ASSERT_EQ(a.first->second.value, b.first->second.value);
                    break;
                }
                case 7: {
                    auto a = map.find(key);
                    auto b = ref.find(key);
                    ASSERT_EQ(a == map.end(), b == ref.end());
                    if (b != ref.end()) {
                        ASSERT_EQ(a->second.value, b->second.value);
                    }
                    ASSERT_EQ(map.count(key), ref.count(key));
                    ASSERT_EQ(map.contains(key), ref.count(key) == 1);
                    break;
                }
                case 8:
                    ASSERT_EQ(map.erase(key), ref.erase(key));
                    break;
                case 9: {  // sequential lookups, as a fold of a second series does
                    uint64_t k = rng() % keySpace;
                    for (int i = 0; i < 20; ++i, ++k) {
                        ASSERT_EQ(map.count(k), ref.count(k));
                    }
                    break;
                }
                default: {
                    const uint64_t k = key + keySpace * 2;  // mostly absent
                    ASSERT_EQ(map.find(k) == map.end(), ref.find(k) == ref.end());
                    break;
                }
            }
            ASSERT_EQ(map.size(), ref.size());
        }
        expectSameContents(map, ref);

        // sortedByKey() never claims an order that is not there.
        if (map.sortedByKey()) {
            uint64_t prev = 0;
            bool first = true;
            for (const auto& [key, v] : map) {
                ASSERT_TRUE(first || key > prev);
                prev = key;
                first = false;
            }
        }
    }
    EXPECT_EQ(Tracked::alive, 0);
}

TEST(BucketMapTest, ReferencesAndIteratorsSurviveInsertions) {
    BucketMap<Tracked> map;
    Tracked* first = &map[1000];
    first->value = 42;
    auto it = map.find(1000);
    std::vector<Tracked*> pointers;
    for (uint64_t k = 0; k < 50000; ++k) {
        pointers.push_back(&map[k * 3 + 2000]);  // grows through every block size
        pointers.back()->value = static_cast<int>(k);
    }
    for (uint64_t k = 0; k < 500; ++k) {
        map[k].value = -1;  // out-of-order keys: builds and uses the index
    }
    EXPECT_EQ(first, &map.at(1000));
    EXPECT_EQ(first->value, 42);
    EXPECT_EQ(it->second.value, 42);
    for (uint64_t k = 0; k < 50000; ++k) {
        ASSERT_EQ(pointers[k], &map.at(k * 3 + 2000));
        ASSERT_EQ(pointers[k]->value, static_cast<int>(k));
    }
}

TEST(BucketMapTest, CopyMoveClearAndDropTailManageLifetimes) {
    const int64_t before = Tracked::alive;
    {
        BucketMap<Tracked> a;
        for (int i = 0; i < 9000; ++i) {
            a[static_cast<uint64_t>((i * 7919) % 100003)].value = i;
        }
        std::unordered_map<uint64_t, Tracked> ref;
        for (int i = 0; i < 9000; ++i) {
            ref[static_cast<uint64_t>((i * 7919) % 100003)].value = i;
        }

        BucketMap<Tracked> copy = a;
        expectSameContents(copy, ref);
        expectSameContents(a, ref);

        BucketMap<Tracked> moved = std::move(a);
        expectSameContents(moved, ref);
        EXPECT_TRUE(a.empty());  // NOLINT(bugprone-use-after-move): the moved-from state is specified
        EXPECT_TRUE(a.sortedByKey());
        a[5].value = 1;  // a moved-from map is usable
        EXPECT_EQ(a.size(), 1u);

        copy = moved;
        expectSameContents(copy, ref);
        a = std::move(copy);
        expectSameContents(a, ref);

        // Release in steps, as a consumer of a large result does.
        size_t remaining = moved.size();
        while (!moved.empty()) {
            moved.dropTail(1000);
            remaining = remaining > 1000 ? remaining - 1000 : 0;
            ASSERT_EQ(moved.size(), remaining);
            size_t seen = 0;
            for (const auto& [key, v] : moved) {
                ASSERT_EQ(v.value, ref.at(key).value);
                ++seen;
            }
            ASSERT_EQ(seen, remaining);
        }
        moved[77].value = 3;
        EXPECT_EQ(moved.at(77).value, 3);
        EXPECT_TRUE(moved.sortedByKey());

        a.clear();
        EXPECT_TRUE(a.empty());
        EXPECT_EQ(a.find(5), a.end());
    }
    EXPECT_EQ(Tracked::alive, before);
}

TEST(BucketMapTest, EraseKeepsEveryOtherEntryReachable) {
    BucketMap<Tracked> map;
    std::map<uint64_t, int> ref;
    for (int i = 0; i < 3000; ++i) {
        map[static_cast<uint64_t>(i) * 2].value = i;
        ref[static_cast<uint64_t>(i) * 2] = i;
    }
    // Erase from the front, the middle and the back, interleaved with inserts.
    for (uint64_t key : {uint64_t{0}, uint64_t{2998}, uint64_t{5998}, uint64_t{4}, uint64_t{5996}}) {
        ASSERT_EQ(map.erase(key), 1u);
        ASSERT_EQ(map.erase(key), 0u);
        ref.erase(key);
        map[key + 1].value = 9;
        ref[key + 1] = 9;
        ASSERT_EQ(map.size(), ref.size());
        for (const auto& [k, v] : ref) {
            ASSERT_EQ(map.at(k).value, v) << "after erasing " << key;
        }
    }
    // The single-bucket case: the only entry is removed.
    BucketMap<Tracked> one;
    one[60];
    EXPECT_EQ(one.erase(60), 1u);
    EXPECT_TRUE(one.empty());
    EXPECT_TRUE(one.sortedByKey());
    one[30].value = 1;
    one[90].value = 2;
    EXPECT_TRUE(one.sortedByKey());
}

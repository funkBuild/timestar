#include "../../../lib/index/native/memtable.hpp"
#include "../../../lib/index/native/merge_iterator.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <format>
#include <memory>
#include <string>
#include <vector>

using namespace timestar::index;

// Adapts a MemTable iterator to the IteratorSource interface.
class MemTableSource : public IteratorSource {
public:
    MemTableSource(const MemTable& mt, int priority) : mt_(mt), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override {
        it_ = mt_.newIterator();
        it_.seek(target);
        co_return;
    }
    seastar::future<> seekToFirst() override {
        it_ = mt_.newIterator();
        it_.seekToFirst();
        co_return;
    }
    seastar::future<> next() override {
        it_.next();
        co_return;
    }

    bool valid() const override { return it_.valid(); }
    std::string_view key() const override { return it_.key(); }
    std::string_view value() const override { return it_.value(); }
    bool isTombstone() const override { return it_.isTombstone(); }
    int priority() const override { return priority_; }

private:
    const MemTable& mt_;
    int priority_;
    MemTable::Iterator it_;
};

TEST(MergeIteratorTest, SingleSource) {
    MemTable mt;
    mt.put("a", "1");
    mt.put("b", "2");
    mt.put("c", "3");

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(mt, 0));

    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();

    std::vector<std::string> keys;
    while (merger.valid()) {
        keys.emplace_back(merger.key());
        merger.next().get();
    }
    EXPECT_EQ(keys, (std::vector<std::string>{"a", "b", "c"}));
}

TEST(MergeIteratorTest, TwoSourcesMerge) {
    MemTable mt1, mt2;
    mt1.put("a", "1");
    mt1.put("c", "3");
    mt2.put("b", "2");
    mt2.put("d", "4");

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(mt1, 0));
    sources.push_back(std::make_unique<MemTableSource>(mt2, 1));

    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();

    std::vector<std::pair<std::string, std::string>> results;
    while (merger.valid()) {
        results.push_back({std::string(merger.key()), std::string(merger.value())});
        merger.next().get();
    }
    EXPECT_EQ(results.size(), 4u);
    EXPECT_EQ(results[0].first, "a");
    EXPECT_EQ(results[1].first, "b");
    EXPECT_EQ(results[2].first, "c");
    EXPECT_EQ(results[3].first, "d");
}

TEST(MergeIteratorTest, DuplicateKeysNewestWins) {
    MemTable newer, older;
    newer.put("key", "new_value");
    older.put("key", "old_value");

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(newer, 0));  // priority 0 = newest
    sources.push_back(std::make_unique<MemTableSource>(older, 1));  // priority 1 = older

    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();

    EXPECT_TRUE(merger.valid());
    EXPECT_EQ(merger.key(), "key");
    EXPECT_EQ(merger.value(), "new_value");

    merger.next().get();
    EXPECT_FALSE(merger.valid());
}

TEST(MergeIteratorTest, TombstoneSuppresses) {
    MemTable newer, older;
    newer.remove("key");                // Tombstone in newer
    older.put("key", "old_value");      // Live entry in older
    older.put("other", "still_alive");  // Another key

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(newer, 0));
    sources.push_back(std::make_unique<MemTableSource>(older, 1));

    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();

    // "key" should be suppressed by tombstone
    EXPECT_TRUE(merger.valid());
    EXPECT_EQ(merger.key(), "other");
    EXPECT_EQ(merger.value(), "still_alive");

    merger.next().get();
    EXPECT_FALSE(merger.valid());
}

TEST(MergeIteratorTest, Seek) {
    MemTable mt1, mt2;
    mt1.put("aaa", "1");
    mt1.put("ccc", "3");
    mt2.put("bbb", "2");
    mt2.put("ddd", "4");

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(mt1, 0));
    sources.push_back(std::make_unique<MemTableSource>(mt2, 1));

    MergeIterator merger(std::move(sources));
    merger.seek("bbb").get();

    EXPECT_TRUE(merger.valid());
    EXPECT_EQ(merger.key(), "bbb");
}

TEST(MergeIteratorTest, EmptySources) {
    std::vector<std::unique_ptr<IteratorSource>> sources;
    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();
    EXPECT_FALSE(merger.valid());
}

TEST(MergeIteratorTest, ThreeSourcesMerge) {
    MemTable mt1, mt2, mt3;
    // Interleaved keys
    for (int i = 0; i < 30; ++i) {
        auto key = std::format("key:{:03d}", i);
        if (i % 3 == 0) mt1.put(key, "src1");
        if (i % 3 == 1) mt2.put(key, "src2");
        if (i % 3 == 2) mt3.put(key, "src3");
    }

    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableSource>(mt1, 0));
    sources.push_back(std::make_unique<MemTableSource>(mt2, 1));
    sources.push_back(std::make_unique<MemTableSource>(mt3, 2));

    MergeIterator merger(std::move(sources));
    merger.seekToFirst().get();

    int count = 0;
    std::string prev;
    while (merger.valid()) {
        EXPECT_GT(std::string(merger.key()), prev);
        prev = std::string(merger.key());
        ++count;
        merger.next().get();
    }
    EXPECT_EQ(count, 30);
}

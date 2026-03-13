#include "subscription_manager.hpp"

#include <gtest/gtest.h>

using namespace timestar;

class SubscriptionManagerTest : public ::testing::Test {
protected:
    SubscriptionManager mgr;

    Subscription makeSub(const std::string& measurement, const std::map<std::string, std::string>& scopes = {},
                         const std::vector<std::string>& fields = {}) {
        Subscription sub;
        sub.id = mgr.allocateId();
        sub.measurement = measurement;
        sub.scopes = scopes;
        sub.fields = fields;
        sub.handlerShard = seastar::this_shard_id();
        return sub;
    }
};

// --- Subscription::matches tests ---

TEST(SubscriptionMatchTest, ExactMeasurementMatch) {
    Subscription sub;
    sub.measurement = "temperature";

    EXPECT_TRUE(sub.matches("temperature", {}, "value"));
    EXPECT_FALSE(sub.matches("humidity", {}, "value"));
}

TEST(SubscriptionMatchTest, FieldFilterMatchesSpecificField) {
    Subscription sub;
    sub.measurement = "cpu";
    sub.fields = {"usage_percent", "idle"};

    EXPECT_TRUE(sub.matches("cpu", {}, "usage_percent"));
    EXPECT_TRUE(sub.matches("cpu", {}, "idle"));
    EXPECT_FALSE(sub.matches("cpu", {}, "steal"));
}

TEST(SubscriptionMatchTest, EmptyFieldsMatchAll) {
    Subscription sub;
    sub.measurement = "cpu";

    EXPECT_TRUE(sub.matches("cpu", {}, "usage_percent"));
    EXPECT_TRUE(sub.matches("cpu", {}, "idle"));
    EXPECT_TRUE(sub.matches("cpu", {}, "anything"));
}

TEST(SubscriptionMatchTest, TagScopeExactMatch) {
    Subscription sub;
    sub.measurement = "temperature";
    sub.scopes = {{"location", "us-west"}, {"host", "server-01"}};

    std::map<std::string, std::string> matchingTags = {{"location", "us-west"}, {"host", "server-01"}};
    EXPECT_TRUE(sub.matches("temperature", matchingTags, "value"));

    std::map<std::string, std::string> partialTags = {{"location", "us-west"}};
    EXPECT_FALSE(sub.matches("temperature", partialTags, "value"));

    std::map<std::string, std::string> wrongTags = {{"location", "us-east"}, {"host", "server-01"}};
    EXPECT_FALSE(sub.matches("temperature", wrongTags, "value"));
}

TEST(SubscriptionMatchTest, EmptyScopesMatchAllTags) {
    Subscription sub;
    sub.measurement = "temperature";

    std::map<std::string, std::string> anyTags = {{"location", "us-west"}, {"host", "server-99"}};
    EXPECT_TRUE(sub.matches("temperature", anyTags, "value"));
    EXPECT_TRUE(sub.matches("temperature", {}, "value"));
}

TEST(SubscriptionMatchTest, CombinedFilters) {
    Subscription sub;
    sub.measurement = "cpu";
    sub.scopes = {{"datacenter", "dc1"}};
    sub.fields = {"usage_percent"};

    std::map<std::string, std::string> tags = {{"datacenter", "dc1"}, {"host", "srv1"}};
    EXPECT_TRUE(sub.matches("cpu", tags, "usage_percent"));
    EXPECT_FALSE(sub.matches("cpu", tags, "idle"));
    EXPECT_FALSE(sub.matches("memory", tags, "usage_percent"));

    std::map<std::string, std::string> wrongDC = {{"datacenter", "dc2"}};
    EXPECT_FALSE(sub.matches("cpu", wrongDC, "usage_percent"));
}

// --- SubscriptionManager tests ---

TEST_F(SubscriptionManagerTest, AddAndRemoveSubscription) {
    EXPECT_EQ(mgr.subscriptionCount(), 0u);

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    EXPECT_EQ(mgr.subscriptionCount(), 1u);

    mgr.removeSubscription(sub.id);
    EXPECT_EQ(mgr.subscriptionCount(), 0u);
}

TEST_F(SubscriptionManagerTest, HasSubscribersReturnsCorrectly) {
    EXPECT_FALSE(mgr.hasSubscribers("temperature"));

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    EXPECT_TRUE(mgr.hasSubscribers("temperature"));
    EXPECT_FALSE(mgr.hasSubscribers("humidity"));

    mgr.removeSubscription(sub.id);
    EXPECT_FALSE(mgr.hasSubscribers("temperature"));
}

TEST_F(SubscriptionManagerTest, MultipleSubscriptionsSameMeasurement) {
    auto sub1 = makeSub("temperature", {{"location", "us-west"}});
    auto sub2 = makeSub("temperature", {{"location", "us-east"}});

    mgr.addSubscription(sub1);
    mgr.addSubscription(sub2);
    EXPECT_NE(sub1.id, sub2.id);
    EXPECT_EQ(mgr.subscriptionCount(), 2u);
    EXPECT_TRUE(mgr.hasSubscribers("temperature"));

    mgr.removeSubscription(sub1.id);
    EXPECT_EQ(mgr.subscriptionCount(), 1u);
    EXPECT_TRUE(mgr.hasSubscribers("temperature"));

    mgr.removeSubscription(sub2.id);
    EXPECT_EQ(mgr.subscriptionCount(), 0u);
    EXPECT_FALSE(mgr.hasSubscribers("temperature"));
}

TEST_F(SubscriptionManagerTest, RemoveNonexistentIdIsNoop) {
    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    EXPECT_EQ(mgr.subscriptionCount(), 1u);

    mgr.removeSubscription(99999);
    EXPECT_EQ(mgr.subscriptionCount(), 1u);
}

TEST_F(SubscriptionManagerTest, NotifyWithNoSubscribers) {
    std::vector<uint64_t> ts = {100, 200};
    std::vector<double> vals = {1.0, 2.0};

    auto remotes = mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    EXPECT_TRUE(remotes.empty());
}

TEST_F(SubscriptionManagerTest, NotifyMatchingSubscriberLocal) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000, 2000, 3000};
    std::vector<double> vals = {23.5, 24.0, 24.5};

    auto remotes = mgr.notifySubscribers("temperature", {{"location", "us-west"}}, "value", ts, vals);
    EXPECT_TRUE(remotes.empty());  // local delivery, no remotes

    EXPECT_FALSE(queue.empty());
    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    const auto& batch = *batchPtr;
    EXPECT_EQ(batch.points.size(), 3u);
    EXPECT_EQ(batch.points[0].timestamp, 1000u);
    EXPECT_EQ(std::get<double>(batch.points[0].value), 23.5);
    EXPECT_EQ(batch.points[0].measurement, "temperature");
    EXPECT_EQ(batch.points[0].field, "value");
}

TEST_F(SubscriptionManagerTest, NotifyNonMatchingMeasurementSkipped) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {42.0};

    auto remotes = mgr.notifySubscribers("humidity", {}, "value", ts, vals);
    EXPECT_TRUE(remotes.empty());
    EXPECT_TRUE(queue.empty());
}

TEST_F(SubscriptionManagerTest, NotifyFiltersByTags) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("temperature", {{"location", "us-west"}});
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {42.0};

    // Matching tags
    mgr.notifySubscribers("temperature", {{"location", "us-west"}}, "value", ts, vals);
    EXPECT_FALSE(queue.empty());
    queue.pop();

    // Non-matching tags
    mgr.notifySubscribers("temperature", {{"location", "us-east"}}, "value", ts, vals);
    EXPECT_TRUE(queue.empty());
}

TEST_F(SubscriptionManagerTest, NotifyFiltersByField) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("cpu", {}, {"usage_percent"});
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {85.0};

    mgr.notifySubscribers("cpu", {}, "usage_percent", ts, vals);
    EXPECT_FALSE(queue.empty());
    queue.pop();

    mgr.notifySubscribers("cpu", {}, "idle", ts, vals);
    EXPECT_TRUE(queue.empty());
}

TEST_F(SubscriptionManagerTest, NotifyMultipleSubscribers) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> q1(16);
    seastar::queue<std::shared_ptr<const StreamingBatch>> q2(16);

    auto sub1 = makeSub("temperature", {{"location", "us-west"}});
    auto sub2 = makeSub("temperature");  // matches all tags
    mgr.addSubscription(sub1);
    mgr.registerQueue(sub1.id, &q1);
    mgr.addSubscription(sub2);
    mgr.registerQueue(sub2.id, &q2);

    std::vector<uint64_t> ts = {1000, 2000};
    std::vector<double> vals = {23.5, 24.0};

    auto remotes = mgr.notifySubscribers("temperature", {{"location", "us-west"}}, "value", ts, vals);
    EXPECT_TRUE(remotes.empty());
    EXPECT_FALSE(q1.empty());
    EXPECT_FALSE(q2.empty());
}

TEST_F(SubscriptionManagerTest, BackpressureDropsOnFullQueue) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(1);

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {23.5};

    // First push succeeds
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    EXPECT_FALSE(queue.empty());

    // Second push fails (queue full), batch is dropped
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    // Queue still has 1 item (the first one)
    EXPECT_EQ(queue.size(), 1u);
}

// When a data-batch push fails the queue is by definition at max capacity
// (push() returns false only when _q.size() >= _max), so a drop-note push
// immediately after will also fail.  The dropped count must accumulate across
// all consecutive failures so that stats reporting is accurate.
//
// Scenario (capacity = 2):
//   1. Fill queue to capacity (2 data batches, each 2 points).
//   2. Three more notifies: all fail (data + drop note both fail each time).
//      Each failure adds 2 to droppedCounters → 6 total dropped.
//   3. getStats() must report droppedPoints == 6.
//   4. Queue size must remain at 2 throughout.
TEST_F(SubscriptionManagerTest, DropNoteCarriesAccumulatedCountAfterQueueDrains) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(2);

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000, 2000};  // 2-point batch
    std::vector<double> vals = {23.5, 24.0};

    // Fill the queue to capacity (both pushes succeed).
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // size→1
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // size→2 (full)
    EXPECT_EQ(queue.size(), 2u);

    // Three more notifies: data push fails (queue full), drop-note push also
    // fails (still full). Each drops 2 points → 6 total dropped.
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // +2 dropped
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // +2 dropped
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // +2 dropped
    EXPECT_EQ(queue.size(), 2u);                                  // unchanged

    auto stats = mgr.getStats();
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].droppedPoints, 6u);
    EXPECT_EQ(stats[0].queueDepth, 2u);
    EXPECT_EQ(stats[0].queueCapacity, 2u);
}

// When a data-batch push fails and the drop-note push also fails (queue is at
// max capacity), the dropped count must be correctly accumulated so it appears
// in the next successful drop notification.
TEST_F(SubscriptionManagerTest, DroppedCountAccumulatesWhenDropNoteAlsoFails) {
    // capacity=1: any push after the first will fail both the data push and the
    // drop-note push. Accumulated count should grow across multiple failures.
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(1);

    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000, 2000, 3000};  // 3-point batch
    std::vector<double> vals = {1.0, 2.0, 3.0};

    // Fill the queue.
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);  // size→1, full
    EXPECT_EQ(queue.size(), 1u);

    // Four consecutive failures, each dropping 3 points → 12 total dropped.
    for (int i = 0; i < 4; ++i) {
        mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    }
    EXPECT_EQ(queue.size(), 1u);  // queue unchanged

    // Stats should reflect the accumulated dropped count.
    auto stats = mgr.getStats();
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].droppedPoints, 12u);
}

TEST_F(SubscriptionManagerTest, NotifyWithStringValues) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("logs");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<std::string> vals = {"error: connection refused"};

    mgr.notifySubscribers("logs", {}, "message", ts, vals);

    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    EXPECT_EQ(std::get<std::string>(batchPtr->points[0].value), "error: connection refused");
}

TEST_F(SubscriptionManagerTest, NotifyWithBoolValues) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("switches");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000, 2000};
    std::vector<bool> vals = {true, false};

    mgr.notifySubscribers("switches", {}, "state", ts, vals);

    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    const auto& batch = *batchPtr;
    EXPECT_EQ(std::get<bool>(batch.points[0].value), true);
    EXPECT_EQ(std::get<bool>(batch.points[1].value), false);
}

TEST_F(SubscriptionManagerTest, NotifyWithInt64Values) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("counters");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<int64_t> vals = {42};

    mgr.notifySubscribers("counters", {}, "count", ts, vals);

    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    EXPECT_EQ(std::get<int64_t>(batchPtr->points[0].value), 42);
}

TEST_F(SubscriptionManagerTest, SequenceIdIncrementsPerSubscription) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {23.5};

    mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);

    auto b1 = queue.pop();
    auto b2 = queue.pop();
    auto b3 = queue.pop();
    ASSERT_TRUE(b1);
    ASSERT_TRUE(b2);
    ASSERT_TRUE(b3);

    EXPECT_EQ(b1->sequenceId, 0u);
    EXPECT_EQ(b2->sequenceId, 1u);
    EXPECT_EQ(b3->sequenceId, 2u);
}

TEST_F(SubscriptionManagerTest, DeliverBatchDirectly) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);
    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    auto batch = std::make_shared<StreamingBatch>();
    StreamingDataPoint pt;
    pt.measurement = "temperature";
    pt.field = "value";
    pt.timestamp = 5000;
    pt.value = 99.9;
    batch->points.push_back(std::move(pt));

    mgr.deliverBatch(sub.id, std::move(batch));

    EXPECT_FALSE(queue.empty());
    auto resultPtr = queue.pop();
    ASSERT_TRUE(resultPtr);
    const auto& result = *resultPtr;
    EXPECT_EQ(result.points.size(), 1u);
    EXPECT_EQ(result.sequenceId, 0u);
    EXPECT_EQ(std::get<double>(result.points[0].value), 99.9);
}

TEST_F(SubscriptionManagerTest, RemoteSubscriptionReturnsRemoteDelivery) {
    // Create a subscription with a different handler shard
    Subscription sub;
    sub.id = mgr.allocateId();
    sub.measurement = "temperature";
    sub.handlerShard = seastar::this_shard_id() + 1;  // different shard

    mgr.addSubscription(sub);
    // No queue registered (this is a remote shard copy)

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {23.5};

    auto remotes = mgr.notifySubscribers("temperature", {}, "value", ts, vals);
    EXPECT_EQ(remotes.size(), 1u);
    EXPECT_EQ(remotes[0].subscriptionId, sub.id);
    EXPECT_EQ(remotes[0].targetShard, sub.handlerShard);
    ASSERT_TRUE(remotes[0].batch);
    EXPECT_EQ(remotes[0].batch->points.size(), 1u);
}

TEST_F(SubscriptionManagerTest, GetStatsReturnsLocalSubscriptions) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(32);
    auto sub = makeSub("temperature", {{"loc", "west"}}, {"value"});
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    // Send some data to increment counters
    std::vector<uint64_t> ts = {1000, 2000};
    std::vector<double> vals = {10.0, 20.0};
    mgr.notifySubscribers("temperature", {{"loc", "west"}}, "value", ts, vals);
    mgr.notifySubscribers("temperature", {{"loc", "west"}}, "value", ts, vals);

    // Drain the queue
    queue.pop();
    queue.pop();

    auto stats = mgr.getStats();
    ASSERT_EQ(stats.size(), 1u);

    EXPECT_EQ(stats[0].id, sub.id);
    EXPECT_EQ(stats[0].measurement, "temperature");
    EXPECT_EQ(stats[0].scopes.at("loc"), "west");
    ASSERT_EQ(stats[0].fields.size(), 1u);
    EXPECT_EQ(stats[0].fields[0], "value");
    EXPECT_EQ(stats[0].queueCapacity, 32u);
    EXPECT_EQ(stats[0].queueDepth, 0u);  // We drained it
    EXPECT_EQ(stats[0].eventsSent, 2u);  // Two notifications
    EXPECT_EQ(stats[0].droppedPoints, 0u);
}

TEST_F(SubscriptionManagerTest, GetStatsSkipsRemoteSubscriptions) {
    // Remote subscription (different handler shard) should not appear in stats
    Subscription sub;
    sub.id = mgr.allocateId();
    sub.measurement = "temperature";
    sub.handlerShard = seastar::this_shard_id() + 1;
    mgr.addSubscription(sub);

    auto stats = mgr.getStats();
    EXPECT_TRUE(stats.empty());
}

TEST_F(SubscriptionManagerTest, GetStatsEmptyWhenNoSubscriptions) {
    auto stats = mgr.getStats();
    EXPECT_TRUE(stats.empty());
}

// --- Label propagation tests ---

TEST_F(SubscriptionManagerTest, LabelPropagatedToBatch) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);

    Subscription sub;
    sub.id = mgr.allocateId();
    sub.measurement = "cpu";
    sub.handlerShard = seastar::this_shard_id();
    sub.label = "cpu_usage";
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {85.0};
    mgr.notifySubscribers("cpu", {}, "usage", ts, vals);

    ASSERT_FALSE(queue.empty());
    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    EXPECT_EQ(batchPtr->label, "cpu_usage");
    EXPECT_EQ(batchPtr->points.size(), 1u);
}

TEST_F(SubscriptionManagerTest, EmptyLabelForSingleQueryCompat) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);

    // Subscription with no label (single-query backward compat)
    auto sub = makeSub("temperature");
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> vals = {23.5};
    mgr.notifySubscribers("temperature", {}, "value", ts, vals);

    auto batchPtr = queue.pop();
    ASSERT_TRUE(batchPtr);
    EXPECT_TRUE(batchPtr->label.empty());
}

TEST_F(SubscriptionManagerTest, MultipleLabelsOnSharedQueue) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);

    // Two subscriptions with different labels, same queue
    Subscription sub1;
    sub1.id = mgr.allocateId();
    sub1.measurement = "cpu";
    sub1.handlerShard = seastar::this_shard_id();
    sub1.label = "cpu";
    mgr.addSubscription(sub1);
    mgr.registerQueue(sub1.id, &queue);

    Subscription sub2;
    sub2.id = mgr.allocateId();
    sub2.measurement = "memory";
    sub2.handlerShard = seastar::this_shard_id();
    sub2.label = "mem";
    mgr.addSubscription(sub2);
    mgr.registerQueue(sub2.id, &queue);

    std::vector<uint64_t> ts = {1000};
    std::vector<double> cpuVals = {90.0};
    std::vector<double> memVals = {70.0};

    mgr.notifySubscribers("cpu", {}, "usage", ts, cpuVals);
    mgr.notifySubscribers("memory", {}, "used", ts, memVals);

    // Both batches should arrive in the shared queue with distinct labels
    ASSERT_EQ(queue.size(), 2u);
    auto b1 = queue.pop();
    auto b2 = queue.pop();
    ASSERT_TRUE(b1);
    ASSERT_TRUE(b2);

    // Order depends on notification order
    std::set<std::string> labels = {b1->label, b2->label};
    EXPECT_TRUE(labels.count("cpu"));
    EXPECT_TRUE(labels.count("mem"));
}

TEST_F(SubscriptionManagerTest, GetStatsIncludesLabel) {
    seastar::queue<std::shared_ptr<const StreamingBatch>> queue(16);

    Subscription sub;
    sub.id = mgr.allocateId();
    sub.measurement = "temperature";
    sub.handlerShard = seastar::this_shard_id();
    sub.label = "temp_west";
    mgr.addSubscription(sub);
    mgr.registerQueue(sub.id, &queue);

    auto stats = mgr.getStats();
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].label, "temp_west");
}

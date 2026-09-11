#include "native_index.hpp"
#include <seastar/core/app-template.hh>
#include <seastar/core/preempt.hh>
#include <cstdio>
#include <stdexcept>

namespace timestar::index {
struct NativeIndexTestAccess {
    static void seed(NativeIndex& index, const std::string& key) { index.dayBitmapCache_[key].bitmap.add(17); }
    static seastar::future<bool> add(NativeIndex& index, std::string& key) { return index.addDayMembership(key,29); }
    static void rehash(NativeIndex& index) { index.dayBitmapCache_.reserve(index.dayBitmapCache_.bucket_count()*4+100); }
    static bool contains(NativeIndex& index, const std::string& key) { return index.dayBitmapCache_.at(key).bitmap.contains(29); }
};
}
using namespace timestar::index;
seastar::future<int> probe() {
    // A cached day needs no disk access. Avoid open()/timers so the only
    // suspension under test is addDayMembership's ready-future await.
    NativeIndex index(0);
    std::string key("server.metrics\0day!",19);
    NativeIndexTestAccess::seed(index,key);
    seastar::internal::preemption_monitor forced;
    forced.head.store(1);forced.tail.store(0);
    auto* previous=seastar::internal::get_need_preempt_var();
    seastar::internal::set_need_preempt_var(&forced);
    auto pending=NativeIndexTestAccess::add(index,key);
    seastar::internal::set_need_preempt_var(previous);
    std::printf("pending=%d; forcing cache rehash before resumption\n",!pending.available());std::fflush(stdout);
    NativeIndexTestAccess::rehash(index);
    bool added=co_await std::move(pending);
    bool present=NativeIndexTestAccess::contains(index,key);
    std::printf("added=%d present=%d\n",added,present);std::fflush(stdout);
    if(!added || !present)throw std::runtime_error("acknowledged day membership missing after cache rehash");
    co_return 0;
}
int main(int argc,char**argv){seastar::app_template app;return app.run(argc,argv,probe);}

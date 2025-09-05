#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include "../../../lib/core/engine.hpp"

int main(int argc, char** argv) {
    seastar::app_template app;
    
    try {
        app.run(argc, argv, [] {
            return seastar::async([] {
                std::cout << "Testing direct engine write..." << std::endl;
                
                // Create engine
                Engine engine;
                
                // Test 1: Write with current timestamp (61-bit)
                uint64_t timestamp = 1756565110829708288ULL;  // 61-bit timestamp
                std::cout << "Writing timestamp: " << timestamp 
                          << " (requires " << (64 - __builtin_clzll(timestamp)) << " bits)" << std::endl;
                
                TSDBInsert<double> insert("test", "value");
                insert.tags = {{"host", "server-01"}};
                insert.addValue(timestamp, 42.0);
                
                try {
                    engine.insert(std::move(insert)).get();
                    std::cout << "✅ Write succeeded!" << std::endl;
                } catch (const std::exception& e) {
                    std::cout << "❌ Write failed: " << e.what() << std::endl;
                }
                
                // Test 2: Write with very large timestamp
                timestamp = UINT64_MAX;  // Max 64-bit
                std::cout << "\nWriting max timestamp: " << timestamp 
                          << " (requires " << (64 - __builtin_clzll(timestamp)) << " bits)" << std::endl;
                
                TSDBInsert<double> insert2("test", "value");
                insert2.tags = {{"host", "server-02"}};
                insert2.addValue(timestamp, 99.0);
                
                try {
                    engine.insert(std::move(insert2)).get();
                    std::cout << "✅ Write succeeded!" << std::endl;
                } catch (const std::exception& e) {
                    std::cout << "❌ Write failed: " << e.what() << std::endl;
                }
                
                std::cout << "\nTest complete!" << std::endl;
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
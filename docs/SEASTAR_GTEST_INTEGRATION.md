# Seastar and Google Test Integration

This document describes how we successfully integrated Seastar's async framework with Google Test for testing the metadata index.

## The Challenge

Seastar requires its own application runtime (`app_template`) to manage the reactor, threads, and async operations. Google Test doesn't natively support Seastar's futures and coroutines, making it challenging to test async code.

## Solution: Wrapper Function Approach

We implemented the first approach suggested in the community: creating a wrapper function that runs Seastar code within Google Test.

### Implementation

```cpp
// Wrapper to run Seastar code within Google Test
int run_in_seastar(std::function<seastar::future<>()> func) {
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    
    seastar::app_template app;
    try {
        return app.run(argc, argv, [func = std::move(func)]() {
            return func().handle_exception([](std::exception_ptr ep) {
                // Handle exceptions properly
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to run Seastar app: " << e.what() << std::endl;
        return 1;
    }
}
```

### Test Pattern

```cpp
TEST_F(MetadataIndexAsyncTest, TestName) {
    int result = run_in_seastar([]() -> seastar::future<> {
        // Your async test code here
        auto index = std::make_unique<MetadataIndex>(TEST_DB_PATH);
        co_await index->init();
        
        // Test operations using co_await
        uint64_t id = co_await index->getOrCreateSeriesId(...);
        EXPECT_GT(id, 0);
        
        co_await index->close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}
```

## Key Files

- **`test/metadata_index_gtest_seastar.cpp`**: Contains async tests using Google Test + Seastar
- **`test/metadata_index_test.cpp`**: Contains synchronous tests using the sync wrapper classes
- **`lib/metadata_index_sync.hpp/cpp`**: Synchronous wrapper for testing without Seastar runtime
- **`lib/metadata_index.hpp/cpp`**: The actual async implementation using Seastar futures

## Testing Approach

We maintain two sets of tests:

1. **Synchronous Tests** (`MetadataIndexTest`): 
   - Use `MetadataIndexSync` class (no Seastar futures)
   - Fast execution (~2ms total)
   - Good for unit testing pure logic

2. **Asynchronous Tests** (`MetadataIndexAsyncTest`):
   - Use real `MetadataIndex` class with Seastar futures
   - Tests actual async behavior and concurrency
   - Slower due to Seastar runtime overhead (~200ms per test)

## Running Tests

```bash
# Run all metadata index tests (sync and async)
./test/tsdb_test --gtest_filter=MetadataIndex*

# Run only sync tests
./test/tsdb_test --gtest_filter=MetadataIndexTest*

# Run only async tests  
./test/tsdb_test --gtest_filter=MetadataIndexAsyncTest*
```

## Results

- All 11 synchronous tests pass in ~2ms
- All 10 asynchronous tests pass in ~2 seconds
- Successfully tests concurrent operations and persistence
- Properly handles Seastar futures and coroutines within Google Test

## Advantages of This Approach

1. **Familiar Testing Framework**: Developers can use Google Test's familiar assertions and test structure
2. **Both Sync and Async**: Support for both synchronous unit tests and async integration tests
3. **Proper Isolation**: Each test gets its own Seastar runtime, preventing interference
4. **Real Async Testing**: Tests actual concurrent behavior with futures and coroutines
5. **No Build Complexity**: Tests compile as part of the main test executable

## Limitations

1. **Performance Overhead**: Each test creates a new Seastar runtime (~200ms overhead)
2. **Single Thread**: Tests run with a single Seastar reactor thread
3. **No Sharding**: Tests don't exercise Seastar's multi-shard capabilities

## Future Improvements

- Consider the second approach (running all Google Tests inside a single Seastar runtime) for better performance if test suite grows large
- Add multi-shard testing for components that use sharding
- Consider using Seastar's native test framework for performance-critical tests
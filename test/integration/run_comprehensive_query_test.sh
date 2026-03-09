#!/bin/bash

# Script to compile and run the comprehensive query test separately
# This is needed because Seastar tests require special handling

echo "Building comprehensive query test..."

# Navigate to build directory
cd "$(dirname "$0")/../../build" || exit 1

# Compile the test as a standalone executable
g++ -std=c++23 \
    -I../external/seastar/include \
    -I../external/seastar/build/release \
    -I../lib \
    -I/usr/include/gtest \
    -L./lib -L../external/seastar/build/release \
    -o comprehensive_query_test \
    ../test/integration/comprehensive_query_test.cpp \
    -llibtimestar -lseastar -lgtest -lgtest_main -lpthread \
    -lboost_system -lboost_filesystem -lboost_thread \
    -lsnappy -lleveldb -lfmt -lnuma -lhwloc \
    -Wl,-rpath,./lib -Wl,-rpath,../external/seastar/build/release

if [ $? -eq 0 ]; then
    echo "Build successful. Running test..."
    ./comprehensive_query_test
else
    echo "Build failed."
    exit 1
fi
#!/bin/bash

# Kill any existing tsdb_http_server processes
echo "Killing existing tsdb_http_server processes..."
pkill -f tsdb_http_server || true
sleep 1

# Clean up any leftover files
echo "Cleaning up old server files..."
rm -f server.log server.pid

# Build the project if needed
if [ ! -f "build/bin/tsdb_http_server" ]; then
    echo "Building tsdb_http_server..."
    mkdir -p build
    cd build
    cmake ..
    make tsdb_http_server
    cd ..
fi

# Start the server in the background
echo "Starting tsdb_http_server..."
./build/bin/tsdb_http_server > server.log 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > server.pid

# Wait for server to start
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:8086/health > /dev/null 2>&1; then
        echo "Server is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Server failed to start. Check server.log for details."
        cat server.log
        exit 1
    fi
    sleep 1
done

# Run the jest tests
echo "Running jest tests..."
cd test_api
npm test

# Capture test exit code
TEST_EXIT_CODE=$?

# Kill the server
echo "Stopping server..."
cd ..
kill $(cat server.pid) 2>/dev/null || true
rm -f server.pid

# Exit with the test exit code
exit $TEST_EXIT_CODE
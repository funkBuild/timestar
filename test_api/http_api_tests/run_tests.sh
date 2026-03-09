#!/bin/bash

# Script to run comprehensive HTTP API tests
# Starts the TimeStar server if not already running, runs tests, then cleans up

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/../../build"
SERVER_PID=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if server is running
check_server() {
    curl -s http://localhost:8086/health > /dev/null 2>&1
    return $?
}

# Function to start the server
start_server() {
    echo -e "${YELLOW}Starting TimeStar HTTP server...${NC}"
    
    # Check if server is already running
    if check_server; then
        echo -e "${GREEN}Server is already running${NC}"
        return 0
    fi
    
    # Build the server if needed
    if [ ! -f "$BUILD_DIR/bin/timestar_http_server" ]; then
        echo -e "${YELLOW}Building TimeStar server...${NC}"
        cd "$BUILD_DIR" && make timestar_http_server
        if [ $? -ne 0 ]; then
            echo -e "${RED}Failed to build server${NC}"
            exit 1
        fi
    fi
    
    # Start the server in background
    cd "$BUILD_DIR" && ./bin/timestar_http_server > /tmp/timestar_server.log 2>&1 &
    SERVER_PID=$!
    echo "Server started with PID: $SERVER_PID"
    
    # Wait for server to be ready
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if check_server; then
            echo -e "${GREEN}Server is ready!${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}Server failed to start. Check /tmp/timestar_server.log for details${NC}"
    return 1
}

# Function to stop the server
stop_server() {
    if [ -n "$SERVER_PID" ]; then
        echo -e "${YELLOW}Stopping TimeStar server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null
        wait $SERVER_PID 2>/dev/null
    fi
}

# Trap to ensure cleanup on exit
trap stop_server EXIT

# Main execution
echo -e "${YELLOW}TimeStar HTTP API Test Suite${NC}"
echo "================================"

# Install dependencies if needed
cd "$SCRIPT_DIR"
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing npm dependencies...${NC}"
    npm install
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install dependencies${NC}"
        exit 1
    fi
fi

# Start server if needed
STARTED_SERVER=false
if ! check_server; then
    start_server
    if [ $? -ne 0 ]; then
        exit 1
    fi
    STARTED_SERVER=true
fi

# Run tests
echo -e "${YELLOW}Running tests...${NC}"
npm test

TEST_RESULT=$?

# Only stop server if we started it
if [ "$STARTED_SERVER" = true ]; then
    stop_server
fi

# Report results
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit $TEST_RESULT
fi
# GET /test - Test Endpoint

## Overview
A simple test endpoint that returns a greeting message. Used to verify that the TSDB HTTP Server is running and responding to requests.

## Endpoint Details
- **URL**: `/test`
- **Method**: `GET`
- **Content-Type**: `text/plain`
- **Authentication**: None required

## Request
No parameters or request body required.

### Example Request
```bash
curl -X GET http://localhost:8086/test
```

## Response

### Success Response (200 OK)
Returns a plain text greeting message.

```
Hello from TSDB HTTP Server!
```

## Usage Examples

### Basic Test
```bash
# Simple connectivity test
curl http://localhost:8086/test
```

### Integration Testing
```python
import requests

def test_server_connectivity():
    """Test that the TSDB server is responding."""
    response = requests.get('http://localhost:8086/test')
    
    assert response.status_code == 200
    assert response.text == "Hello from TSDB HTTP Server!"
    print("✓ Server is responding correctly")

test_server_connectivity()
```

### Monitoring Script
```bash
#!/bin/bash
# Simple health monitoring script

SERVER_URL="http://localhost:8086/test"
EXPECTED_MESSAGE="Hello from TSDB HTTP Server!"

response=$(curl -s "$SERVER_URL")

if [ "$response" = "$EXPECTED_MESSAGE" ]; then
    echo "$(date): Server is healthy"
    exit 0
else
    echo "$(date): Server is not responding correctly"
    echo "Expected: $EXPECTED_MESSAGE"
    echo "Got: $response"
    exit 1
fi
```

## Use Cases
1. **Connectivity Testing**: Verify the server is reachable
2. **Load Balancer Health Checks**: Simple endpoint for health probes
3. **Integration Testing**: Validate server startup in test environments
4. **Network Troubleshooting**: Test basic HTTP functionality

## Related Endpoints
- [`GET /health`](health-endpoint.md) - Detailed health check with JSON response
- [`GET /`](root-endpoint.md) - Root endpoint with API discovery
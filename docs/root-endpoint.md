# GET / - Root Endpoint

## Overview
The root endpoint provides a simple way to discover all available API endpoints in the TSDB HTTP Server. It returns a JSON response listing all supported endpoints for easy API exploration.

## Endpoint Details
- **URL**: `/`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request
No parameters or request body required.

### Example Request
```bash
curl -X GET http://localhost:8086/
```

## Response

### Success Response (200 OK)
Returns a JSON object with server information and available endpoints.

```json
{
  "message": "TSDB HTTP Server",
  "endpoints": [
    "/test",
    "/health", 
    "/write",
    "/query",
    "/delete",
    "/measurements",
    "/tags",
    "/fields",
    "/functions",
    "/query/parse",
    "/functions/performance",
    "/functions/cache"
  ]
}
```

### Response Fields
- **message**: Server identification string
- **endpoints**: Array of available API endpoint paths

## Usage Examples

### Basic Discovery
```bash
# Discover all available endpoints
curl http://localhost:8086/

# Pretty print the JSON response
curl -s http://localhost:8086/ | jq
```

### Integration Example
```python
import requests
import json

# Get available endpoints
response = requests.get('http://localhost:8086/')
data = response.json()

print(f"Server: {data['message']}")
print("Available endpoints:")
for endpoint in data['endpoints']:
    print(f"  - {endpoint}")
```

## Use Cases
1. **API Discovery**: Quickly discover what endpoints are available
2. **Service Health**: Verify the server is responding and operational
3. **Documentation**: Programmatically generate API documentation
4. **Integration Testing**: Validate expected endpoints are present

## Related Endpoints
- [`GET /health`](health-endpoint.md) - Health check status
- [`GET /test`](test-endpoint.md) - Simple test message
# GET /health - Health Check Endpoint

## Overview
The health check endpoint provides server status information in JSON format. It's designed for monitoring systems, load balancers, and automated health checks that need structured health information.

## Endpoint Details
- **URL**: `/health`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request
No parameters or request body required.

### Example Request
```bash
curl -X GET http://localhost:8086/health
```

## Response

### Success Response (200 OK)
Returns a JSON object indicating the server's health status.

```json
{
  "status": "healthy"
}
```

### Response Fields
- **status**: Server health status (always "healthy" when server is responding)

## Usage Examples

### Basic Health Check
```bash
# Simple health check
curl http://localhost:8086/health

# Pretty print JSON response
curl -s http://localhost:8086/health | jq
```

### Load Balancer Configuration
```nginx
# Nginx upstream health check configuration
upstream tsdb_servers {
    server 127.0.0.1:8086;
    server 127.0.0.1:8087;
    server 127.0.0.1:8088;
}

# Health check configuration
location /health {
    access_log off;
    return 200 "healthy\n";
    add_header Content-Type text/plain;
}
```

### Monitoring Script
```python
import requests
import json
import sys
import time

def check_tsdb_health(url="http://localhost:8086/health"):
    """Check TSDB server health and return status."""
    try:
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "healthy":
                print(f"✓ TSDB server is healthy")
                return True
        
        print(f"✗ TSDB server returned unhealthy status: {response.status_code}")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to connect to TSDB server: {e}")
        return False

# Continuous monitoring
while True:
    if not check_tsdb_health():
        sys.exit(1)
    time.sleep(30)  # Check every 30 seconds
```

### Docker Healthcheck
```dockerfile
# Dockerfile healthcheck example
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8086/health || exit 1
```

### Kubernetes Liveness Probe
```yaml
# Kubernetes deployment with health check
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tsdb-server
spec:
  template:
    spec:
      containers:
      - name: tsdb
        image: tsdb-server:latest
        ports:
        - containerPort: 8086
        livenessProbe:
          httpGet:
            path: /health
            port: 8086
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health
            port: 8086
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
```

## Use Cases
1. **Load Balancer Health Checks**: Automated traffic routing decisions
2. **Container Orchestration**: Kubernetes/Docker health probes
3. **Monitoring Systems**: Automated alerting and status monitoring
4. **Service Discovery**: Health-aware service registration
5. **Automated Recovery**: Restart unhealthy instances

## Error Handling
- **Connection Refused**: Server is down or not responding
- **Timeout**: Server is overloaded or network issues
- **Non-200 Status**: Server is experiencing internal issues

## Related Endpoints
- [`GET /test`](test-endpoint.md) - Simple text-based connectivity test
- [`GET /`](root-endpoint.md) - Root endpoint with service information
# Security

## Current State

TimeStar does not include built-in authentication, authorization, or TLS. All HTTP endpoints are unauthenticated and communicate in plaintext. The server is designed to run behind a reverse proxy that handles these concerns.

## Recommended Deployment

Place a reverse proxy in front of TimeStar to provide:

- **TLS termination** -- encrypt traffic between clients and the proxy.
- **Authentication** -- API keys, HTTP basic auth, OAuth2, or mutual TLS (mTLS).
- **Rate limiting** -- protect against excessive request volume.
- **IP allowlisting** -- restrict access to known clients or internal networks.

### Example: nginx Reverse Proxy

```nginx
upstream timestar {
    server 127.0.0.1:8086;
}

server {
    listen 443 ssl;
    server_name tsdb.example.com;

    ssl_certificate     /etc/ssl/certs/tsdb.pem;
    ssl_certificate_key /etc/ssl/private/tsdb.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # API key validation via header
    # Clients must send: X-API-Key: <secret>
    set $expected_key "your-secret-api-key";
    if ($http_x_api_key != $expected_key) {
        return 401 '{"error": "unauthorized"}';
    }

    # Rate limiting (10 req/s per client IP)
    limit_req_zone $binary_remote_addr zone=tsdb:10m rate=10r/s;

    location / {
        limit_req zone=tsdb burst=20 nodelay;

        proxy_pass http://timestar;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Match TimeStar's internal body size limits
        client_max_body_size 64m;
    }

    # SSE streaming endpoint needs long timeouts
    location /subscribe {
        proxy_pass http://timestar;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_read_timeout 3600s;
    }
}
```

## Input Validation

TimeStar enforces the following limits and validation rules at the application layer:

| Check | Default | Config key |
|---|---|---|
| Write body size | 64 MB | `http.max_write_body_size` |
| Query body size | 1 MB | `http.max_query_body_size` |
| Series per query | 10,000 | `http.max_series_count` |
| Total points per query | 10,000,000 | `http.max_total_points` |
| Query timeout | 30 s | `http.query_timeout_seconds` |
| Regex pattern length | 512 chars | hardcoded |

Additional validation:

- **Name validation** -- Measurement names, tag keys, and field names must be non-empty and must not contain null bytes (`\0`), commas (`,`), equals signs (`=`), or spaces. Tag values follow the same rules but allow spaces.
- **Regex / ReDoS protection** -- User-supplied regex patterns in query scopes (`~pattern`) are capped at 512 characters. The function security layer also bounds regex quantifiers to prevent catastrophic backtracking.
- **JSON parsing** -- Malformed JSON bodies return HTTP 400 with a descriptive error message. No partial processing occurs on parse failure.

## Network Recommendations

- **Bind to localhost or a private interface.** TimeStar listens on all interfaces by default. Use `--address 127.0.0.1` or a private VLAN address to restrict direct access.
- **Firewall rules.** Block external access to port 8086 (or your configured port). Only the reverse proxy should reach TimeStar.
- **Never expose TimeStar directly to the internet.** Without authentication and TLS, any client can read, write, and delete data.
- **Separate data and management traffic.** If running in a multi-server cluster (Phase 6+), use a dedicated network for inter-node RPC traffic and restrict it with firewall rules.

## Environment Variable Overrides

All config values listed above can be set via environment variables (e.g., `TIMESTAR_HTTP_MAX_SERIES_COUNT=5000`). This is useful for hardening deployments without modifying config files.

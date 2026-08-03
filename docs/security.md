# Security

## Current state

TimeStar supports an optional bearer token for HTTP APIs (`server.auth_enabled`
and `server.auth_token`), but client-facing HTTP is still plaintext and has no
fine-grained roles. `/cluster/status` is intentionally public for liveness
diagnostics; cluster mutation routes such as leadership rebalance use the normal
bearer-token wrapper.

Replicated RF&gt;1 clusters require mutual TLS for both data-plane and Raft RPC by
default. Configure `cluster.tls_cert_file`, `tls_key_file`, and `tls_ca_file`;
listeners require a client certificate and outbound connections verify the CA
and the certificate DNS/IP SAN against that peer's configured address.
Plaintext replicated transport is available only through
`cluster.development_allow_insecure_transport` and must not be used in
production. The obsolete best-effort static full-replication mode remains an
explicitly quarantined demo and does not inherit these RF&gt;1 guarantees.

Cluster backup manifests require a separate 256-bit HMAC-SHA-256 key configured
through `cluster.backup_auth_key_file`. Export and restore fail closed without
it or when a manifest tag does not match. The protected owner-only key file is
never accepted over HTTP or embedded in the artifact. TSBK units remain
plaintext, so encrypted staging, KMS-backed immutable off-site storage, key
rotation/retention, and the recovery ceremony in
[backup-restore.md](backup-restore.md) are mandatory production controls.

Rotate a node certificate by issuing a replacement from the existing cluster
CA with the same DNS/IP SAN, updating that node's protected certificate/key
files, and restarting only that node. Wait for it to recover before rotating
the next node. Runtime credential hot reload is not implemented; replacing the
CA requires an explicitly planned trust-overlap ceremony and is not yet a
documented production operation.

The server is still designed to run behind a reverse proxy for client TLS,
rate-limiting, network policy, and stronger authentication/authorization.
The complete RF=3 deployment, failure-domain, resource, lifecycle, and
qualification contract is in [cluster-operations.md](cluster-operations.md).

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
- **Never expose TimeStar directly to the internet.** Client-facing TLS still
  belongs at the proxy, and disabling bearer authentication leaves data APIs
  open to any network client.
- **Separate data and management traffic.** Restrict the HTTP, data-plane, and
  Raft ports independently even when cluster mTLS is enabled.

## Environment Variable Overrides

All config values listed above can be set via environment variables (e.g., `TIMESTAR_HTTP_MAX_SERIES_COUNT=5000`). This is useful for hardening deployments without modifying config files.

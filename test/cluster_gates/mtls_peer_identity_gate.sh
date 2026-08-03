#!/bin/bash
# P1 mTLS IDENTITY GATE: use distinct per-peer IP SANs on both inter-node
# transports, reject a certificate for the wrong configured endpoint even when
# the cluster CA signed it, then recover the blocked write with the right cert.
# All credentials, roots, logs, and process temporaries stay under build/tmp.

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
command -v openssl >/dev/null || { echo "openssl is required"; exit 2; }

PORTS="19920 19921 19922"
PREFIX=1992
PEERS="127.0.0.11:19920,127.0.0.12:19921,127.0.0.13:19922"
ROOT="$BUILD_DIR/tmp/tsgate_ti"
CERTS="$ROOT/certs"
PIDS=("" "" "" "")

kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs "$ROOT" || exit 2
trap 'gate_cleanup 1992 "$ROOT"' EXIT
mkdir -p "$CERTS" "$ROOT/n1" "$ROOT/n2" "$ROOT/n3"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -sha256 \
    -subj '/CN=TimeStar test CA' -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" >/dev/null 2>&1 || exit 2
for node in 1 2 3; do
    ip="127.0.0.$((10 + node))"
    openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=$ip" \
        -keyout "$CERTS/n$node.key" -out "$CERTS/n$node.csr" >/dev/null 2>&1 || exit 2
    openssl x509 -req -in "$CERTS/n$node.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
        -CAcreateserial -days 1 -sha256 -out "$CERTS/n$node.crt" \
        -extfile <(printf 'subjectAltName=IP:%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$ip") \
        >/dev/null 2>&1 || exit 2
done

start_node() { # logical node, certificate owner (defaults to logical node)
    local node="$1" cert_node="${2:-$1}" port
    port=$((19919 + node))
    env $GATE_SERVER_ENV TMPDIR="$ROOT" TIMESTAR_DATA_DIR="$ROOT/n$node" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 \
        TIMESTAR_CLUSTER_UUID=11223344556677889900aabbccddeeff \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        TIMESTAR_CLUSTER_TLS_CERT_FILE="$CERTS/n$cert_node.crt" \
        TIMESTAR_CLUSTER_TLS_KEY_FILE="$CERTS/n$cert_node.key" \
        TIMESTAR_CLUSTER_TLS_CA_FILE="$CERTS/ca.crt" \
        "$BIN" --port "$port" --smp 1 --memory "$GATE_SERVER_MEMORY" --overprovisioned \
        >>"$ROOT/node${node}.log" 2>&1 &
    PIDS[$node]=$!
}

stop_node() {
    local node="$1" pid="${PIDS[$1]:-}"
    [ -n "$pid" ] || return
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    PIDS[$node]=""
}

write_body() { # measurement suffix
    printf '{"measurement":"tls_identity_%s","tags":{"probe":"identity"},"fields":{"value":1.0},"timestamp":%s}' \
        "$1" "$((1785713000000000000 + $1))"
}

write_code() { # body, response file
    curl -sS -m15 -o "$ROOT/$2" -w '%{http_code}' -X POST 'http://127.0.0.1:19920/write' \
        -H 'Content-Type: application/json' -d "$1" 2>/dev/null
}

for node in 1 2 3; do start_node "$node"; done
wait_all_led "$PORTS" 4096 120 || gate_exit
wait_healthy "$PORTS" 60 || gate_exit

BASELINE_BODY=$(write_body 0)
BASELINE_CODE=$(write_code "$BASELINE_BODY" baseline.json)
assert_eq "matching endpoint identities commit a replicated write" "$BASELINE_CODE" 200

echo "=== restart node 2 with node 3's otherwise trusted certificate ==="
stop_node 2
start_node 2 3
for _ in $(seq 1 60); do
    cluster_status 19921 >/dev/null && break
    sleep 1
done
sleep 5

# Roughly one third of these VShards are led by node 2. A correct per-endpoint
# SAN check makes node 1's data-plane connection to node 2 fail before the RPC;
# the write router must return bounded retryable 503, never accept the write.
FAILED_BODY=""
for i in $(seq 1 80); do
    body=$(write_body "$i")
    code=$(write_code "$body" wrong.json)
    if [ "$code" = 503 ]; then
        FAILED_BODY="$body"
        break
    fi
done
if [ -n "$FAILED_BODY" ]; then
    gate_ok "trusted certificate for the wrong endpoint blocked a real commit with 503"
else
    gate_fail "wrong endpoint certificate did not block any of 80 distributed writes"
fi

echo "=== restore node 2's endpoint certificate and retry the exact write ==="
stop_node 2
start_node 2 2
RECOVERED_CODE=000
if [ -n "$FAILED_BODY" ]; then
    for _ in $(seq 1 60); do
        RECOVERED_CODE=$(write_code "$FAILED_BODY" recovered.json)
        [ "$RECOVERED_CODE" = 200 ] && break
        sleep 1
    done
    assert_eq "correct endpoint identity restores the blocked commit" "$RECOVERED_CODE" 200
else
    gate_fail "recovery check has no blocked write to retry"
fi

gate_exit

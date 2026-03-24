#!/bin/bash
# Generates Python protobuf bindings from timestar.proto
#
# Usage:
#   cd test_api/python
#   ./generate_proto.sh
#
# Requires: protoc (Protocol Buffer Compiler)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${SCRIPT_DIR}/../../proto"
PROTO_FILE="${PROTO_DIR}/timestar.proto"

if ! command -v protoc &>/dev/null; then
    echo "ERROR: protoc not found. Install with: apt install protobuf-compiler" >&2
    exit 1
fi

if [ ! -f "$PROTO_FILE" ]; then
    echo "ERROR: Proto file not found at ${PROTO_FILE}" >&2
    exit 1
fi

echo "Generating Python protobuf bindings..."
protoc --python_out="$SCRIPT_DIR" --proto_path="$PROTO_DIR" "$PROTO_FILE"

# Verify the generated file
if [ -f "${SCRIPT_DIR}/timestar_pb2.py" ]; then
    echo "Generated: ${SCRIPT_DIR}/timestar_pb2.py"
    echo "OK"
else
    echo "ERROR: Generation failed - timestar_pb2.py not created" >&2
    exit 1
fi

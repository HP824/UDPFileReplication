#!/usr/bin/env bash
# Basic transfer test with no packet drops.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p test/roots/replica1 test/roots/replica2 test/input test/logs

if [[ ! -x bin/myserver || ! -x bin/myclient ]]; then
  echo "Building binaries..."
  make
fi

# Clean previous outputs
rm -f test/roots/replica1/uploads/sample.txt test/roots/replica2/uploads/sample.txt

echo "Starting servers (droppc=0)..."
bin/myserver 9100 0 test/roots/replica1 > test/logs/server1.log 2>&1 &
PID1=$!
bin/myserver 9101 0 test/roots/replica2 > test/logs/server2.log 2>&1 &
PID2=$!

cleanup() {
  kill "$PID1" "$PID2" 2>/dev/null || true
  wait "$PID1" "$PID2" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

echo "Running client..."
bin/myclient 2 servaddr.conf 1400 8 test/input/sample.txt uploads/sample.txt

echo ""
echo "Verifying replicas..."
cmp test/input/sample.txt test/roots/replica1/uploads/sample.txt
cmp test/input/sample.txt test/roots/replica2/uploads/sample.txt
echo "Basic test passed."

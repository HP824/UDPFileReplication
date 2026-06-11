#!/usr/bin/env bash
# Single-server transfer with packet drops (retransmission exercise).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p test/roots/replica1 test/input test/logs

if [[ ! -x bin/myserver || ! -x bin/myclient ]]; then
  echo "Building binaries..."
  make
fi

# ~8 KB file (several DATA packets at MSS 1400)
dd if=/dev/urandom of=test/input/medium.bin bs=1024 count=8 status=none 2>/dev/null \
  || dd if=/dev/urandom of=test/input/medium.bin bs=1024 count=8 2>/dev/null

rm -f test/roots/replica1/uploads/medium.bin

DROP_PCT="${1:-5}"
echo "Starting server (droppc=${DROP_PCT})..."
bin/myserver 9100 "$DROP_PCT" test/roots/replica1 > test/logs/server1_lossy.log 2>&1 &
PID1=$!

cleanup() {
  kill "$PID1" 2>/dev/null || true
  wait "$PID1" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

echo "Running client..."
bin/myclient 1 servaddr.conf 1400 8 test/input/medium.bin uploads/medium.bin

echo ""
echo "Verifying replica..."
cmp test/input/medium.bin test/roots/replica1/uploads/medium.bin
echo "Lossy test passed (droppc=${DROP_PCT})."
echo "Server log: test/logs/server1_lossy.log"

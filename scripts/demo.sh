#!/usr/bin/env bash
# End-to-end portfolio demo: multi-replica upload + lossy retransmission.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=============================================="
echo "  UDP File Replication — Demo"
echo "=============================================="
echo ""
echo "This demo runs two automated scenarios:"
echo "  1. Multi-replica upload (2 servers, no packet loss)"
echo "  2. Lossy upload (Go-Back-N retransmission under packet drops)"
echo ""

echo "----------------------------------------------"
echo "Step 1: Multi-replica transfer"
echo "----------------------------------------------"
bash scripts/run_basic_test.sh

echo ""
echo "----------------------------------------------"
echo "Step 2: Transfer under packet loss (droppc=5)"
echo "----------------------------------------------"
bash scripts/run_lossy_test.sh 5

LOSSY_LOG="test/logs/server1_lossy.log"
if [[ -f "$LOSSY_LOG" ]]; then
  DROP_DATA=$(grep -c "DROP DATA" "$LOSSY_LOG" || true)
  DROP_ACK=$(grep -c "DROP ACK" "$LOSSY_LOG" || true)
  echo ""
  echo "Loss injection summary (server log):"
  echo "  DROP DATA events: $DROP_DATA"
  echo "  DROP ACK events:  $DROP_ACK"
fi

echo ""
echo "=============================================="
echo "  Demo complete — all checks passed"
echo "=============================================="

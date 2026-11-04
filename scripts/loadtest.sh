#!/usr/bin/env bash
# Flash sale: reset stock, run k6 (Docker image, no install needed), then report from the databases.
# Services must be running (npm run dev). For the naive comparison: RESERVATION_MODE=naive npm run dev
set -euo pipefail
STOCK="${STOCK:-100}"
MODE="${MODE:-locked}"
mkdir -p loadtest/results
npx tsx scripts/seed.ts FLASH-1 "$STOCK"
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep 1
docker run --rm -i -v "$PWD/loadtest:/scripts" \
  -e BASE_URL="${BASE_URL:-http://host.docker.internal:4000}" -e BUYERS="${BUYERS:-2000}" -e VUS="${VUS:-200}" -e RUN_ID="$MODE-$(date +%s)" \
  grafana/k6:0.54.0 run --summary-export "/scripts/results/k6-$MODE.json" /scripts/flash-sale.js || true
npx tsx scripts/flash-report.ts "$STOCK" "$SINCE" "$MODE"

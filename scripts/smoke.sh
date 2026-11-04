#!/usr/bin/env bash
# End-to-end smoke test against a running stack (gateway on :4000): an order is confirmed, and a
# declined payment cancels the order and gives the stock back.
set -euo pipefail
BASE="${BASE_URL:-http://localhost:4000}"
order() { curl -sf -X POST "$BASE/orders" -H 'content-type: application/json' -H "x-customer-id: $1" -H "idempotency-key: smoke-$1-$(date +%s%N)" -d "{\"customerId\":\"$1\",\"items\":[{\"sku\":\"$2\",\"quantity\":1}]}" | sed -E 's/.*"orderId":"([^"]+)".*/\1/'; }
wait_for() { for _ in $(seq 1 60); do s=$(curl -sf "$BASE/orders/$1" | sed -E 's/.*"status":"([A-Z]+)".*/\1/'); [ "$s" = "$2" ] && return 0; sleep 1; done; echo "order $1 never reached $2 (last: $s)"; return 1; }
before=$(curl -sf "$BASE/products/LAPTOP-13" | sed -E 's/.*"available":([0-9]+).*/\1/')
ok=$(order smoke-ok MUG); wait_for "$ok" CONFIRMED && echo "✓ order confirmed"
bad=$(order declined-smoke LAPTOP-13); wait_for "$bad" CANCELLED && echo "✓ declined payment cancelled the order"
for _ in $(seq 1 30); do after=$(curl -sf "$BASE/products/LAPTOP-13" | sed -E 's/.*"available":([0-9]+).*/\1/'); [ "$after" = "$before" ] && break; sleep 1; done
[ "$after" = "$before" ] && echo "✓ stock released (compensation): $after available" || { echo "✗ stock not released ($before → $after)"; exit 1; }

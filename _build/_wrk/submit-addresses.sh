#!/bin/bash
# Submit producer requests in parallel - 10 concurrent workers
FILE="$1"
PARALLEL="${2:-10}"
TOTAL=$(wc -l < "$FILE")
echo "Submitting $TOTAL addresses with $PARALLEL parallel workers..."

submit_addr() {
  local addr=$(echo "$1" | tr -d '\r\n ')
  [ -z "$addr" ] && return
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST "https://svcs.the16oracles.io/api/trigger/producer" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: BMFer" \
    -H "X-Priority: 5" \
    -d "{\"batch\":{\"filters\":{\"addresses\":[\"${addr}\"],\"before\":{\"block_time\":\"2027-01-20T00:00:00Z\"},\"until\":{\"block_time\":\"2025-01-01T00:00:00Z\"}},\"address_sig_cnt\":10}}"
}
export -f submit_addr

cat "$FILE" | tr -d '\r' | parallel -j "$PARALLEL" submit_addr {} 2>/dev/null | sort | uniq -c | sort -rn

echo "Done"

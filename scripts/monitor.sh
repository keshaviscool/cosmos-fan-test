#!/usr/bin/env bash
# Usage: ./monitor.sh <output.csv> <duration_seconds> <interval_seconds> <container1> [container2 ...]
set -euo pipefail

OUT="$1"
DURATION="$2"
INTERVAL="$3"
shift 3
CONTAINERS=("$@")

mkdir -p "$(dirname "$OUT")"
echo "timestamp,container,cpu_perc,mem_usage,mem_perc" > "$OUT"

END=$((SECONDS + DURATION))
echo "Sampling ${CONTAINERS[*]} every ${INTERVAL}s for ${DURATION}s -> $OUT"

while [ "$SECONDS" -lt "$END" ]; do
  TS=$(date +%Y-%m-%dT%H:%M:%S)
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" "${CONTAINERS[@]}" \
    | awk -F',' -v ts="$TS" '{print ts","$0}' >> "$OUT"
  sleep "$INTERVAL"
done

echo "Done. Saved to $OUT"
#!/usr/bin/env bash
# Runs every scenario in order against the images currently built, and records
# the exit code of each. The point is provenance: one sweep, one build, so every
# number in ANALYSIS.md can be traced to the same binaries.
set -u

cd "$(dirname "$0")/.."
EVIDENCE=".evidence/scenarios"
mkdir -p "$EVIDENCE"

SUMMARY="$EVIDENCE/sweep-summary.txt"
{
  echo "sweep started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "images:"
  docker images --format '{{.Repository}}:{{.Tag}} {{.ID}} {{.CreatedAt}}' | grep '^kv-db-' | sort
  echo
} >"$SUMMARY"

for s in s1 s2 s2b s3 s4 s5 s6 reshard lag; do
  echo "=== $s ==="
  start=$(date -u +%H:%M:%S)
  bash scripts/scenario.sh "$s" >"$EVIDENCE/console-$s.txt" 2>&1
  code=$?
  echo "$s EXIT=$code  started=$start finished=$(date -u +%H:%M:%S)" | tee -a "$SUMMARY"
done

echo "sweep finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$SUMMARY"
cat "$SUMMARY"

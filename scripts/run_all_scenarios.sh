#!/usr/bin/env bash
# Official 6-scenario grading run for the distributed in-memory KV store.
#
#   ./scripts/run_all_scenarios.sh            # all six, in order
#   ./scripts/run_all_scenarios.sh s3        # one scenario only
#   ./scripts/run_all_scenarios.sh s1 s5     # any subset, in order
#
# Each scenario delegates to ./scripts/scenario.sh (docker compose stack +
# load client) and logs raw output under .evidence/scenarios/. This wrapper
# adds the banners, the pass/fail ledger and the final summary table that a
# grader (or a screen recording) wants to see.
#
# Requires: docker compose, curl, python. On native Windows without docker,
# the same behaviours are covered by:
#   go test -tags integration ./test/      (dockerised behaviours, in-process)
#   go run ./cmd/scenarios -scenario s1    (data-plane smoke + RPS, any OS)
#
# Mapping to the homework (section 10):
#   s1  10.1  single node: Set/Get/Delete + baseline RPS
#   s2  10.2  multi-node & multi-shard: RPS scaling
#   s3  10.3  stop a master container: failover, traffic continues
#   s4  10.4  sequential rolling restarts: data preserved
#   s5  10.5  add a node under active load: rebalanced, zero drops
#   s6  10.6  raise/lower replica count live, then survive a kill

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

ALL_SCENARIOS="s1 s2 s3 s4 s5 s6"
WANT="${*:-$ALL_SCENARIOS}"

declare -A DESCRIBE=(
  [s1]="Scenario 1 (10.1): single node up, Set/Get/Delete, baseline RPS"
  [s2]="Scenario 2 (10.2): multi-node multi-shard, RPS scaling"
  [s3]="Scenario 3 (10.3): kill a master container, automatic failover"
  [s4]="Scenario 4 (10.4): sequential rolling restarts, data preserved"
  [s5]="Scenario 5 (10.5): add a node under active load, live rebalance"
  [s6]="Scenario 6 (10.6): replica count up/down live, survive a kill"
)

for s in $WANT; do
  if [ -z "${DESCRIBE[$s]:-}" ]; then
    echo "unknown scenario '$s' (want one of: $ALL_SCENARIOS)" >&2
    exit 2
  fi
done

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }

pass=0
fail=0
declare -a RESULTS=()

banner() {
  echo
  echo "======================================================================"
  echo " $1"
  echo "======================================================================"
}

for s in $WANT; do
  banner "${DESCRIBE[$s]}"
  echo "STARTED: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if ./scripts/scenario.sh "$s"; then
    echo "RESULT: $s PASS"
    RESULTS+=("$s PASS")
    pass=$((pass + 1))
  else
    echo "RESULT: $s FAIL (exit $?) - raw logs under .evidence/scenarios/$s.*"
    RESULTS+=("$s FAIL")
    fail=$((fail + 1))
  fi
done

banner "GRADING SUMMARY: $pass passed, $fail failed"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "Evidence: .evidence/scenarios/"

if [ "$fail" -gt 0 ]; then
  exit 1
fi

#!/usr/bin/env bash
# Measurement harness for the six spec test scenarios (section 10), plus the
# re-shard and replication-lag measurements.
#
#   ./scripts/scenario.sh <name>
#
# Every run writes its raw output under .evidence/scenarios/. Nothing in this
# script summarises or interprets: it captures what happened so ANALYSIS.md can
# quote it. If a step fails, the failure is what gets recorded.
#
# Requires: docker compose, curl, python (for JSON summaries).

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

EVIDENCE="$REPO_ROOT/.evidence/scenarios"
mkdir -p "$EVIDENCE"

CTRL="http://localhost:8000"
LB="http://localhost:8080"

log() { echo "[harness] $*"; }

# --- primitives -------------------------------------------------------------

compose() { docker compose "$@"; }

reset_stack() {
  log "tearing the stack down (including volumes)"
  compose --profile demo down -v >/dev/null 2>&1
}

# wait_http <url> <timeout-seconds>
wait_http() {
  local url="$1" timeout="${2:-60}" i=0
  while [ "$i" -lt "$timeout" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
    i=$((i + 1))
  done
  echo "TIMEOUT waiting for $url after ${timeout}s" >&2
  return 1
}

# wait_nodes <count> <timeout-seconds> - block until the controller sees N healthy nodes
wait_nodes() {
  local want="$1" timeout="${2:-60}" i=0 got
  while [ "$i" -lt "$timeout" ]; do
    got=$(curl -fsS "$CTRL/nodes" 2>/dev/null |
      python -c "import sys,json;d=json.load(sys.stdin);print(sum(1 for n in d.get('nodes',[]) if n.get('status') in ('healthy',1)))" 2>/dev/null)
    if [ "${got:-0}" -ge "$want" ]; then return 0; fi
    sleep 1
    i=$((i + 1))
  done
  echo "TIMEOUT: controller sees ${got:-0} healthy nodes, wanted $want" >&2
  return 1
}

# wait_balanced <timeout-seconds> - block until the rebalancer has nothing left to do
wait_balanced() {
  local timeout="${1:-60}" i=0 pending
  while [ "$i" -lt "$timeout" ]; do
    pending=$(curl -fsS "$CTRL/rebalance/status" 2>/dev/null |
      python -c "import sys,json;d=json.load(sys.stdin);print(len(d.get('pending_plan') or []) + (1 if d.get('running') else 0))" 2>/dev/null)
    if [ "${pending:-1}" -eq 0 ]; then return 0; fi
    sleep 1
    i=$((i + 1))
  done
  echo "NOTE: rebalance still has ${pending:-?} pending after ${timeout}s" >&2
  return 0
}

# map_summary <label> - append the current ownership picture to the evidence file
map_summary() {
  local label="$1" out="$EVIDENCE/$SCENARIO.map.txt"
  {
    echo "--- $label ---"
    curl -fsS "$CTRL/map" | python -c "
import sys, json, collections
d = json.load(sys.stdin)
print('num_partitions=%s version=%s reshard=%s' % (d['num_partitions'], d['version'], d['reshard_state']))
m = collections.Counter(p['master_addr'] for p in d['partitions'])
r = collections.Counter(x for p in d['partitions'] for x in p['replicas'])
print('masters :', dict(sorted(m.items())))
print('replicas:', dict(sorted(r.items())))
"
    echo
  } >>"$out" 2>&1
}

# run_client <tag> <extra args...>
run_client() {
  local tag="$1"
  shift
  local out="$EVIDENCE/$SCENARIO.client-$tag.txt"
  log "client run '$tag': $*"
  {
    echo "COMMAND: docker compose --profile demo run --rm --no-deps client $*"
    echo "STARTED: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
  } >"$out"
  compose --profile demo run --rm --no-deps client "$@" >>"$out" 2>&1
  local rc=$?
  echo "" >>"$out"
  echo "EXIT CODE: $rc" >>"$out"
  grep -E "^(elapsed|throughput|latency|get|set|delete|errors):" "$out" || tail -5 "$out"
  return $rc
}

# sample_stats <tag> <samples> - docker stats, one line per container per sample
sample_stats() {
  local tag="$1"
  local samples="${2:-10}"
  local out="$EVIDENCE/$SCENARIO.stats-$tag.txt"
  : >"$out"
  for _ in $(seq 1 "$samples"); do
    {
      date -u +%Y-%m-%dT%H:%M:%SZ
      docker stats --no-stream --format "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
      echo
    } >>"$out" 2>&1
    sleep 2
  done
}

# sample_replication_lag <tag> <samples>
sample_replication_lag() {
  local tag="$1"
  local samples="${2:-10}"
  local out="$EVIDENCE/$SCENARIO.repl-lag-$tag.txt"
  : >"$out"
  for _ in $(seq 1 "$samples"); do
    {
      printf '%s ' "$(date -u +%H:%M:%S)"
      for port in 8001 8002 8003 8004; do
        q=$(curl -fsS --max-time 2 "http://localhost:$port/health" 2>/dev/null |
          python -c "import sys,json;d=json.load(sys.stdin);print(d['details']['replication_queue'])" 2>/dev/null)
        printf 'node@%s=%s ' "$port" "${q:-n/a}"
      done
      echo
    } >>"$out" 2>&1
    sleep 1
  done
}

# cgroup_limits - what the kernel actually enforces, read from inside each
# container. docker stats reports CPU as a percentage of one core and overshoots
# on short samples; cpu.max and cpu.stat are the authority.
cgroup_limits() {
  local out="$EVIDENCE/$SCENARIO.limits.txt"
  export MSYS_NO_PATHCONV=1
  {
    echo "\$ docker inspect --format '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}'"
    docker inspect --format '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}'       $(docker compose ps -q) 2>&1
    echo
    echo "=== cgroup limits and CPU throttling, read inside each container ==="
    echo "(cpu.max is 'quota period' in microseconds: 100000 100000 == exactly 1.0 CPU)"
    for c in $(docker compose ps --format '{{.Name}}'); do
      echo "--- $c ---"
      printf 'cpu.max: '
      docker exec "$c" cat /sys/fs/cgroup/cpu.max 2>&1
      printf 'memory.max: '
      docker exec "$c" cat /sys/fs/cgroup/memory.max 2>&1
      docker exec "$c" cat /sys/fs/cgroup/cpu.stat 2>&1 | grep -E "usage_usec|nr_periods|nr_throttled|throttled_usec"
      printf 'nproc (what Go uses for GOMAXPROCS): '
      docker exec "$c" nproc 2>&1
    done
  } >>"$out" 2>&1
  tail -40 "$out"
}

# lb_error_timeline <tag> - one line per second, counting the load balancer's
# non-2xx responses. This is what turns "24 requests failed" into "they failed
# in this 3-second window", which is the only form that says anything about
# failover.
lb_error_timeline() {
  local tag="$1"
  local out="$EVIDENCE/$SCENARIO.lb-timeline-$tag.txt"
  {
    echo "status codes returned by the load balancer on /kv/, per second"
    docker compose logs loadbalancer --tail 200000 --no-log-prefix 2>/dev/null |
      grep -oE "^[0-9/]+ [0-9:]+ \[http\] (GET|PUT|POST|DELETE) /kv/[^ ]+ [0-9]{3}" |
      awk '{print $2, $NF}' |
      sort | uniq -c |
      awk '{print $2, $3, $1}'
  } >"$out" 2>&1
  echo "--- non-2xx only ---"
  grep -vE " (200|404) " "$out" | head -40
}

# lb_throughput_timeline <tag> - requests per second through the load balancer,
# taken from its own access log. The client only reports one throughput number
# for the whole run, which is exactly the wrong shape for a question like "how
# far did throughput dip during the reshard, and for how long". This gives the
# per-second series that answers it.
lb_throughput_timeline() {
  local tag="$1"
  local out="$EVIDENCE/$SCENARIO.lb-rps-$tag.txt"
  {
    echo "requests per second on /kv/ through the load balancer (all status codes)"
    docker compose logs loadbalancer --tail 200000 --no-log-prefix 2>/dev/null |
      grep -oE "^[0-9/]+ [0-9:]+ \[http\] (GET|PUT|POST|DELETE) /kv/" |
      awk '{print $2}' |
      sort | uniq -c |
      awk '{print $2, $1}'
  } >"$out" 2>&1
  echo "--- requests/second (min/median/max over the run) ---"
  tail -n +2 "$out" | awk '{print $2}' | sort -n | awk '
    { v[NR] = $1 }
    END {
      if (NR == 0) { print "(no requests logged)"; exit }
      print "samples=" NR " min=" v[1] " median=" v[int((NR+1)/2)] " max=" v[NR]
    }'
}

# controller_log <tag> <grep-pattern>
controller_log() {
  local tag="$1"
  local pattern="$2"
  local out="$EVIDENCE/$SCENARIO.controller-$tag.txt"
  docker compose logs controller --tail 200000 --no-log-prefix 2>/dev/null |
    grep -E "$pattern" >"$out"
  echo "--- controller log ($tag) ---"
  cat "$out"
}

# read_key <key> - echo the decoded value, or nothing if the key is absent.
# The node returns Value as []byte, which JSON-encodes to base64.
read_key() {
  curl -fsS --max-time 3 "$LB/kv/$1" 2>/dev/null | python -c "
import sys, json, base64
try:
    v = json.load(sys.stdin).get('value')
except Exception:
    sys.exit(0)
if v:
    sys.stdout.write(base64.b64decode(v).decode('utf-8', 'replace'))
" 2>/dev/null
}

kv_smoke() {
  local out="$EVIDENCE/$SCENARIO.smoke.txt"
  {
    echo "=== Set/Get/Delete through the load balancer ==="
    echo "\$ curl -X PUT $LB/kv/smoke-key -d 'hello'"
    curl -isS -X PUT "$LB/kv/smoke-key" -d 'hello' | head -1
    echo "\$ curl $LB/kv/smoke-key"
    curl -isS "$LB/kv/smoke-key" | tail -1
    echo ""
    echo "\$ curl -X DELETE $LB/kv/smoke-key"
    curl -isS -X DELETE "$LB/kv/smoke-key" | head -1
    echo "\$ curl $LB/kv/smoke-key   (expect 404)"
    curl -isS "$LB/kv/smoke-key" | head -1
  } >"$out" 2>&1
  cat "$out"
}

# --- scenarios --------------------------------------------------------------

scenario_s1_single_node() {
  reset_stack
  log "bringing up controller + loadbalancer + one database node"
  compose up -d controller loadbalancer dbnode1 >/dev/null 2>&1
  wait_http "$CTRL/health" 60 || return 1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 1 60 || return 1
  map_summary "one node"

  kv_smoke
  run_client "1node-mixed" -addr http://loadbalancer:8080 -duration 20s -concurrency 8 -keyspace 1000 -value-bytes 128 -read 0.5 -delete 0.1
}

scenario_s2_scaling() {
  # Throughput as nodes are added. Every step follows the same procedure -
  # seed, then three 20s measurement runs - because a single run on a laptop
  # varies by 20-30% and one sample cannot support a scaling claim.
  reset_stack
  log "1 node"
  compose up -d controller loadbalancer dbnode1 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 1 60 || return 1
  map_summary "1 node"
  run_client "n1-seed" -addr http://loadbalancer:8080 -ops 5000 -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.0
  for rep in 1 2 3; do
    run_client "n1-rep$rep" -addr http://loadbalancer:8080 -duration 20s -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.5
  done

  log "2 nodes"
  compose up -d dbnode2 >/dev/null 2>&1
  wait_nodes 2 60 || return 1
  wait_balanced 60
  map_summary "2 nodes"
  run_client "n2-seed" -addr http://loadbalancer:8080 -ops 5000 -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.0
  for rep in 1 2 3; do
    run_client "n2-rep$rep" -addr http://loadbalancer:8080 -duration 20s -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.5
  done

  log "4 nodes"
  compose up -d dbnode3 >/dev/null 2>&1
  compose --profile demo up -d dbnode4 >/dev/null 2>&1
  wait_nodes 4 90 || return 1
  wait_balanced 90
  map_summary "4 nodes"
  run_client "n4-seed" -addr http://loadbalancer:8080 -ops 5000 -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.0
  for rep in 1 2 3; do
    run_client "n4-rep$rep" -addr http://loadbalancer:8080 -duration 20s -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.5
  done

  log "resource usage at 4 nodes under load (verifies the compose cpu/memory limits)"
  compose --profile demo run --rm --no-deps client -addr http://loadbalancer:8080     -duration 30s -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.5     >"$EVIDENCE/$SCENARIO.client-n4-load-for-stats.txt" 2>&1 &
  local client_pid=$!
  sleep 5
  sample_stats "under-load" 10
  wait $client_pid

  log "declared limits, straight from the running containers"
  {
    echo "\$ docker inspect --format '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}' <containers>"
    docker inspect --format '{{.Name}} NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}'       kvdb-controller kvdb-loadbalancer kvdb-dbnode1 kvdb-dbnode2 kvdb-dbnode3 kvdb-dbnode4 2>&1
  } >"$EVIDENCE/$SCENARIO.limits.txt"
  cgroup_limits
}

scenario_s2b_replica_reads() {
  # Read throughput with the two strategies spec 2.3 allows: master-only, and
  # master+replicas. The two are alternated three times rather than run once
  # each, because a single pair cannot separate the strategy from the order it
  # ran in - and on this host the run-to-run spread is large enough to reverse
  # the answer.
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "3 nodes, replication factor 2"

  log "seeding the keyspace so reads hit data"
  run_client "seed" -addr http://loadbalancer:8080 -ops 5000 -concurrency 16 -keyspace 5000 -value-bytes 128 -read 0.0
  sleep 3

  : >"$EVIDENCE/$SCENARIO.lb-config.txt"
  for rep in 1 2 3; do
    for mode in false true; do
      log "load balancer restart with LB_READ_FROM_REPLICAS=$mode (rep $rep)"
      LB_READ_FROM_REPLICAS="$mode" compose up -d --force-recreate loadbalancer >/dev/null 2>&1
      wait_http "$LB/health" 60 || return 1
      sleep 3
      {
        echo "--- rep $rep, LB_READ_FROM_REPLICAS=$mode ---"
        compose logs loadbalancer --tail 20 2>&1 | grep "read strategy"
      } >>"$EVIDENCE/$SCENARIO.lb-config.txt"

      local tag="reads-master-only-rep$rep"
      if [ "$mode" = "true" ]; then tag="reads-with-replicas-rep$rep"; fi
      run_client "$tag" -addr http://loadbalancer:8080 -duration 20s -concurrency 16 -keyspace 5000 -read 1.0
    done
  done

  cgroup_limits
}

scenario_s3_node_failure() {
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "before the kill"

  log "starting load, then stopping dbnode2 mid-run"
  run_client "during-failover" -addr http://loadbalancer:8080 -duration 40s -concurrency 8 -keyspace 2000 -read 0.5 &
  local client_pid=$!

  sleep 12
  {
    echo "STOPPED AT: $(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    docker compose stop dbnode2 2>&1
  } >"$EVIDENCE/$SCENARIO.kill.txt"
  log "dbnode2 stopped; watching the map converge"

  {
    for i in $(seq 1 30); do
      printf '%s t+%ss ' "$(date -u +%H:%M:%S)" "$i"
      curl -fsS --max-time 2 "$CTRL/map" 2>/dev/null | python -c "
import sys, json, collections
d = json.load(sys.stdin)
m = collections.Counter(p['master_addr'] for p in d['partitions'])
print('version=%s masters=%s' % (d['version'], dict(sorted(m.items()))))
" 2>/dev/null || echo "(map unavailable)"
      sleep 1
    done
  } >"$EVIDENCE/$SCENARIO.convergence.txt" 2>&1

  wait $client_pid
  map_summary "after failover"
  lb_error_timeline "failover"
  kv_smoke
}

scenario_s4_rolling_restart() {
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "before the rolling restart"

  log "writing a known dataset"
  local out="$EVIDENCE/$SCENARIO.dataset.txt"
  : >"$out"
  for i in $(seq 1 60); do
    curl -fsS -X PUT "$LB/kv/roll-$i" -d "value-$i" >/dev/null 2>&1 || echo "write roll-$i FAILED" >>"$out"
  done
  echo "wrote 60 keys" >>"$out"
  sleep 2

  for n in 1 2 3; do
    log "restarting dbnode$n"
    {
      echo "=== restart dbnode$n at $(date -u +%H:%M:%S) ==="
      compose restart "dbnode$n" 2>&1
    } >>"$EVIDENCE/$SCENARIO.restarts.txt"
    sleep 8
    wait_nodes 3 60
    map_summary "after restarting dbnode$n"

    local missing=0 bad=0
    for i in $(seq 1 60); do
      got=$(read_key "roll-$i")
      if [ -z "$got" ]; then
        missing=$((missing + 1))
      elif [ "$got" != "value-$i" ]; then
        bad=$((bad + 1))
      fi
    done
    echo "after restarting dbnode$n: missing=$missing wrong=$bad of 60" >>"$out"
    log "after restarting dbnode$n: missing=$missing wrong=$bad of 60"
  done

  cat "$out"
  controller_log "rejoin" "\[rejoin\]"
  kv_smoke
}

scenario_s5_add_node_under_load() {
  reset_stack
  log "starting with two nodes"
  compose up -d controller loadbalancer dbnode1 dbnode2 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 2 60 || return 1
  wait_balanced 60
  map_summary "two nodes, before the join"

  log "load on, then adding dbnode3 mid-run"
  run_client "during-join" -addr http://loadbalancer:8080 -duration 40s -concurrency 8 -keyspace 2000 -read 0.5 &
  local client_pid=$!

  sleep 12
  {
    echo "JOINED AT: $(date -u +%H:%M:%S)"
    compose up -d dbnode3 2>&1
  } >"$EVIDENCE/$SCENARIO.join.txt"

  {
    for i in $(seq 1 30); do
      printf '%s t+%ss ' "$(date -u +%H:%M:%S)" "$i"
      curl -fsS --max-time 2 "$CTRL/rebalance/status" 2>/dev/null | python -c "
import sys, json
d = json.load(sys.stdin)
print('running=%s total_runs=%s last_moves=%s pending=%d ownership=%s' % (
    d.get('running'), d.get('total_runs'), d.get('last_moves'),
    len(d.get('pending_plan') or []), d.get('master_ownership')))
" 2>/dev/null || echo "(status unavailable)"
      sleep 1
    done
  } >"$EVIDENCE/$SCENARIO.rebalance-progress.txt" 2>&1

  wait $client_pid
  map_summary "three nodes, after the rebalance"
  lb_error_timeline "join"
  kv_smoke
}

# rf_read_pass reads the 40 scenario-6 keys once and records, per failure, the
# HTTP status the load balancer returned - 404 is "the cluster says this key
# does not exist" and 502 is "the route still points at something that is not
# answering". Collapsing both into "missing" hides which one happened.
rf_read_pass() {
  local out="$1"
  local label="$2"
  local missing=0
  local bad=0
  local detail=""
  local i
  local got
  local code
  for i in $(seq 1 40); do
    got=$(read_key "rf-$i")
    if [ -z "$got" ]; then
      code=$(curl -s -o /dev/null -w '%{http_code}' "$LB/kv/rf-$i")
      detail="$detail rf-$i:$code"
      missing=$((missing + 1))
    elif [ "$got" != "value-$i" ]; then
      detail="$detail rf-$i:wrong"
      bad=$((bad + 1))
    fi
  done
  echo "$label: missing=$missing wrong=$bad of 40${detail:+ ->$detail}" >>"$out"
  log "$label: missing=$missing wrong=$bad of 40${detail:+ ->$detail}"
}

scenario_s6_replica_count() {
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "replication factor 2 (compose default)"

  local out="$EVIDENCE/$SCENARIO.rf.txt"
  : >"$out"

  log "writing a known dataset"
  for i in $(seq 1 40); do
    curl -fsS -X PUT "$LB/kv/rf-$i" -d "value-$i" >/dev/null 2>&1 || echo "write rf-$i FAILED" >>"$out"
  done
  sleep 2

  for rf in 3 1 2; do
    log "setting replication factor to $rf"
    {
      echo "=== POST /replication/factor {\"replication_factor\": $rf} ==="
      curl -isS -X POST "$CTRL/replication/factor" \
        -H 'Content-Type: application/json' \
        -d "{\"replication_factor\": $rf}" | head -20
      echo
    } >>"$out" 2>&1
    sleep 5
    map_summary "replication factor $rf"
  done

  log "back at replication factor 2: killing a node and checking the data survives"
  {
    echo "=== stopping dbnode3 ==="
    compose stop dbnode3 2>&1
  } >>"$out"
  sleep 12

  # Two passes, deliberately. Failover detection was measured in scenario 3 at
  # ~12-14s after the kill (heartbeat 5s, dead timeout 15s), so the first pass
  # lands inside the convergence window and shows what a client actually sees
  # there. The second pass runs well past it. A key missing in pass 1 and
  # present in pass 2 is the failover window; a key missing in BOTH is real
  # data loss, and only the second number says anything about replication.
  rf_read_pass "$out" "pass 1 (t+12s, inside the failover window)"
  sleep 20
  rf_read_pass "$out" "pass 2 (t+32s, after convergence)"
  map_summary "after killing dbnode3"
  controller_log "rf" "replication-factor|failover|rejoin"
  cat "$out"
}

scenario_reshard() {
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "16 partitions, before the reshard"

  log "seeding"
  run_client "seed" -addr http://loadbalancer:8080 -ops 4000 -concurrency 16 -keyspace 4000 -value-bytes 128 -read 0.0

  # The load generator picks keys at random, so 4000 random writes over a 4000
  # key space leave roughly a third of it never written - which shows up later
  # as read misses that have nothing to do with the reshard. These 60 keys are
  # written explicitly so there is a set whose presence after the reshard is a
  # statement about the reshard and not about the generator.
  log "writing 60 known keys before the reshard"
  local known="$EVIDENCE/$SCENARIO.known-keys.txt"
  : >"$known"
  for i in $(seq 1 60); do
    curl -fsS -X PUT "$LB/kv/shard-$i" -d "value-$i" >/dev/null 2>&1 || echo "write shard-$i FAILED" >>"$known"
  done
  echo "wrote 60 known keys before the reshard" >>"$known"
  sleep 2

  log "load on, then growing 16 -> 32 partitions mid-run"
  run_client "during-reshard" -addr http://loadbalancer:8080 -duration 40s -concurrency 8 -keyspace 4000 -read 0.8 &
  local client_pid=$!

  sleep 10
  {
    echo "STARTED AT: $(date -u +%H:%M:%S.%N)"
    curl -isS -X POST "$CTRL/reshard" -H 'Content-Type: application/json' -d '{"new_num_partitions": 32}' | head -20
  } >"$EVIDENCE/$SCENARIO.start.txt" 2>&1

  {
    for i in $(seq 1 40); do
      printf '%s t+%ss ' "$(date -u +%H:%M:%S)" "$i"
      curl -fsS --max-time 2 "$CTRL/reshard/status" 2>/dev/null | python -c "
import sys, json
d = json.load(sys.stdin)
states = {}
for r in d.get('ranges') or []:
    states[r['state']] = states.get(r['state'], 0) + 1
print('state=%s running=%s ranges=%s/%s by_state=%s duration_ms=%s err=%s' % (
    d.get('reshard_state'), d.get('running'), d.get('ranges_done'),
    d.get('ranges_total'), states, d.get('duration_ms'), d.get('last_error', '')))
" 2>/dev/null || echo "(status unavailable)"
      sleep 1
    done
  } >"$EVIDENCE/$SCENARIO.progress.txt" 2>&1

  wait $client_pid
  map_summary "after the reshard"

  local missing=0
  local bad=0
  local got
  for i in $(seq 1 60); do
    got=$(read_key "shard-$i")
    if [ -z "$got" ]; then
      missing=$((missing + 1))
      echo "shard-$i missing" >>"$known"
    elif [ "$got" != "value-$i" ]; then
      bad=$((bad + 1))
      echo "shard-$i wrong: $got" >>"$known"
    fi
  done
  echo "after the reshard: missing=$missing wrong=$bad of 60" >>"$known"
  log "known keys after the reshard: missing=$missing wrong=$bad of 60"

  lb_error_timeline "reshard"
  lb_throughput_timeline "reshard"
  controller_log "reshard" "reshard|migrat"
  kv_smoke
}

scenario_replication_lag() {
  reset_stack
  compose up -d controller loadbalancer dbnode1 dbnode2 dbnode3 >/dev/null 2>&1
  wait_http "$LB/health" 60 || return 1
  wait_nodes 3 90 || return 1
  wait_balanced 90
  map_summary "3 nodes, replication factor 2"

  log "write-only load; sampling each node's replication queue every second"
  run_client "write-only" -addr http://loadbalancer:8080 -duration 30s -concurrency 16 -keyspace 20000 -value-bytes 512 -read 0.0 &
  local client_pid=$!
  sleep 3
  sample_replication_lag "under-write-load" 24
  wait $client_pid

  log "queue depth after the load stops"
  sample_replication_lag "after-load" 5
}

# --- dispatch ---------------------------------------------------------------

SCENARIO="${1:-}"

# Each run replaces its own evidence rather than appending to the last run's,
# so a file in .evidence/scenarios always belongs to exactly one execution.
if [ -n "$SCENARIO" ]; then
  rm -f "$EVIDENCE/$SCENARIO."*
fi

case "$SCENARIO" in
s1) SCENARIO="s1"; scenario_s1_single_node ;;
s2) SCENARIO="s2"; scenario_s2_scaling ;;
s2b) SCENARIO="s2b"; scenario_s2b_replica_reads ;;
s3) SCENARIO="s3"; scenario_s3_node_failure ;;
s4) SCENARIO="s4"; scenario_s4_rolling_restart ;;
s5) SCENARIO="s5"; scenario_s5_add_node_under_load ;;
s6) SCENARIO="s6"; scenario_s6_replica_count ;;
reshard) SCENARIO="reshard"; scenario_reshard ;;
lag) SCENARIO="lag"; scenario_replication_lag ;;
*)
  cat <<'USAGE'
usage: ./scripts/scenario.sh <name>

  s1       spec 10.1  one node: Set/Get/Delete plus a baseline RPS
  s2       spec 10.2  RPS with 1, 2 and 4 nodes, plus docker stats under load
  s2b      spec 10.2  read RPS: master-only versus master+replicas
  s3       spec 10.3  stop a node under load; failover and convergence
  s4       spec 10.4  restart the nodes one after another; data preserved?
  s5       spec 10.5  add a node under load; controller rebalances
  s6       spec 10.6  raise and lower the replication factor; survive a kill
  reshard  spec 13    grow 16 -> 32 partitions under load
  lag                 replication queue depth under write load

Raw output lands in .evidence/scenarios/.
USAGE
  exit 2
  ;;
esac

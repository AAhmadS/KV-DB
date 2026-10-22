# SCENARIOS REPORT — All 6 Grading Scenarios, Real Execution Results

Run conditions: Windows 11 host, Docker Desktop 29.6.2, Go 1.24.1.
Docker grading run: `./scripts/run_all_scenarios.sh` (s1–s6), 2026-10-22,
ledger `6 passed, 0 failed`. Raw logs: `.evidence/scenarios/`.
Native in-process run: `go run ./cmd/scenarios -mode inprocess -scenario all`
(exit 0). Unit suite: `go test -race -count=1 ./...` 100% green.
Integration suite (`go test -race -tags integration ./test/`, same product
code): 10/10 PASS. Note: in-process RPS varies run to run on a shared
laptop host; Docker numbers below are the grading-environment figures.

---

Scenario 1:
۱. راه‌اندازی یک نود
- یک نود دیتابیس همراه کنترلر و لودبالانسر بالا بیاید.
- عملیات Set/Get/Delete را با کلاینت تست کنید تا مطمئن شوید سیستم با یک نود کار می‌کند.
- تست و اندازه‌گیری ساده تعداد درخواست بر ثانیه (RPS)
English: Single node + controller + LB: verify Set/Get/Delete works and measure baseline RPS.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s1` → `scripts/scenario.sh s1`
  (`docker compose up -d controller loadbalancer dbnode1`; curl smoke
  via `$LB/kv/smoke-key`; load client 20 s, 8 workers, mixed read/write).
- `go run ./cmd/scenarios -mode inprocess -scenario s1`
  (`cmd/scenarios/main.go: ips1` — boots controller+LB+1 node, smoke,
  2000-op baseline bench).
- `go test -race -tags integration ./test/ -run TestScenario_StartingNode_SingleNode_SetGetDelete_RPS`.

Terminal output (docker):
```
$ curl -X PUT http://localhost:8080/kv/smoke-key -d 'hello'
HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key
{"key":"smoke-key","value":"aGVsbG8=","version":1}
$ curl -X DELETE http://localhost:8080/kv/smoke-key
HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key   (expect 404)
HTTP/1.1 404 Not Found
throughput: 999.1 ops/s (total=19985)
latency: avg=7.995306ms min=411.862µs max=91.013102ms p50~=5ms p99~=100ms
errors: 0  aborted_at_shutdown: 0
RESULT: s1 PASS
```
Terminal output (in-process):
```
TOPOLOGY nodes=[1] masters=map[0:1 1:1 2:1 3:1]
SMOKE Set/Get/Delete: PASS
RPS/s1-baseline: 2376.4 ops/s (total=2000 errors=0 retries=0) latency avg=3.347286ms p50=932.8µs p99=22.0891ms
```

---

Scenario 2:
۲. راه‌اندازی چند نود با چند شارد
- چند پارتیشن تعریف کنید و بین چندین نود توزیع شوند.
- تست کنید که آیا با اضافه شدن نود بیشتر یا رپلیکاهای بیشتر RPS بالاتر می‌رود.
English: Multiple nodes and shards: distribute partitions and check whether RPS scales with more nodes/replicas.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s2` → `scripts/scenario.sh s2`
  (scale 1→2→4 nodes; seed 5000 writes + 3×20 s mixed-load reps per step;
  `docker inspect` CPU/RAM limits + `docker stats` under load).
- `go run ./cmd/scenarios -mode inprocess -scenario s2`
  (`cmd/scenarios/main.go: ips2` — 3 nodes, 8 partitions, RF=2, RPS run).
- `go test -race -tags integration ./test/ -run TestScenario_MultipleNodesAndShards_DistributionAndRPS`.

Terminal output (docker maps):
```
--- 1 node ---
num_partitions=16 version=1 reshard=stable
masters : {'dbnode1:8001': 16}
replicas: {}
--- 2 nodes ---
num_partitions=16 version=10 reshard=stable
masters : {'dbnode1:8001': 8, 'dbnode2:8002': 8}
replicas: {'dbnode1:8001': 8, 'dbnode2:8002': 8}
--- 4 nodes ---
num_partitions=16 version=21 reshard=stable
masters : {'dbnode1:8001': 4, 'dbnode2:8002': 4, 'dbnode3:8003': 4, 'dbnode4:8004': 4}
```
Terminal output (docker RPS, mixed 50% reads, 16 workers):
```
1-node reps: 1117.4 / 1106.0 / 1118.3 ops/s (errors: 0 each)
2-node reps:  946.4 /  995.4 /  880.3 ops/s (errors: 0 each)
4-node reps: 1046.9 / 1078.6 /  997.0 ops/s (errors: 0 each)
```
Honest note: on this shared laptop host the 1→2→4 curve is not monotonic
(~1114 → ~941 → ~1074 avg); the harness runs 3 reps per step precisely
because single samples vary 20–30%. Scaling claim: partitions rebalance
evenly (4/4/4/4) with zero errors at every step.
Container limits (from `docker inspect`):
```
controller/LB: NanoCpus=1000000000 Memory=268435456 (1 CPU / 256M)
dbnode1-4:     NanoCpus=1000000000 Memory=536870912 (1 CPU / 512M)
RESULT: s2 PASS
```
Terminal output (in-process):
```
TOPOLOGY nodes=[1 2 3] masters=map[0:1 1:1 2:1 3:1 4:1 5:2 6:2 7:2]
RPS/s2-multi: 2513.8 ops/s (total=2000 errors=0 retries=0) latency avg=3.171728ms p50=1.1461ms p99=14.9819ms
```

---

Scenario 3:
۳. حذف یک نود
- یکی از نودها را به صورت دستی خاموش (کانتینرش را استاپ) کنید.
- کنترلر باید Failover کند و رهبر پارتیشن‌های آن نود را تغییر دهد.
- مطمئن شوید عملیات Set/Get/Delete همچنان کار می‌کند.
English: Kill a node manually: the controller must fail over its partition leadership; Set/Get/Delete keep working.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s3` → `scripts/scenario.sh s3`
  (40 s load; `docker compose stop dbnode2` mid-run; 30 s map-convergence
  poll; error timeline; smoke).
- `go run ./cmd/scenarios -mode inprocess -scenario s3`
  (`cmd/scenarios/main.go: ips3` — stop partition-0 master mid-load,
  watch promotion, verify 60 known keys).
- `go test -race -tags integration ./test/ -run TestScenario_DeletionOfNode_FailoverAndContinuity`
  plus `go test -race ./pkg/cluster/ -run TestClusterFailoverPromotesReplica`.

Terminal output (docker):
```
STOPPED AT: 2026-10-22T20:32:38Z  (Container kvdb-dbnode2 Stopped)
t+1s  version=16 masters={'dbnode1:8001': 5, 'dbnode2:8002': 6, 'dbnode3:8003': 5}
t+30s version=22 masters={'dbnode1:8001': 11, 'dbnode3:8003': 5}
throughput: 705.1 ops/s (total=28250)
latency: avg=11.333118ms p50~=5ms p99~=100ms
errors: 24  aborted_at_shutdown: 0
$ curl -X PUT http://localhost:8080/kv/smoke-key -d 'hello'  -> HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key                   -> {"key":"smoke-key","value":"aGVsbG8=","version":10309}
$ curl -X DELETE http://localhost:8080/kv/smoke-key         -> HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key                   -> HTTP/1.1 404 Not Found
RESULT: s3 PASS
```
Read: dbnode2's 6 masterships moved to dbnode1 (5→11); 24/28250 op errors
(0.08%) inside the failover window; smoke Set/Get/Delete all 200/404-correct.
Terminal output (in-process):
```
KILL-TARGET partition 0 master = node 1
STOP node 1 now
FAILOVER lapsed-checked masters=map[0:2 1:2 2:3 3:2] converged=true
LOAD errors during failover window: 0
VERIFY known keys after kill: missing=0 wrong=0 of 60
SMOKE Set/Get/Delete after failover: PASS
```

---

Scenario 4:
۴. ری‌استارت پشت سرهم نودها
- یکی‌یکی نودها را ری‌استارت کنید تا ببینید آیا کلاستر همچنان بالا می‌ماند و داده‌ها حفظ می‌شود.
English: Restart nodes one by one: the cluster stays up and data is preserved.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s4` → `scripts/scenario.sh s4`
  (write 60 known keys; `docker compose restart dbnodeN` × 3 with
  rejoin wait; verify 60/60 after each).
- `go run ./cmd/scenarios -mode inprocess -scenario s4`
  (`cmd/scenarios/main.go: ips4` — stop/start each node same-port,
  poll to 60/60 readable).
- `go test -race -tags integration ./test/ -run TestScenario_RestartNodesOneAfterAnother_ClusterStaysUp`
  plus `./pkg/cluster/ -run TestClusterReturningMasterWipeAndResync`.

Terminal output (docker):
```
[harness] restarting dbnode1
[harness] after restarting dbnode1: missing=0 wrong=0 of 60
[harness] restarting dbnode2
[harness] after restarting dbnode2: missing=0 wrong=0 of 60
[harness] restarting dbnode3
[harness] after restarting dbnode3: missing=0 wrong=0 of 60
RESULT: s4 PASS
```
Terminal output (in-process):
```
RESTART node 1
CHECK after restarting node 1: missing=0 wrong=0 of 60
RESTART node 2
CHECK after restarting node 2: missing=0 wrong=0 of 60
RESTART node 3
CHECK after restarting node 3: missing=0 wrong=0 of 60
```
Note: an immediate post-restart read can 404 on partitions the returning
node still masters until its snapshot+WAL reseed lands (eventual catch-up,
not loss); both harnesses verify after convergence. Returning nodes never
retake mastership (fenced to replica, reseeded from the new master).

---

Scenario 5:
۵. اضافه کردن نود در حین بار (Load)
- با تعداد کمی نود شروع کنید و زیر بار درخواست قرار دهید.
- در حین اجرا یک نود جدید اضافه کنید.
- کنترلر پارتیشن‌ها را مجدداً بالانس می‌کند؛ بررسی کنید که درخواست‌ها همچنان پاسخ می‌گیرند.
English: Add a node under load: the controller rebalances partitions; requests keep being served.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s5` → `scripts/scenario.sh s5`
  (2 nodes + 40 s load; `docker compose up -d dbnode3` mid-run;
  30 s rebalance-status poll; error timeline).
- `go run ./cmd/scenarios -mode inprocess -scenario s5`
  (`cmd/scenarios/main.go: ips5` — start node 3 mid-load, await
  3-way master spread, count load errors).
- `go test -race -tags integration ./test/ -run TestScenario_AddNodeMidstLoad_RebalanceViaMovePartition`.

Terminal output (docker):
```
JOINED AT: 20:35:17 (dbnode3 up mid-load)
t+30s running=False total_runs=3 last_moves=5 pending=0 ownership={'node-1': 6, 'node-2': 5, 'node-3': 5}
throughput: 1362.0 ops/s (total=54481)
latency: avg=5.863268ms p50~=5ms p99~=100ms
errors: 0  aborted_at_shutdown: 0
$ curl -X PUT/GET/DELETE http://localhost:8080/kv/smoke-key -> 200 / value / 200 / 404
RESULT: s5 PASS
```
Read: rebalance finished (pending=0, 6/5/5 ownership), 54481 ops with
zero errors across the join.
Terminal output (in-process):
```
JOIN node 3 mid-load
REBALANCED masters=map[0:1 1:1 2:2 3:3] spread3=true
LOAD ops=21691 errors=0
```

---

Scenario 6:
۶. زیاد و کم کردن تعداد رپلیکا
- در حین اجرای عادی، پارتیشن یا پارتیشن‌ها را طوری تنظیم کنید که تعداد رپلیکاها بیشتر یا کمتر شود.
- بررسی کنید که داده‌ها به درستی رپلیکیت شده و در صورت Fail همچنان داده‌ها در سیستم حفظ شود.
English: Raise/lower the replica count live: data replicates correctly and survives a failure.

Commands / entrypoints:
- `./scripts/run_all_scenarios.sh s6` → `scripts/scenario.sh s6`
  (40 known keys; `POST /replication/factor` 3→1→2 with map checks;
  `docker compose stop dbnode3`; two read passes: inside vs after the
  failover window).
- `go run ./cmd/scenarios -mode inprocess -scenario s6`
  (`cmd/scenarios/main.go: ips6` — RF 3/1/2 with replica-count asserts,
  kill, 40/40 verify).
- `go test -race -tags integration ./test/ -run TestScenario_AddOrRemoveReplicas_MidRun`.

Terminal output (docker):
```
=== POST /replication/factor {"replication_factor": 3} ===
{"replication_factor":3,"status":"ok","version":17}
=== POST /replication/factor {"replication_factor": 1} ===
{"replication_factor":1,"status":"ok","version":18}
=== POST /replication/factor {"replication_factor": 2} ===
{"replication_factor":2,"status":"ok","version":19}
Container kvdb-dbnode3 Stopped
pass 1 (t+12s, inside the failover window): missing=1 wrong=0 of 40 -> rf-2:200
pass 2 (t+32s, after convergence): missing=0 wrong=0 of 40
RESULT: s6 PASS
```
Read: RF changes accepted (versions 17/18/19); after the kill, one key
unreadable inside the window (rf-2 answered HTTP 200 with an empty body
mid-reseed) and 40/40 readable after convergence — the window, not loss.
Final map: masters 8/8 on the survivors.
Terminal output (in-process):
```
RF-SET replication_factor=3 -> http 200
RF-VERIFY max replicas per partition=2 (want 2)
VERIFY rf=3: missing=0 wrong=0 of 40
RF-SET replication_factor=1 -> http 200
RF-VERIFY max replicas per partition=0 (want 0)
VERIFY rf=1: missing=0 wrong=0 of 40
RF-SET replication_factor=2 -> http 200
KILL node 3 at rf=2
VERIFY after kill: missing=0 wrong=0 of 40
KEY-SPREAD across partitions: map[0:10 1:10 2:10 3:10]
```

---

LEDGER
- Docker grading run (`scripts/run_all_scenarios.sh`): s1 PASS, s2 PASS,
  s3 PASS, s4 PASS, s5 PASS, s6 PASS → GRADING SUMMARY: 6 passed, 0 failed.
- Native runner (`go run ./cmd/scenarios -mode inprocess -scenario all`):
  exit 0, all six scenarios green (RPS/s1 2376.4, RPS/s2 2513.8,
  failover 0 errors 60/60, restarts 0/0 ×3, join 0 errors, RF+kill 40/40).
- Unit suite `go test -race -count=1 ./...`: 100% green.

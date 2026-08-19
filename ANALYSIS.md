# Analysis

Every number below came from a run performed on this machine. Each section
gives the command, the raw output, and then what the numbers mean. Where a
result is ambiguous or does not reproduce, that is said rather than smoothed
over.

The raw evidence files are committed under `.evidence/`. Each section names the
file it quotes, so any figure here can be checked against the output that
produced it.

---

## How these were run

```bash
docker compose build                     # all seven images
bash scripts/sweep.sh                    # s1 s2 s2b s3 s4 s5 s6 reshard lag, in order
```

`scripts/sweep.sh` records the image IDs and the exit code of each scenario in
`.evidence/scenarios/sweep-summary.txt`. That file is the provenance for
everything here — one sweep, one set of images:

```
sweep started: 2026-08-19T07:01:59Z
images:
kv-db-client:latest de4047add384 2026-08-19 05:05:10 +0000 UTC
kv-db-controller:latest 4b629f28e485 2026-08-19 06:34:23 +0000 UTC
kv-db-dbnode1:latest 83d67778aa69 2026-08-19 04:27:12 +0000 UTC
kv-db-dbnode2:latest 0efc74c402ee 2026-08-19 04:27:12 +0000 UTC
kv-db-dbnode3:latest b82206aecf3e 2026-08-19 04:27:12 +0000 UTC
kv-db-dbnode4:latest c40a35b9e94e 2026-08-19 04:27:12 +0000 UTC
kv-db-loadbalancer:latest 86573542719a 2026-08-19 06:08:00 +0000 UTC

s1 EXIT=0  started=07:01:59 finished=07:02:36
s2 EXIT=0  started=07:02:36 finished=07:07:32
s2b EXIT=0  started=07:07:32 finished=07:10:50
s3 EXIT=0  started=07:10:50 finished=07:12:00
s4 EXIT=0  started=07:12:00 finished=07:13:41
s5 EXIT=0  started=07:13:41 finished=07:14:54
s6 EXIT=0  started=07:14:54 finished=07:16:25
reshard EXIT=0  started=07:16:25 finished=07:18:16
lag EXIT=0  started=07:18:16 finished=07:20:01
sweep finished: 2026-08-19T07:20:01Z
```

Scenario 6 was re-run afterwards on the same images to capture its controller
log, which the following scenario's teardown had discarded; the replication-lag
scenario was run three further times to measure its spread. Both are noted where
they appear.

### The machine, and why it dominates everything below

- AMD Ryzen 5 5600H, 12 logical CPUs, Windows 11, Docker Desktop
- Go 1.24.1; module has **zero external dependencies**
- **Every container is capped at 1.0 CPU.** Nodes get 512 MB, controller and
  load balancer 256 MB (`deploy.resources.limits` in `docker-compose.yml`)

Those limits are enforced, not decorative — read straight out of the running
containers (`.evidence/scenarios/console-s2.txt`):

```
--- kvdb-dbnode1 ---
cpu.max: 100000 100000
memory.max: 536870912
nproc (what Go uses for GOMAXPROCS): 12
```

`cpu.max: 100000 100000` is a 100 ms quota per 100 ms period — exactly one CPU.
`nproc` reports **12**, because a cgroup CPU quota is not a CPU count and Go
reads the latter. So each container runs `GOMAXPROCS=12` against a one-core
budget: twelve threads burn the whole quota in roughly the first 8 ms of each
100 ms period and the kernel then freezes the container for the remaining 92 ms.
That mechanism is the reason for almost every shape in this document, and it is
measured directly in scenario 2.

---

## Scenario 1 — one node, Set / Get / Delete

```bash
bash scripts/scenario.sh s1
```

Evidence: `.evidence/scenarios/console-s1.txt`

Correctness first, through the load balancer:

```
$ curl -X PUT http://localhost:8080/kv/smoke-key -d 'hello'
HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key
{"key":"smoke-key","value":"aGVsbG8=","version":1}

$ curl -X DELETE http://localhost:8080/kv/smoke-key
HTTP/1.1 200 OK
$ curl http://localhost:8080/kv/smoke-key   (expect 404)
HTTP/1.1 404 Not Found
```

(`aGVsbG8=` is base64 for `hello`; the node's `GetResponse.Value` is a `[]byte`,
which is how Go's JSON encoder renders it.)

Then load: 50% reads, 40% writes, 10% deletes, 8 concurrent workers, 20 seconds.

```
[harness] client run '1node-mixed': -addr http://loadbalancer:8080 -duration 20s -concurrency 8 -keyspace 1000 -value-bytes 128 -read 0.5 -delete 0.1
elapsed: 20.00182236s
throughput: 1353.7 ops/s (total=27077)
latency: avg=5.900354ms min=452.394µs max=90.23644ms
get: ok=10062 miss=3489 err=0
set: ok=10848 err=0
delete: ok=2678 err=0
errors: 0  aborted_at_shutdown: 0
```

**1353.7 ops/s, zero errors, average latency 5.9 ms.**

The 3489 `miss` results are not failures: the generator picks keys at random from
a 1000-key space and 10% of its operations are deletes, so a read of a key that
was deleted (or never written) correctly returns 404. `err=0` is the number that
matters.

`aborted_at_shutdown: 0` is worth one sentence, because it used to be 8. A
fixed-duration run used to cancel its root context at the deadline, which killed
the operations still in flight and counted them as errors — every run ended in a
small fake outage. `cmd/client/main.go` now stops workers from *starting* new
operations at the deadline and lets the in-flight ones drain, bounded by their
own per-operation timeout. That matters because the same counter is used to
measure a real outage in scenario 3, and eight phantom errors in a measurement
of 24 real ones would have been a third of the signal.

---

## Scenario 2 — throughput with 1, 2 and 4 nodes

```bash
bash scripts/scenario.sh s2
```

Evidence: `.evidence/scenarios/console-s2.txt`

Identical load at each size — 16 workers, 5000-key space, 50% reads, 20 seconds,
three repetitions after a seeding run:

| Nodes | rep 1 | rep 2 | rep 3 | median |
|---|---|---|---|---|
| 1 | 1260.5 | 1347.9 | 1320.4 | **1320.4** |
| 2 | 1186.8 | 1115.7 | 862.7 | **1115.7** |
| 4 | 1003.2 | 1069.9 | 1111.3 | **1069.9** |

All runs: `errors: 0  aborted_at_shutdown: 0`.

### Throughput went *down*

This is the honest result and it is more interesting than the expected one.
Going from one node to four cost about 19% of throughput. Two things cause it,
and the evidence separates them.

**1. The load balancer is the bottleneck, not the storage nodes.** From the same
run, cgroup CPU accounting read from inside each container:

| Container | CPU used | CFS periods | throttled | throttled time |
|---|---|---|---|---|
| **kvdb-loadbalancer** | 218.87 s | 2316 | **2088 (90.2%)** | **382.05 s** |
| kvdb-dbnode1 | 142.00 s | 2339 | 309 (13.2%) | 69.81 s |
| kvdb-dbnode2 | 118.43 s | 1688 | 281 (16.6%) | 60.99 s |
| kvdb-dbnode3 | 35.57 s | 986 | 1 (0.1%) | 0.001 s |
| kvdb-dbnode4 | 38.00 s | 990 | 2 (0.2%) | 0.09 s |

The load balancer was frozen by its CPU quota in **nine periods out of ten**.
Every request in the system passes through it, and it is capped at one CPU just
like a node that handles a quarter of the traffic. Adding storage capacity behind
a saturated funnel cannot increase throughput — the nodes that were added
(dbnode3, dbnode4) were throttled essentially never and used a quarter of the CPU
dbnode1 did. They were idle.

**2. Replication work appears when the second node does.** At one node, a
replication factor of 2 cannot be satisfied — there is nowhere to put a replica —
so the node does zero replication work. At two nodes every write must also be
shipped over HTTP to a replica. The one-node figure is therefore not the same
workload as the others, and comparing them directly overstates the penalty of
scaling out.

**What this does not show.** It does not show that the system fails to scale. It
shows that on this hardware, with one CPU per container and every request funneled
through a single load balancer process, the load balancer saturates first. The
correct next experiment is to raise the load balancer's CPU limit, or run several
load balancer instances, and re-measure — and the honest position is that this was
not done, so no scaling claim is made in either direction.

Run-to-run spread is also large: the two-node repetitions ranged 862.7 to 1186.8,
a spread of ±16% around their own mean. Differences smaller than that between
configurations are noise, and this document does not read anything into them.

---

## Scenario 2b — reading from replicas

```bash
bash scripts/scenario.sh s2b
```

Evidence: `.evidence/scenarios/console-s2b.txt`, `.evidence/scenarios/s2b.lb-config.txt`

Three nodes, replication factor 2, 100% reads over a pre-seeded key space. The
load balancer is restarted between every run so the two strategies alternate,
and each restart's own log line is captured so there is no doubt which strategy
produced which number:

```
--- rep 1, LB_READ_FROM_REPLICAS=false ---
[loadbalancer] read strategy: master only (a Get always sees the latest acknowledged Set)
--- rep 1, LB_READ_FROM_REPLICAS=true ---
[loadbalancer] read strategy: master and replicas (higher read throughput, no read-your-writes)
```

| Repetition | master only | master + replicas |
|---|---|---|
| 1 | 1234.7 | 1380.4 |
| 2 | 1330.8 | 1416.6 |
| 3 | 1397.9 | 1387.2 |
| mean | 1321.1 | **1394.7** (+5.6%) |

**The +5.6% mean difference should not be believed.** Look down the master-only
column: 1234.7 → 1330.8 → 1397.9, rising monotonically across the run. The
replica column is flat. By the third pair the two strategies are within 0.8% of
each other (1397.9 vs 1387.2). The apparent advantage is mostly a warm-up trend
in the first strategy measured, and the alternating design was what exposed it.

The reason there is so little to find is scenario 2's reason: reads are not
limited by the nodes. The load balancer is saturated, so spreading reads across
more nodes behind it changes nothing measurable. There is a second reason too —
the replicas are not spread evenly. From `.evidence/scenarios/s2b.map.txt`:

```
masters : {'dbnode1:8001': 6, 'dbnode2:8002': 5, 'dbnode3:8003': 5}
replicas: {'dbnode1:8001': 7, 'dbnode3:8003': 9}
```

dbnode2 holds **no replicas at all**, so "read from replicas" spreads reads over
two nodes, not three. That is the rebalancer limitation documented in
`STATUS.md`: it moves masters, never replicas.

**The default is still master-only, and that is a correctness choice, not a
performance one.** Replication is asynchronous, so a read served by a replica can
miss a write that has already been acknowledged. Since the measurement shows no
throughput to be gained here, there is nothing to trade for it.

---

## Scenario 3 — a node fails under load

```bash
bash scripts/scenario.sh s3
```

Evidence: `.evidence/scenarios/console-s3.txt`, `s3.kill.txt`,
`s3.convergence.txt`, `s3.map.txt`

40-second run, 8 workers, 50% reads. `dbnode2` is stopped 10 seconds in.

```
STOPPED AT: 2026-08-19T07:11:18.321974900Z
```

```
elapsed: 40.029880482s
throughput: 732.0 ops/s (total=29300)
latency: avg=10.918353ms min=433.785µs max=5.001886616s
get: ok=12716 miss=1994 err=11
set: ok=14566 err=13
delete: ok=0 err=0
errors: 24  aborted_at_shutdown: 0
```

### How many requests failed: 24 of 29,300 (0.08%)

Eleven reads and thirteen writes. No request returned wrong data.

### How long until the map converged: 13.7 seconds

From `s3.convergence.txt`, polling `/map` once a second:

```
07:11:19 t+1s  version=16 masters={'dbnode1': 5, 'dbnode2': 6, 'dbnode3': 5}
...            (unchanged for ten seconds)
07:11:31 t+10s version=16 masters={'dbnode1': 5, 'dbnode2': 6, 'dbnode3': 5}
07:11:32 t+11s version=22 masters={'dbnode1': 5, 'dbnode3': 11}
```

The node stopped at 07:11:18.32 and the map changed at 07:11:32 — **13.7 seconds**.
That is not a bug, it is the configuration: `CONTROLLER_HEARTBEAT_INTERVAL=5s`
and `CONTROLLER_HEARTBEAT_TIMEOUT=15s`. The controller must observe silence for
15 seconds before declaring a node dead, and it only checks every 5 seconds, so
detection lands somewhere in [15 s, 20 s] of the last successful heartbeat — and
the last heartbeat was up to 5 seconds before the kill. 13.7 s is inside that
window.

The map version jumped 16 → 22, one bump per promoted partition: dbnode2
mastered 6, and dbnode3 absorbed all six (5/6/5 → 5/–/11).

### The error pattern, and what retries did

Per-second HTTP status counts from the load balancer's own access log:

```
07:11:19 502 24
07:11:24 502 8
07:11:29 502 8
07:11:34 502 8
```

48 upstream failures at the load balancer, 24 errors reaching the application.
The load balancer retried (`LB_MAX_RETRIES=2`), and the client SDK retried on top
of that (3 attempts, exponential backoff from 100 ms with 20% jitter, 502 in its
retryable set) — together they halved the failures.

The pattern is exactly 8 every 5 seconds after the initial burst, and the
maximum latency in the run is `5.001886616s`. The generator's per-operation
timeout is 5 seconds (`-timeout`, `cmd/client/main.go:167`) and there are 8
workers. So each worker burned one operation per 5-second timeout cycle while its
partition had no master: 8 workers × 3 cycles = 24. The failure window closes at
07:11:34, two seconds after the map converged — the extra two seconds is the load
balancer's 5-second topology refresh interval catching up.

**Retries do not eliminate the outage; they shrink it.** A client with more
patience than the detection window would have seen zero errors. A client with
none would have seen 48. The system's floor is set by the heartbeat timeout, and
that is a tuning decision with a cost on both sides: shortening it speeds up
failover and increases the chance of declaring a healthy-but-slow node dead.

### The cost that is not in the numbers

```
--- before the kill ---   replicas: {'dbnode2': 9, 'dbnode3': 7}
--- after failover ---    replicas: {'dbnode3': 1}
```

Sixteen partitions, and after the failover **one** of them has a replica. The
controller promotes a replica to master but never creates a replacement replica
elsewhere. The cluster is serving correctly and is one node failure away from
losing data, and nothing in the throughput or error figures says so. This is the
most important limitation in the system and it is listed as such in `STATUS.md`.

---

## Scenario 4 — restarting every node in turn

```bash
bash scripts/scenario.sh s4
```

Evidence: `.evidence/scenarios/console-s4.txt`, `s4.map.txt`, `s4.controller-*.txt`

Sixty known keys are written, then each node is restarted in turn and all sixty
are read back.

```
wrote 60 keys
after restarting dbnode1: missing=0 wrong=0 of 60
after restarting dbnode2: missing=0 wrong=0 of 60
after restarting dbnode3: missing=0 wrong=0 of 60
```

**No data lost, at any point.** The controller log shows why:

```
[rejoin] node 1 re-registered; it holds no data, moving its partitions to replicas
[rejoin] node 1: 6 partitions promoted away, 14 reseeded, 0 unrecoverable
[rejoin] node 2: 5 partitions promoted away, 13 reseeded, 0 unrecoverable
[rejoin] node 3: 5 partitions promoted away, 5 reseeded, 0 unrecoverable
```

This is the scenario that found the worst bug in the project, and it is worth
explaining because the failure was invisible.

Nodes are in-memory. A restarted node comes back **empty** — but it comes back on
the same address and port, re-registers, is re-adopted under the same id, and is
healthy again in under a second. The heartbeat timeout never fires, so no failure
path runs. From the controller's point of view nothing happened. The node then
continues to master the partitions it mastered before, all of which are now
empty. Every key it held reads as missing while `/nodes` and `/health` report a
perfectly healthy cluster. Restart all three in turn and the entire dataset is
gone with no error anywhere.

Before the fix this scenario reported `missing=25`, then `43`, then `60 of 60`.

The fix is `internal/controller/rejoin.go`. `HandleRegister` now distinguishes a
first registration from a repeat one (`ClusterState.RegisterNode` returns a
`rejoined` flag), and a repeat registration is treated as what it is — a node that
lost its memory. For each partition: if the returning node was master, promote a
healthy replica away from it and then reseed it as a replica; if it was a
replica, reseed it from the current master; if it was master with no healthy
replica, log that the data is unrecoverable. The counts above are that logic
reporting itself, and `0 unrecoverable` is the claim being made.

Master distribution was unchanged throughout (6/5/5 before and after all three
restarts); the map version went 16 → 29 → 40 → 51, which is the reseeding and
role announcements doing their work.

**What this does not prove.** With replication factor 1, or if two nodes were
restarted at once, the log would read `N unrecoverable` and the data would be
gone — the recovery works because a copy existed somewhere else, not because the
node recovered anything of its own.

---

## Scenario 5 — adding a node under load

```bash
bash scripts/scenario.sh s5
```

Evidence: `.evidence/scenarios/console-s5.txt`, `s5.join.txt`,
`s5.rebalance-progress.txt`, `s5.map.txt`

Two nodes under a 40-second load; `dbnode3` is started 10 seconds in.

```
elapsed: 40.0s
throughput: 1117.9 ops/s (total=44738)
latency: avg=7.143044ms min=404.519µs max=107.105233ms
get: ok=20431 miss=1905 err=0
set: ok=22402 err=0
errors: 0  aborted_at_shutdown: 0
```

**44,738 operations, zero errors.** The load balancer's access log records
exactly one 502 in the entire run, in the second the node joined:

```
07:14:10 502 1
```

The rebalance itself:

```
JOINED AT: 07:14:08
07:14:10 t+1s running=False total_runs=3 last_moves=5 pending=0 ownership={'node-1': 6, 'node-2': 5, 'node-3': 5}
```

Ownership went from 8/8 to **6/5/5 in about two seconds**, moving five
partitions, while the cluster was serving over a thousand operations a second.

Adding a node is the cheapest topology change in this system, and the reason is
structural: it never leaves a partition without a master. The rebalancer moves
one partition at a time, and for each move it re-checks that the plan is still
valid before acting and restores the announced roles if the move fails
(`Rebalancer.movePartition`, `internal/controller/rebalance.go`). Both of those
guards were added after a failure in which a stale plan clobbered a node's master
role, the copy then failed, and the partition stayed unwritable — returning 409
NOT_MASTER indefinitely — because nothing re-announced it.

### The limitation this scenario exposes

```
--- three nodes, after the rebalance ---
masters : {'dbnode1': 6, 'dbnode2': 5, 'dbnode3': 5}
replicas: {'dbnode1': 6, 'dbnode2': 10}
```

Masters are balanced. **Replicas are not — dbnode3 received none.** The
rebalancer moves masters only, so replica placement stays wherever it was first
assigned. The practical consequences: the new node carries less than its share of
work when reads come from replicas, and the two original nodes carry all the
replication traffic. The system is correct, just unevenly loaded.

---

## Scenario 6 — changing the replication factor

```bash
bash scripts/scenario.sh s6
```

Evidence: `.evidence/scenarios/console-s6.txt`, `s6.map.txt`,
`s6.controller-rf.txt`, `s6.rf.txt`

Forty known keys are written, the replication factor is cycled 2 → 3 → 1 → 2, and
then a node is stopped to see whether the data survives.

| Replication factor | Replica placement (`/map`) | Controller log |
|---|---|---|
| 2 (initial) | `{dbnode2: 8, dbnode3: 8}` | — |
| 3 | `{dbnode1: 11, dbnode2: 10, dbnode3: 11}` | `factor=3: 16 replicas seeded, 0 failed` |
| 1 | `{}` | `factor=1: 0 replicas seeded, 0 failed` |
| 2 | `{dbnode1: 5, dbnode2: 5, dbnode3: 6}` | `factor=2: 16 replicas seeded, 0 failed` |

Then `dbnode3` is stopped and all forty keys are read twice:

```
pass 1 (t+12s, inside the failover window): missing=1 wrong=0 of 40 -> rf-2:200
pass 2 (t+32s, after convergence):          missing=0 wrong=0 of 40
```

**No data was lost.** The single miss in pass 1 is a read that landed inside the
failover window; the immediate status re-check on that same key returned `200`,
which is what `-> rf-2:200` records. Pass 2, run past the 13.7-second convergence
measured in scenario 3, is clean.

The two-pass design exists because the one-pass version was misleading. A single
read sweep at t+12 s straddles the convergence boundary and reports transient
routing failures as missing data. Separating "missing now" from "missing
permanently" is the difference between a measurement of replication and a
measurement of the failover window.

### The bug this scenario found

The first honest run of this scenario reported `missing=5 wrong=0 of 40` — real,
permanent data loss at replication factor 2, which should not be possible.

The cause was ordering in `Handler.HandleSetReplicationFactor`. It pushed a
partition snapshot to each newly added replica and *then* announced the new
roles. But a node refuses a snapshot for a partition it holds no role in
(`Node.ApplySnapshotWithSequence` returns `ErrPartitionNotOwned`), so every new
replica rejected its seed — and the error was swallowed by a bare `continue`
commented "best-effort". The replica was then told it was a replica of a
partition it had no data for.

Raising the replication factor therefore *looked* like it worked. The map showed
the replicas, and the live write stream kept them current from that moment
onward. Only keys written *before* the change were missing, and nothing ever
reads a replica until its master dies — so the loss stayed invisible until a
failover promoted an empty replica.

The fix announces roles first, seeds second, and logs every failure with a
summary line per call. The `16 replicas seeded, 0 failed` lines in the table
above are that logging, and they are the direct evidence that the seed now
happens.

### The limitation this scenario exposes

```
--- after killing dbnode3 ---
masters : {'dbnode1': 8, 'dbnode2': 8}
replicas: {'dbnode1': 2, 'dbnode2': 3}
```

The same degradation as scenario 3: sixteen partitions, five replicas. Surviving
one failure at replication factor 2 works. Surviving a second one, before the
first node returns, does not.

Note also that replica placement at replication factor 2 depends on how you got
there. Arriving at 2 from registration order gave `{dbnode2: 8, dbnode3: 8}` —
one node with none. Arriving at 2 via 3 → 1 → 2 gave `{5, 5, 6}` — even. The
placement algorithm distributes evenly when it computes a fresh assignment; the
uneven case is a leftover from the order nodes joined in.

---

## Re-sharding 16 → 32 partitions under load

```bash
bash scripts/scenario.sh reshard
```

Evidence: `.evidence/scenarios/console-reshard.txt`, `reshard.start.txt`,
`reshard.progress.txt`, `reshard.lb-rps-reshard.txt`,
`reshard.lb-timeline-reshard.txt`, `reshard.known-keys.txt`

This is spec section 13 — changing the partition count while the cluster serves
traffic. Sixty known keys are written first, then a 40-second 80%-read load
starts, and ten seconds in the partition count is doubled.

```
STARTED AT: 07:17:03.956422200
{"new_num_partitions":32,"old_num_partitions":16,"status":"started","version":17}
```

```
elapsed: 40.0s
throughput: 1144.5 ops/s (total=45838)
latency: avg=6.978205ms min=465.556µs max=370.367195ms
get: ok=31159 miss=5602 err=0
set: ok=9077 err=0
errors: 0  aborted_at_shutdown: 0

[harness] known keys after the reshard: missing=0 wrong=0 of 60
```

### Duration: 857 ms for 32 ranges

The controller's own timing, from `/reshard/status`:

```
07:17:04 t+1s state=migrating running=True ranges=22/32 by_state={'done': 22, 'transferring': 1, 'pending': 9}
07:17:05 t+2s state=stable   running=False duration_ms=857
```

The one-second poll only caught a single frame of it: 22 of 32 ranges were
already done when the first sample landed.

### Throughput dip: 31%, for one second

Requests per second through the load balancer, from its access log:

```
07:17:01 1091
07:17:02 1287
07:17:03 1069      <- reshard POSTed at 07:17:03.956
07:17:04  735      <- 31% below the surrounding rate
07:17:05 1235
07:17:06 1196
07:17:07 1179
```

### Errors: 19 upstream, 0 reaching the client

```
07:17:04 502 19
```

Nineteen 502s, all inside the same second as the dip, and `errors: 0` in the
client's own count — the retry layers absorbed every one. No request returned
wrong data, and the sixty known keys all survived with the right values.

### Why 5602 read misses is not data loss

This is the number a hostile reader should pick up, so here is the disposal. The
generator seeds by writing 4000 operations to a 4000-key space *choosing keys at
random*, so by the coupon-collector argument only about 63% of the space gets
written — roughly 1500 keys never exist. Reads of those correctly return 404.

Two pieces of evidence confirm it is the seeding and not the reshard. First, the
404 rate starts high and **decays smoothly across the reshard boundary** as the
run's own writes fill in the gaps, with no discontinuity at 07:17:04:

```
07:16:54 404  64      (load starts)
07:16:55 404 314
07:16:56 404 334
07:16:57 404 305
...
07:17:30 404  52
07:17:34 404  35      (last second with any 404s)
```

Second, and decisively: the sixty keys that *were* written explicitly, before the
reshard, by name, all came back correct — `missing=0 wrong=0 of 60`. That check
was added to this scenario for exactly this reason. A statistical argument about
the generator is not proof; a named key set is.

### What is not exercised here

The load balancer's per-key choice between the old and new formula,
`DecideRouteForKey`, **is not implemented** — see `ARCHITECTURE.md` §5 and
`STATUS.md`. The router therefore fell back to `routeWithDualHash`, which probes
the new owner and then the old owner.

So the result above is a measurement of the *fallback*, not of the intended
routing. That distinction matters:

- The fallback is safe for reads, and this run was 80% reads.
- The fallback is **not** safe for writes: it can write to a node whose copy is
  about to be superseded by the final drain. No write in this run was observed to
  be lost, but with 9,077 writes over a migration that finished in 857 ms, very
  few writes could have landed in a vulnerable window. **Absence of an observed
  failure here is not evidence that the fallback is safe.**
- The headline test for this path, which drives a re-shard under sustained write
  load, is skipped while the function is unimplemented. It has never been
  observed to pass. `STATUS.md` names it.

---

## Replication lag under write load

```bash
bash scripts/scenario.sh lag
```

Evidence: `.evidence/scenarios/console-lag.txt`,
`console-lag-rep{1,2,3}.txt`, `lag-rep{1,2,3}.queue.txt`

Write-only load — 16 workers, 512-byte values, 30 seconds, no reads — while each
node's `/health` is polled for `details.replication_queue`, the depth of the
outbound replication channel. Run four times.

| Run | write throughput | peak queue depth (any node) | queue evidence |
|---|---|---|---|
| sweep | 848.9 ops/s | **not recoverable** | overwritten — see below |
| rep 1 | 799.8 ops/s | 47 | `lag-rep1.queue.txt` (07:27:50–07:29:00) |
| rep 2 | 610.6 ops/s | 136 | `lag-rep2.queue.txt` (07:29:41–07:31:01) |
| rep 3 | 655.2 ops/s | 112 | `lag-rep3.queue.txt` (07:31:42–07:32:59) |

Every throughput above is read from its own `console-lag*.txt`, which the harness
does not overwrite. The queue depths are the maximum sample in the named file.

**The sweep run's queue file no longer exists.** `scenario.sh` clears
`$EVIDENCE/$SCENARIO.*` when a scenario is dispatched, so the third repeat
rewrote `lag.repl-lag-under-write-load.txt` — it is now byte-identical to
`lag-rep3.queue.txt` (same 07:31:42–07:32:59 series; `md5sum` on both gives
`a9f4e83b8b2245447a7d19702db68094`, and `diff` exits 0). A peak depth for the
sweep run therefore cannot be resolved against any
file on disk, and none is claimed here. Only its throughput (848.9 ops/s, from
`console-lag.txt`) survives.

The queue's capacity is **10,000** entries (`internal/dbnode/replicator.go:53`).
The deepest sample that can still be resolved — 136, in rep 2 — used **1.4%** of it.

The deepest run's series (`lag-rep2.queue.txt`) shows what the shape actually is —
brief spikes, not a growing backlog:

```
07:29:41 node@8001=136 node@8002=10 node@8003=0
07:29:45 node@8001=6   node@8002=0  node@8003=2
07:29:49 node@8001=0   node@8002=0  node@8003=0
07:29:53 node@8001=7   node@8002=5  node@8003=6
...
07:30:10 node@8001=0   node@8002=0  node@8003=0   (and zero for the rest of the run)
```

**At these rates replication keeps up.** Four masters' worth of writes at
600–850 ops/s are absorbed by 4 replication workers per node without a
persistent backlog. The queue hits zero repeatedly *during* the load — in rep 2
it is back to 0 by 07:29:49, eight seconds in — and
`lag.repl-lag-after-load.txt` is flat at 0 across its whole post-load window.
**Zero entries were dropped** on any node in any run — checked directly:

```
$ docker compose logs dbnodeN | grep -c "queue full, dropping"
dbnode1: 0
dbnode2: 0
dbnode3: 0
```

Two caveats, both real:

- Peak depth is **inversely** correlated with throughput across the three runs
  whose queue files survive.
  That is the CPU cap again: when the whole stack gets less CPU the request path
  slows down more than the replication workers do, so the queue stays shallower
  at lower throughput. It means these numbers characterise this machine's
  contention, not a property of the replication design.
- An earlier run of this scenario, on the same images, showed a queue an order of
  magnitude deeper. Its evidence file was overwritten by a later run of the same
  scenario (the harness clears a scenario's files at dispatch, which is what makes
  each file belong to exactly one execution). **No number from that run is quoted
  here**, but it is mentioned because it means the four runs above should not be
  read as the whole distribution.

What the design guarantees regardless of the number: the queue is bounded and
`Replicator.Enqueue` **drops on overflow** after logging. A write that is dropped
from the replication stream has already returned 200 to the client. That is the
failure mode to watch for at higher write rates, and the log line is the only
warning.

---

## Benchmarks

```bash
go test ./test -tags=integration_external -run '^$' \
  -bench 'BenchmarkRead|BenchmarkWrite' -benchtime=5000x -count=3 -timeout 600s
```

Evidence: `.evidence/27_benchmarks.txt`, exit code **0**

```
goos: windows
goarch: amd64
pkg: github.com/kvdb/kvdb/test
cpu: AMD Ryzen 5 5600H with Radeon Graphics
BenchmarkWrite-12    	    5000	   2576193 ns/op
BenchmarkWrite-12    	    5000	   2438932 ns/op
BenchmarkWrite-12    	    5000	   2290218 ns/op
BenchmarkRead-12     	    5000	   1524102 ns/op
BenchmarkRead-12     	    5000	   1416589 ns/op
BenchmarkRead-12     	    5000	   1633630 ns/op
PASS
ok  	github.com/kvdb/kvdb/test	60.888s
```

| | median ns/op | implied ops/s |
|---|---|---|
| `BenchmarkWrite` | 2,438,932 | ~410 |
| `BenchmarkRead` | 1,524,102 | ~656 |

These are **single-goroutine, sequential** operations issued from the Windows
host, so they measure round-trip latency, not capacity. Two things follow.

**Reads are about 38% faster than writes.** That is the replication enqueue and
the partition WAL append on the write path, plus the extra JSON body.

**Both are far slower than the concurrent figures.** 410 sequential writes/s
against 1350 concurrent ops/s is not a contradiction — it is the difference
between one request in flight and eight. Each round trip here also crosses
Docker Desktop's Windows port forward, which the in-container load generator
(`docker compose run client`, used by every scenario above) does not. That is why
the scenarios run the generator inside the network rather than from the host, and
why these benchmark numbers should be read as a latency floor for a host client,
not as the system's throughput.

---

## What was not measured

Stated so that nothing here is mistaken for a complete performance picture.

- **The race detector never ran.** `-race` requires cgo, and with
  `CGO_ENABLED=1` the build fails with `cgo: C compiler "gcc" not found` on this
  machine. No claim is made about data races. Evidence:
  `.evidence/20_test_race.txt`.
- **No memory profiling.** `memory.max` is enforced at 512 MB per node, but no
  run pushed against it and no heap profile was taken.
- **No sustained soak.** The longest run here is 40 seconds. Nothing is known
  about behaviour over hours, memtable growth past the 64 MB freeze threshold, or
  immutable-table accumulation — the memtable was never observed to freeze at all
  at these data volumes.
- **The load balancer was never given more CPU.** It is the measured bottleneck
  in scenario 2 and the obvious next experiment, and it was not run.
- **Writes during a re-shard were never exercised against the intended routing**,
  because `DecideRouteForKey` is unimplemented. See the re-shard section above.

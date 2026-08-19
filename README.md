# KV-DB

A distributed, in-memory key-value store in Go. Keys are spread across
partitions by `hash(key) % numPartitions`; each partition has one master and N
replicas; a controller owns the partition map and handles failure, rebalancing
and re-sharding; a load balancer routes client requests using a cached copy of
that map.

No external dependencies — `go.mod` requires nothing beyond the standard
library. Built and tested with Go 1.24.1.

- **What it does and how**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Measurements, with the commands that produced them**: [ANALYSIS.md](ANALYSIS.md)
- **Design decisions, rejected alternatives, examiner questions**: [DEFENSE.md](DEFENSE.md)
- **What works, what does not, what was not measured**: [STATUS.md](STATUS.md)

---

## Run it

### With Docker (recommended)

```bash
docker compose up --build -d
docker compose ps
```

That starts a controller, a load balancer and three database nodes. Wait a few
seconds for the nodes to register, then:

```bash
curl -i -X PUT    http://localhost:8080/kv/hello -d 'world'
curl -i           http://localhost:8080/kv/hello
curl -i -X DELETE http://localhost:8080/kv/hello
curl -i           http://localhost:8080/kv/hello    # 404
```

Ports:

| Service | URL | Notes |
|---|---|---|
| Load balancer | http://localhost:8080 | the only address a client needs |
| Controller | http://localhost:8000 | `/map`, `/nodes`, `/health`, `/admin` |
| dbnode1..3 | http://localhost:8001-8003 | direct node access, for debugging |

Useful controller endpoints:

```bash
curl -s http://localhost:8000/map                # partition map, masters and replicas
curl -s http://localhost:8000/nodes              # registered nodes and their status
curl -s http://localhost:8000/reshard/status     # re-shard progress
curl -s http://localhost:8000/rebalance/status   # rebalance progress
```

A fourth node and a load-generating client are behind the `demo` profile so they
do not start by default:

```bash
docker compose --profile demo up -d dbnode4
docker compose --profile demo run --rm --no-deps client \
  -addr http://loadbalancer:8080 -duration 30s -concurrency 8 -keyspace 5000 -read 0.8
```

Tear down:

```bash
docker compose down -v
```

Every container has a CPU and memory limit set in `docker-compose.yml`
(`deploy.resources.limits`). Compose v2 enforces these — that is verified, not
assumed; see `ANALYSIS.md` scenario 2, which reads `cpu.max` and `cpu.stat` from
inside the containers. Those limits are the reason the throughput numbers look
the way they do.

### Without Docker

```bash
go build ./cmd/...
```

**The binaries take no command-line flags.** Every setting is an environment
variable, read in `loadConfig()` in each `cmd/*/main.go`. Start the pieces in
this order, each in its own shell:

```bash
CONTROLLER_PORT=8000 CONTROLLER_NUM_PARTITIONS=16 CONTROLLER_REPLICATION_FACTOR=2 \
CONTROLLER_ADVERTISE_URL=http://127.0.0.1:8000 ./controller

NODE_PORT=8001 NODE_REPLICATION_PORT=9001 NODE_CONTROLLER_URL=http://127.0.0.1:8000 ./dbnode
NODE_PORT=8002 NODE_REPLICATION_PORT=9002 NODE_CONTROLLER_URL=http://127.0.0.1:8000 ./dbnode
NODE_PORT=8003 NODE_REPLICATION_PORT=9003 NODE_CONTROLLER_URL=http://127.0.0.1:8000 ./dbnode

LB_PORT=8080 LB_CONTROLLER_URL=http://127.0.0.1:8000 ./loadbalancer
```

`NODE_CONTROLLER_URL` and `LB_CONTROLLER_URL` default to `http://controller:8000`
— the Docker service name — so both must be set when running outside Compose.
`CONTROLLER_ADVERTISE_URL` is the address nodes call back on during a re-shard;
its default is already a localhost URL, so it only matters inside Docker.

---

## Configuration

Every value below is an environment variable. The "code default" column is what
`DefaultServerConfig()` / `DefaultNodeConfig()` returns; the "compose" column is
what `docker-compose.yml` sets, where it differs.

| Variable | Component | Code default | Compose | Effect |
|---|---|---|---|---|
| `CONTROLLER_PORT` | controller | 8000 | 8000 | listen port |
| `CONTROLLER_NUM_PARTITIONS` | controller | 16 | 16 | partitions the key space is split into |
| `CONTROLLER_REPLICATION_FACTOR` | controller | 2 | 2 | copies of each partition (1 master + N-1 replicas) |
| `CONTROLLER_HEARTBEAT_INTERVAL` | controller | 5s | 5s | how often nodes are health-checked |
| `CONTROLLER_HEARTBEAT_TIMEOUT` | controller | **30s** | **15s** | silence before a node is declared dead; also sets suspect timeout to half of it |
| `CONTROLLER_ADVERTISE_URL` | controller | `http://127.0.0.1:8000` | service name | the URL nodes call back on during a re-shard |
| `NODE_PORT` / `NODE_REPLICATION_PORT` | dbnode | 8001 / 9001 | per node | listen ports |
| `NODE_CONTROLLER_URL` | dbnode | `http://controller:8000` | same | where to register |
| `NODE_ADVERTISE_ADDR` | dbnode | the container hostname, else `127.0.0.1` | service name | the host other cluster members use to reach this node (`cmd/dbnode/main.go:63`) |
| `NODE_DATA_DIR` | dbnode | `/data` | volume | **has no effect** — nodes are in-memory; see STATUS.md |
| `LB_PORT` | load balancer | 8080 | 8080 | listen port |
| `LB_CONTROLLER_URL` | load balancer | `http://controller:8000` | same | where to fetch `/map` |
| `LB_TOPOLOGY_REFRESH` | load balancer | 5s | 5s | how often the cached map is refreshed |
| `LB_MAX_RETRIES` | load balancer | 2 | 2 | retries per request before giving up |
| `LB_READ_FROM_REPLICAS` | load balancer | `false` | `false` | `false` = reads go to the master, so a Get always sees the last acknowledged Set. `true` = reads spread over master and replicas: higher read capacity, no read-your-writes |

The 15s dead timeout in Compose is deliberately shorter than the 30s code
default, so a failover is observable within a demo rather than half a minute
later. It directly sets the length of the failure window measured in
`ANALYSIS.md` scenario 3.

The load balancer logs which read strategy it started with, so the choice is
visible in `docker compose logs loadbalancer` rather than inferred.

---

## Tests

The integration suite starts a controller, a load balancer and several nodes
**in-process** — no Docker required.

```bash
go build ./...
go vet ./...
go test ./test -tags=integration -count=1 -timeout 900s
```

`-count=1` matters: without it Go serves cached results, and a cached pass is not
a pass.

Two other build tags exist:

- `integration_external` — tests and benchmarks that need a cluster already
  running at `localhost:8080`. Start Docker first, then:
  ```bash
  go test ./test -tags=integration_external -run '^$' -bench 'BenchmarkRead|BenchmarkWrite' -benchtime=5000x -count=3
  ```
- `reshard_probe` — diagnostic probes for the re-shard path.

Run one scenario on its own:

```bash
go test ./test -tags=integration -count=1 -v -run '^TestScenario_AddOrRemoveReplicas_MidRun$'
```

---

## Measurement harness

`scripts/scenario.sh` runs one named scenario end to end against Docker and
writes its raw evidence to `.evidence/scenarios/`:

```bash
bash scripts/scenario.sh s1        # single node baseline
bash scripts/scenario.sh s2        # 1 vs 2 vs 4 nodes
bash scripts/scenario.sh s2b       # master-only vs master+replica reads
bash scripts/scenario.sh s3        # kill a node under load
bash scripts/scenario.sh s4        # rolling restart of every node
bash scripts/scenario.sh s5        # add a node under load
bash scripts/scenario.sh s6        # change the replication factor, then kill a node
bash scripts/scenario.sh reshard   # grow 16 -> 32 partitions under load
bash scripts/scenario.sh lag       # replication queue depth under write load
```

`scripts/sweep.sh` runs all nine in order and records each exit code in
`.evidence/scenarios/sweep-summary.txt`, so every number in `ANALYSIS.md` can be
traced to one sweep against one set of images.

The `.evidence/` directory is committed on purpose. Each number quoted in
`ANALYSIS.md` names the file it came from.

---

## Layout

```
cmd/            controller, dbnode, loadbalancer, client (load generator)
internal/
  controller/   partition map, health, failover, rebalance, reshard, rejoin repair
  dbnode/       key-value operations, per-partition WAL, replication, range migration
  loadbalancer/ routing, retry, topology cache, dual-formula routing
  netutil/      small HTTP helpers
pkg/
  kvdb/         client SDK (retry with exponential backoff and jitter)
  storage/      in-memory LSM engine; also a file WAL and snapshot, which are
                built but never invoked - see STATUS.md
test/           integration and scenario tests (build tags above)
scripts/        the measurement harness
.evidence/      raw output behind every number in ANALYSIS.md
```

## Known limitations

Stated in full in [STATUS.md](STATUS.md). The short version: nothing is
persisted to disk; a failed node's partitions are not re-replicated elsewhere
while it is down; the rebalancer moves masters but not replicas; and the load
balancer's per-key dual-formula routing decision during a re-shard
(`DecideRouteForKey`) is deliberately not implemented.

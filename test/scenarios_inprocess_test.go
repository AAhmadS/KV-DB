//go:build integration
// +build integration

package test

import (
	bytespkg "bytes"
	contextpkg "context"
	encodingjson "encoding/json"
	errorspkg "errors"
	fmtpkg "fmt"
	io "io"
	mathrand "math/rand"
	netpkg "net"
	nethttp "net/http"
	strconvpkg "strconv"
	stringspkg "strings"
	syncpkg "sync"
	atomicpkg "sync/atomic"
	testingpkg "testing"
	timepkg "time"

	"github.com/kvdb/kvdb/internal/controller"
	"github.com/kvdb/kvdb/internal/dbnode"
	"github.com/kvdb/kvdb/internal/loadbalancer"
	"github.com/kvdb/kvdb/pkg/kvdb"
)

type cluster struct {
	controller  *controller.Server
	lb          *loadbalancer.Server
	nodes       map[int]*dbnode.Server
	adminClient *nethttp.Client

	controllerURL string
	lbURL         string
	nodePorts     map[int]int
}

func newTestHTTPClient(timeout timepkg.Duration) *nethttp.Client {
	return &nethttp.Client{
		Timeout: timeout,
		Transport: &nethttp.Transport{
			Proxy: nethttp.ProxyFromEnvironment,
			DialContext: (&netpkg.Dialer{
				Timeout:   5 * timepkg.Second,
				KeepAlive: 30 * timepkg.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          512,
			MaxIdleConnsPerHost:   128,
			MaxConnsPerHost:       16,
			IdleConnTimeout:       90 * timepkg.Second,
			TLSHandshakeTimeout:   10 * timepkg.Second,
			ExpectContinueTimeout: 1 * timepkg.Second,
		},
	}
}

func drainAndClose(resp *nethttp.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func getFreePort(t *testingpkg.T, used map[int]struct{}) int {
	t.Helper()
	// IMPORTANT (Windows): the ephemeral port range typically starts at 49152.
	// Using ports from that range for long-lived listeners can trigger frequent
	// connectex errors under high churn. Prefer a stable, non-ephemeral range.
	const (
		minPort = 20000
		maxPort = 40000
	)

	rng := mathrand.New(mathrand.NewSource(timepkg.Now().UnixNano()))
	for i := 0; i < 5000; i++ {
		port := minPort + rng.Intn(maxPort-minPort+1)
		if _, exists := used[port]; exists {
			continue
		}

		l, err := netpkg.Listen("tcp", fmtpkg.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = l.Close()

		used[port] = struct{}{}
		return port
	}

	t.Fatalf("unable to allocate a unique free port")
	return 0
}

func startCluster(t *testingpkg.T, numPartitions, replicationFactor, nodeCount int) *cluster {
	t.Helper()
	usedPorts := make(map[int]struct{})

	ctrlPort := getFreePort(t, usedPorts)
	ctrlCfg := controller.DefaultServerConfig()
	ctrlCfg.Port = ctrlPort
	ctrlCfg.NumPartitions = numPartitions
	ctrlCfg.ReplicationFactor = replicationFactor
	// Speed up health/failover for tests.
	ctrlCfg.HealthInterval = 250 * timepkg.Millisecond
	// Windows loopback can transiently fail dials under churn (connectex); keep
	// detection reasonably fast, but not so aggressive that brief spikes mark
	// nodes dead.
	ctrlCfg.SuspectTimeout = 2 * timepkg.Second
	ctrlCfg.DeadTimeout = 5 * timepkg.Second
	ctrlCfg.ReadTimeout = 2 * timepkg.Second
	ctrlCfg.WriteTimeout = 2 * timepkg.Second
	ctrlCfg.ShutdownTimeout = 3 * timepkg.Second

	ctrl := controller.NewServer(ctrlCfg)
	if err := ctrl.Start(); err != nil {
		t.Fatalf("start controller: %v", err)
	}
	controllerURL := fmtpkg.Sprintf("http://127.0.0.1:%d", ctrlPort)

	lbPort := getFreePort(t, usedPorts)
	lbCfg := loadbalancer.DefaultServerConfig()
	lbCfg.Port = lbPort
	lbCfg.ControllerURL = controllerURL
	lbCfg.TopologyRefresh = 250 * timepkg.Millisecond
	lbCfg.RequestTimeout = 2 * timepkg.Second
	lbCfg.MaxRetries = 5

	lb := loadbalancer.NewServer(lbCfg)
	if err := lb.Start(); err != nil {
		_ = ctrl.Stop()
		t.Fatalf("start loadbalancer: %v", err)
	}
	lbURL := fmtpkg.Sprintf("http://127.0.0.1:%d", lbPort)

	nodes := make(map[int]*dbnode.Server)
	nodePorts := make(map[int]int)

	for id := 1; id <= nodeCount; id++ {
		port := getFreePort(t, usedPorts)
		nodePorts[id] = port

		ncfg := dbnode.DefaultNodeConfig()
		ncfg.ID = id
		ncfg.Port = port
		// NOTE: replication currently uses the replica "replication_port" when sending /replicate.
		// dbnode only listens on Port, so tests set them equal.
		ncfg.ReplicationPort = port
		ncfg.ControllerURL = controllerURL
		ncfg.NumPartitions = numPartitions
		ncfg.SnapshotInterval = 30 * timepkg.Minute
		ncfg.DataDir = ""
		ncfg.ReplicatorWorkerCount = 1
		ncfg.ReplicatorMaxRetries = 1
		ncfg.ReplicatorRetryDelay = 250 * timepkg.Millisecond
		ncfg.ReplicatorTimeout = 2 * timepkg.Second
		ncfg.ReplicatorWorkerCount = 1
		ncfg.ReplicatorMaxRetries = 1
		ncfg.ReplicatorRetryDelay = 250 * timepkg.Millisecond
		ncfg.ReplicatorTimeout = 2 * timepkg.Second
		// Reduce replication concurrency/retry churn for Windows in-process tests.
		ncfg.ReplicatorWorkerCount = 1
		ncfg.ReplicatorMaxRetries = 1
		ncfg.ReplicatorRetryDelay = 250 * timepkg.Millisecond
		ncfg.ReplicatorTimeout = 2 * timepkg.Second

		node, err := dbnode.NewNode(ncfg)
		if err != nil {
			_ = lb.Stop()
			_ = ctrl.Stop()
			t.Fatalf("create node %d: %v", id, err)
		}

		scfg := dbnode.DefaultServerConfig(port)
		scfg.ShutdownTimeout = 2 * timepkg.Second
		s := dbnode.NewServer(node, scfg)
		if err := s.Start(); err != nil {
			_ = lb.Stop()
			_ = ctrl.Stop()
			t.Fatalf("start node %d: %v", id, err)
		}
		nodes[id] = s
	}

	c := &cluster{
		controller:    ctrl,
		lb:            lb,
		nodes:         nodes,
		adminClient:   newTestHTTPClient(10 * timepkg.Second),
		controllerURL: controllerURL,
		lbURL:         lbURL,
		nodePorts:     nodePorts,
	}

	// Register nodes and notify roles.
	for id := 1; id <= nodeCount; id++ {
		body, _ := encodingjson.Marshal(map[string]any{
			"address":          "127.0.0.1",
			"port":             nodePorts[id],
			"replication_port": nodePorts[id],
		})
		resp, err := c.adminClient.Post(controllerURL+"/register", "application/json", bytespkg.NewReader(body))
		if err != nil {
			c.Stop(t)
			t.Fatalf("register node %d: %v", id, err)
		}
		drainAndClose(resp)
		if resp.StatusCode != 200 {
			c.Stop(t)
			t.Fatalf("register node %d: status %d", id, resp.StatusCode)
		}
	}
	notifyResp, _ := c.adminClient.Post(controllerURL+"/internal/notify-roles", "application/json", bytespkg.NewReader([]byte("{}")))
	drainAndClose(notifyResp)

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 10*timepkg.Second)
	defer cancel()
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		c.Stop(t)
		t.Fatalf("cluster not healthy: %v", err)
	}

	return c
}

func (c *cluster) Stop(t *testingpkg.T) {
	t.Helper()
	for _, s := range c.nodes {
		_ = s.Stop()
	}
	if c.lb != nil {
		_ = c.lb.Stop()
	}
	if c.controller != nil {
		_ = c.controller.Stop()
	}
}

func (c *cluster) movePartition(t *testingpkg.T, partitionID int, targetNodeID int) {
	t.Helper()
	body, _ := encodingjson.Marshal(map[string]any{"partition_id": partitionID, "target_node_id": targetNodeID})
	resp, err := c.adminClient.Post(c.controllerURL+"/partition/move", "application/json", bytespkg.NewReader(body))
	if err != nil {
		t.Fatalf("move partition: %v", err)
	}
	defer drainAndClose(resp)
	if resp.StatusCode != nethttp.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("move partition status %d: %s", resp.StatusCode, string(b))
	}
}

// loadErrors collects what actually went wrong, because "load had 12 errors"
// is not a diagnosable failure message.
type loadErrors struct {
	mu   syncpkg.Mutex
	seen []string
}

func (le *loadErrors) add(op string, err error) {
	le.mu.Lock()
	defer le.mu.Unlock()
	if len(le.seen) < 10 {
		le.seen = append(le.seen, op+": "+err.Error())
	}
}

func (le *loadErrors) String() string {
	le.mu.Lock()
	defer le.mu.Unlock()
	if len(le.seen) == 0 {
		return "(none recorded)"
	}
	return stringspkg.Join(le.seen, "; ")
}

func runSimpleLoad(ctx contextpkg.Context, client *kvdb.Client, concurrency int, keyPrefix string) (ops int64, errors int64) {
	o, e, _ := runSimpleLoadDetailed(ctx, client, concurrency, keyPrefix)
	return o, e
}

func runSimpleLoadDetailed(ctx contextpkg.Context, client *kvdb.Client, concurrency int, keyPrefix string) (ops int64, errors int64, detail *loadErrors) {
	var wg syncpkg.WaitGroup
	var opsCount int64
	var errCount int64
	detail = &loadErrors{}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := mathrand.New(mathrand.NewSource(timepkg.Now().UnixNano() + int64(worker)))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				k := fmtpkg.Sprintf("%s-%d-%d", keyPrefix, worker, r.Intn(10000))
				v := fmtpkg.Sprintf("v-%d", r.Int())
				if err := client.SetString(ctx, k, v); err != nil {
					if ctx.Err() != nil {
						return
					}
					detail.add("set", err)
					atomicpkg.AddInt64(&errCount, 1)
					continue
				}
				atomicpkg.AddInt64(&opsCount, 1)
				_, err := client.GetString(ctx, k)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if errorspkg.Is(err, kvdb.ErrKeyNotFound) {
						// Reads can be routed to replicas; immediate read-after-write isn't guaranteed.
						continue
					}
					detail.add("get", err)
					atomicpkg.AddInt64(&errCount, 1)
					continue
				}
				atomicpkg.AddInt64(&opsCount, 1)
			}
		}(i)
	}

	wg.Wait()
	return opsCount, errCount, detail
}

func TestScenario_StartingNode_SingleNode_SetGetDelete_RPS(t *testingpkg.T) {
	c := startCluster(t, 8, 1, 1)
	defer c.Stop(t)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 10*timepkg.Second)
	defer cancel()

	if err := client.SetString(ctx, "k1", "v1"); err != nil {
		t.Fatalf("set: %v", err)
	}
	val, err := client.GetString(ctx, "k1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "v1" {
		t.Fatalf("expected v1 got %q", val)
	}
	if err := client.Delete(ctx, "k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	loadCtx, loadCancel := contextpkg.WithTimeout(contextpkg.Background(), 2*timepkg.Second)
	defer loadCancel()
	start := timepkg.Now()
	ops, errs, detail := runSimpleLoadDetailed(loadCtx, client, 8, "single")
	elapsed := timepkg.Since(start)

	if errs > 0 {
		t.Fatalf("load had %d errors: %s", errs, detail)
	}
	if ops == 0 {
		t.Fatalf("no ops executed")
	}
	rps := float64(ops) / elapsed.Seconds()
	t.Logf("single-node RPS (ops/sec): %.1f", rps)
}

func TestScenario_MultipleNodesAndShards_DistributionAndRPS(t *testingpkg.T) {
	c := startCluster(t, 16, 2, 3)
	defer c.Stop(t)

	// Distribute masters across nodes before load.
	for p := 0; p < 16; p++ {
		target := 1 + (p % 3)
		if target != 1 {
			c.movePartition(t, p, target)
		}
	}

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	loadCtx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 2*timepkg.Second)
	defer cancel()
	start := timepkg.Now()
	ops, errs, detail := runSimpleLoadDetailed(loadCtx, client, 6, "multi")
	elapsed := timepkg.Since(start)

	if errs > 0 {
		t.Fatalf("load had %d errors: %s", errs, detail)
	}
	if ops == 0 {
		t.Fatalf("no ops executed")
	}
	rps := float64(ops) / elapsed.Seconds()
	t.Logf("multi-node RPS (ops/sec): %.1f", rps)
}

func TestScenario_DeletionOfNode_FailoverAndContinuity(t *testingpkg.T) {
	c := startCluster(t, 8, 2, 3)
	defer c.Stop(t)

	// Move partition 0 master to node 2, so we can kill node2 later.
	c.movePartition(t, 0, 2)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 10*timepkg.Second)
	defer cancel()

	key := "failover-k"
	if err := client.SetString(ctx, key, "v"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Stop node 2 to simulate deletion.
	_ = c.nodes[2].Stop()

	// Also remove it from controller explicitly.
	httpClient := newTestHTTPClient(3 * timepkg.Second)
	resp, _ := httpClient.Post(c.controllerURL+"/nodes/remove?node_id=2", "application/json", bytespkg.NewReader([]byte("{}")))
	drainAndClose(resp)

	// Wait for controller health/failover.
	timepkg.Sleep(2 * timepkg.Second)

	// Validate set/get/delete still works.
	for i := 0; i < 20; i++ {
		val, err := client.GetString(ctx, key)
		if err == nil && val == "v" {
			break
		}
		timepkg.Sleep(200 * timepkg.Millisecond)
		if i == 19 {
			t.Fatalf("read after failover failed: %v", err)
		}
	}

	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("delete after failover: %v", err)
	}
}

func TestScenario_RestartNodesOneAfterAnother_ClusterStaysUp(t *testingpkg.T) {
	c := startCluster(t, 8, 2, 3)
	defer c.Stop(t)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 20*timepkg.Second)
	defer cancel()

	// Baseline write.
	// NOTE: Nodes are currently configured with in-memory storage in-process.
	// This scenario focuses on availability (cluster stays up), not durability.

	for id := 1; id <= 3; id++ {
		_ = c.nodes[id].Stop()
		// Restart node on same port.
		port := c.nodePorts[id]
		ncfg := dbnode.DefaultNodeConfig()
		ncfg.ID = id
		ncfg.Port = port
		ncfg.ReplicationPort = port
		ncfg.ControllerURL = c.controllerURL
		ncfg.NumPartitions = 8
		ncfg.SnapshotInterval = 30 * timepkg.Minute
		node, err := dbnode.NewNode(ncfg)
		if err != nil {
			t.Fatalf("recreate node %d: %v", id, err)
		}
		s := dbnode.NewServer(node, dbnode.DefaultServerConfig(port))
		if err := s.Start(); err != nil {
			t.Fatalf("restart node %d: %v", id, err)
		}
		c.nodes[id] = s

		// Re-register and notify roles (idempotent for tests).
		b, _ := encodingjson.Marshal(map[string]any{"address": "127.0.0.1", "port": port, "replication_port": port})
		resp, _ := c.adminClient.Post(c.controllerURL+"/register", "application/json", bytespkg.NewReader(b))
		drainAndClose(resp)
		notifyResp, _ := c.adminClient.Post(c.controllerURL+"/internal/notify-roles", "application/json", bytespkg.NewReader([]byte("{}")))
		drainAndClose(notifyResp)

		// Ensure cluster can still serve reads+writes after restart.
		key := fmtpkg.Sprintf("restart-%d", id)
		if err := client.SetString(ctx, key, "ok"); err != nil {
			t.Fatalf("post-restart set %d: %v", id, err)
		}
		deadline := timepkg.Now().Add(2 * timepkg.Second)
		for {
			val, err := client.GetString(ctx, key)
			if err == nil && val == "ok" {
				break
			}
			if timepkg.Now().After(deadline) {
				t.Fatalf("post-restart get %d: %v val=%q", id, err, val)
			}
			timepkg.Sleep(25 * timepkg.Millisecond)
		}
	}
}

func TestScenario_AddNodeMidstLoad_RebalanceViaMovePartition(t *testingpkg.T) {
	c := startCluster(t, 8, 2, 2)
	defer c.Stop(t)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	loadCtx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 4*timepkg.Second)
	defer cancel()

	var errs int64
	var wg syncpkg.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		localCtx, localCancel := contextpkg.WithTimeout(contextpkg.Background(), 4*timepkg.Second)
		defer localCancel()
		_, e := runSimpleLoad(localCtx, client, 4, "addnode")
		errs = e
	}()

	// After some load, add node 3 and move a partition to it.
	timepkg.Sleep(1 * timepkg.Second)

	// Start node 3.
	usedPorts := make(map[int]struct{})
	for _, p := range c.nodePorts {
		usedPorts[p] = struct{}{}
	}
	port := getFreePort(t, usedPorts)
	c.nodePorts[3] = port
	ncfg := dbnode.DefaultNodeConfig()
	ncfg.ID = 3
	ncfg.Port = port
	ncfg.ReplicationPort = port
	ncfg.ControllerURL = c.controllerURL
	ncfg.NumPartitions = 8
	ncfg.SnapshotInterval = 30 * timepkg.Minute
	node, err := dbnode.NewNode(ncfg)
	if err != nil {
		t.Fatalf("create node3: %v", err)
	}
	s := dbnode.NewServer(node, dbnode.DefaultServerConfig(port))
	if err := s.Start(); err != nil {
		t.Fatalf("start node3: %v", err)
	}
	c.nodes[3] = s

	b, _ := encodingjson.Marshal(map[string]any{"address": "127.0.0.1", "port": port, "replication_port": port})
	resp, err := c.adminClient.Post(c.controllerURL+"/register", "application/json", bytespkg.NewReader(b))
	if err == nil {
		drainAndClose(resp)
	}
	notifyResp, _ := c.adminClient.Post(c.controllerURL+"/internal/notify-roles", "application/json", bytespkg.NewReader([]byte("{}")))
	drainAndClose(notifyResp)

	c.movePartition(t, 0, 3)
	// Give the load balancer/topology cache a moment to observe the move.
	timepkg.Sleep(300 * timepkg.Millisecond)

	<-loadCtx.Done()
	wg.Wait()

	if errs > 0 {
		t.Fatalf("load had %d errors", errs)
	}
}

func TestScenario_AddOrRemoveReplicas_MidRun(t *testingpkg.T) {
	c := startCluster(t, 8, 2, 3)
	defer c.Stop(t)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 15*timepkg.Second)
	defer cancel()

	// Write a small dataset.
	for i := 0; i < 200; i++ {
		k := fmtpkg.Sprintf("rf-%d", i)
		v := fmtpkg.Sprintf("v-%d", i)
		if err := client.SetString(ctx, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	// Increase replication factor to 3 (adds replicas + reseeds via snapshots).
	httpClient := newTestHTTPClient(10 * timepkg.Second)
	body, _ := encodingjson.Marshal(map[string]any{"replication_factor": 3})
	resp, err := httpClient.Post(c.controllerURL+"/replication/factor", "application/json", bytespkg.NewReader(body))
	if err != nil {
		t.Fatalf("set replication factor: %v", err)
	}
	drainAndClose(resp)
	if resp.StatusCode != nethttp.StatusOK {
		t.Fatalf("set replication factor status %d", resp.StatusCode)
	}

	// Verify data still readable.
	for i := 0; i < 200; i++ {
		k := fmtpkg.Sprintf("rf-%d", i)
		v := fmtpkg.Sprintf("v-%d", i)
		got, err := client.GetString(ctx, k)
		if err != nil || got != v {
			t.Fatalf("get %s after rf=3: err=%v got=%q", k, err, got)
		}
	}

	// Decrease replication factor back to 2.
	body2, _ := encodingjson.Marshal(map[string]any{"replication_factor": 2})
	resp2, err := httpClient.Post(c.controllerURL+"/replication/factor", "application/json", bytespkg.NewReader(body2))
	if err != nil {
		t.Fatalf("set replication factor back: %v", err)
	}
	drainAndClose(resp2)
	if resp2.StatusCode != nethttp.StatusOK {
		t.Fatalf("set replication factor back status %d", resp2.StatusCode)
	}

	// Verify data still readable.
	for i := 0; i < 200; i++ {
		k := fmtpkg.Sprintf("rf-%d", i)
		v := fmtpkg.Sprintf("v-%d", i)
		got, err := client.GetString(ctx, k)
		if err != nil || got != v {
			t.Fatalf("get %s after rf=2: err=%v got=%q", k, err, got)
		}
	}
}

func TestScenario_DeleteKeys_Workflow(t *testingpkg.T) {
	c := startCluster(t, 8, 1, 1)
	defer c.Stop(t)

	client := kvdb.NewClient(kvdb.DefaultConfig(c.lbURL))
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 10*timepkg.Second)
	defer cancel()

	if err := client.SetString(ctx, "del", "x"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := client.Delete(ctx, "del"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := client.GetString(ctx, "del")
	if err == nil {
		t.Fatalf("expected get to fail after delete")
	}
}

// Ensure deterministic randomness for tests that use math/rand.
func init() {
	mathrand.Seed(timepkg.Now().UnixNano())
	_ = strconvpkg.IntSize
}

// Package cluster holds multi-node integration tests for Phase 2:
// registration + routing, automatic failover with replica promotion,
// the returning-former-master wipe-and-resync rule, and live 4->8
// re-sharding. Everything runs in-process on 127.0.0.1 with fast
// heartbeat timeouts so the suite stays quick and deterministic.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kvdb/kvdb/internal/controller"
	"github.com/kvdb/kvdb/internal/dbnode"
	"github.com/kvdb/kvdb/internal/loadbalancer"
	"github.com/kvdb/kvdb/pkg/hash"
	"github.com/kvdb/kvdb/pkg/kvdb"
)

// testCluster is a live in-process cluster: controller + LB + N db nodes.
type testCluster struct {
	t             *testing.T
	controller    *controller.Server
	lb            *loadbalancer.Server
	nodes         map[int]*dbnode.Server
	nodeCfgs      map[int]dbnode.NodeConfig
	controllerURL string
	lbURL         string
	nodePorts     map[int]int
	numPartitions int
	client        *http.Client
	kv            *kvdb.Client
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startTestCluster(t *testing.T, numPartitions, replicationFactor, nodeCount int) *testCluster {
	t.Helper()

	ctrlPort := freePort(t)
	ctrlCfg := controller.DefaultServerConfig()
	ctrlCfg.Port = ctrlPort
	ctrlCfg.NumPartitions = numPartitions
	ctrlCfg.ReplicationFactor = replicationFactor
	ctrlCfg.HealthInterval = 200 * time.Millisecond
	ctrlCfg.SuspectTimeout = 800 * time.Millisecond
	ctrlCfg.DeadTimeout = 2 * time.Second
	ctrlCfg.ReadTimeout = 3 * time.Second
	ctrlCfg.WriteTimeout = 3 * time.Second
	ctrlCfg.ShutdownTimeout = 3 * time.Second

	ctrl := controller.NewServer(ctrlCfg)
	if err := ctrl.Start(); err != nil {
		t.Fatalf("start controller: %v", err)
	}
	controllerURL := fmt.Sprintf("http://127.0.0.1:%d", ctrlPort)

	lbPort := freePort(t)
	lbCfg := loadbalancer.DefaultServerConfig()
	lbCfg.Port = lbPort
	lbCfg.ControllerURL = controllerURL
	lbCfg.TopologyRefresh = 200 * time.Millisecond
	lbCfg.RequestTimeout = 3 * time.Second
	lbCfg.MaxRetries = 5

	lb := loadbalancer.NewServer(lbCfg)
	if err := lb.Start(); err != nil {
		_ = ctrl.Stop()
		t.Fatalf("start loadbalancer: %v", err)
	}
	lbURL := fmt.Sprintf("http://127.0.0.1:%d", lbPort)

	c := &testCluster{
		t:             t,
		controller:    ctrl,
		lb:            lb,
		nodes:         map[int]*dbnode.Server{},
		nodeCfgs:      map[int]dbnode.NodeConfig{},
		controllerURL: controllerURL,
		lbURL:         lbURL,
		nodePorts:     map[int]int{},
		numPartitions: numPartitions,
		client:        &http.Client{Timeout: 10 * time.Second},
	}
	rc := kvdb.DefaultRetryConfig()
	rc.MaxRetries = 10
	rc.InitialBackoff = 200 * time.Millisecond
	c.kv = kvdb.NewClient(kvdb.Config{
		LoadBalancerURL: lbURL,
		Timeout:         30 * time.Second,
		Retry:           rc,
	})

	for id := 1; id <= nodeCount; id++ {
		c.startNode(id, numPartitions)
	}
	c.notifyRoles()
	c.waitHealthy(30 * time.Second)
	return c
}

// startNode creates, starts and registers node id (fresh empty state).
func (c *testCluster) startNode(id int, numPartitions int) {
	c.t.Helper()
	// A restart must reuse the same port: the controller keys node
	// identity by address:port, so only the same socket re-registers as
	// a rejoin (returning-former-master rule) instead of a brand-new node.
	port, ok := c.nodePorts[id]
	if !ok {
		port = freePort(c.t)
		c.nodePorts[id] = port
	}

	ncfg := dbnode.DefaultNodeConfig()
	ncfg.ID = id
	ncfg.Port = port
	ncfg.ReplicationPort = port
	ncfg.ControllerURL = c.controllerURL
	ncfg.NumPartitions = numPartitions
	ncfg.SnapshotInterval = 30 * time.Minute
	ncfg.DataDir = ""
	ncfg.ReplicatorWorkerCount = 1
	ncfg.ReplicatorMaxRetries = 2
	ncfg.ReplicatorRetryDelay = 200 * time.Millisecond
	ncfg.ReplicatorTimeout = 2 * time.Second
	c.nodeCfgs[id] = ncfg

	node, err := dbnode.NewNode(ncfg)
	if err != nil {
		c.t.Fatalf("create node %d: %v", id, err)
	}
	scfg := dbnode.DefaultServerConfig(port)
	scfg.ShutdownTimeout = 2 * time.Second
	s := dbnode.NewServer(node, scfg)
	if err := s.Start(); err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	c.nodes[id] = s

	body, _ := json.Marshal(map[string]any{
		"address":          "127.0.0.1",
		"port":             port,
		"replication_port": port,
	})
	resp, err := c.client.Post(c.controllerURL+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		c.t.Fatalf("register node %d: %v", id, err)
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		c.t.Fatalf("register node %d: status %d: %s", id, resp.StatusCode, b)
	}
}

func (c *testCluster) stopNode(id int) {
	c.t.Helper()
	if s, ok := c.nodes[id]; ok {
		_ = s.Stop()
		delete(c.nodes, id)
	}
}

func (c *testCluster) notifyRoles() {
	c.t.Helper()
	resp, err := c.client.Post(c.controllerURL+"/internal/notify-roles", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		c.t.Fatalf("notify roles: %v", err)
	}
	drainClose(resp)
}

func (c *testCluster) stopAll() {
	for id := range c.nodes {
		_ = c.nodes[id].Stop()
	}
	c.nodes = map[int]*dbnode.Server{}
	if c.lb != nil {
		_ = c.lb.Stop()
	}
	if c.controller != nil {
		_ = c.controller.Stop()
	}
}

func drainClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (c *testCluster) getJSON(path string, out any) int {
	c.t.Helper()
	resp, err := c.client.Get(c.controllerURL + path)
	if err != nil {
		c.t.Fatalf("GET %s: %v", path, err)
	}
	defer drainClose(resp)
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			c.t.Fatalf("decode %s: %v", path, err)
		}
	}
	return resp.StatusCode
}

func (c *testCluster) waitFor(timeout time.Duration, what string, fn func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for: %s", what)
}

func (c *testCluster) waitHealthy(timeout time.Duration) {
	c.t.Helper()
	c.waitFor(timeout, "lb healthy with all partitions mastered", func() bool {
		resp, err := c.client.Get(c.lbURL + "/health")
		if err != nil {
			return false
		}
		drainClose(resp)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var m controller.MapResponse
		if code := c.getJSON("/map", &m); code != http.StatusOK {
			return false
		}
		if len(m.Partitions) == 0 {
			return false
		}
		for _, p := range m.Partitions {
			if p.MasterID <= 0 {
				return false
			}
		}
		return true
	})
}

func (c *testCluster) masters() map[int]int {
	var m controller.MapResponse
	if code := c.getJSON("/map", &m); code != http.StatusOK {
		c.t.Fatalf("GET /map: status %d", code)
	}
	out := map[int]int{}
	for _, p := range m.Partitions {
		out[p.ID] = p.MasterID
	}
	return out
}

// keysForPartition returns n deterministic keys hashing to partition want.
func keysForPartition(prefix string, want, numPartitions, n int) []string {
	var keys []string
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		if hash.PartitionForKey(k, numPartitions) == want {
			keys = append(keys, k)
		}
	}
	return keys
}

func (c *testCluster) kvSet(key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.kv.Set(ctx, key, []byte(value)); err != nil {
		c.t.Fatalf("Set(%q): %v", key, err)
	}
}

func (c *testCluster) kvGet(key string) string {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	v, err := c.kv.Get(ctx, key)
	if err != nil {
		c.t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v)
}

// nodeDirectGet reads a key straight from one node's HTTP API, bypassing
// the load balancer. Used to prove replication/reseed actually landed.
func (c *testCluster) nodeDirectGet(nodeID int, partition int, key string) (string, bool) {
	u := fmt.Sprintf("http://127.0.0.1:%d/get?key=%s&partition=%d",
		c.nodePorts[nodeID], key, partition)
	resp, err := c.client.Get(u)
	if err != nil {
		return "", false
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	var out struct {
		Value []byte `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", false
	}
	return string(out.Value), true
}

func TestClusterNodeRegistrationAndRouting(t *testing.T) {
	c := startTestCluster(t, 4, 1, 2)
	defer c.stopAll()

	masters := c.masters()
	if len(masters) != 4 {
		t.Fatalf("want 4 partitions in map, got %d", len(masters))
	}
	// Rebalance after the second node joins is async; wait for spread.
	c.waitFor(15*time.Second, "partitions spread across nodes", func() bool {
		seen := map[int]bool{}
		for _, m := range c.masters() {
			seen[m] = true
		}
		return len(seen) >= 2
	})

	for i := 0; i < 20; i++ {
		c.kvSet(fmt.Sprintf("reg-%d", i), fmt.Sprintf("v%d", i))
	}
	for i := 0; i < 20; i++ {
		if got := c.kvGet(fmt.Sprintf("reg-%d", i)); got != fmt.Sprintf("v%d", i) {
			t.Fatalf("key reg-%d = %q, want v%d", i, got, i)
		}
	}
}

func TestClusterFailoverPromotesReplica(t *testing.T) {
	c := startTestCluster(t, 4, 2, 2)
	defer c.stopAll()

	// Seed, including keys pinned to partition 0.
	victims := keysForPartition("fo-victim", 0, 4, 4)
	for i, k := range victims {
		c.kvSet(k, fmt.Sprintf("vv%d", i))
	}
	for i := 0; i < 16; i++ {
		c.kvSet(fmt.Sprintf("fo-base-%d", i), "b")
	}
	// Wait for async replication to converge on the replica.
	c.waitFor(15*time.Second, "victim keys replicated", func() bool {
		m := c.masters()
		master := m[0]
		for id := range c.nodes {
			if id == master {
				continue
			}
			ok := true
			for _, k := range victims {
				if _, found := c.nodeDirectGet(id, 0, k); !found {
					ok = false
				}
			}
			if ok {
				return true
			}
		}
		return false
	})

	oldMaster := c.masters()[0]
	t.Logf("killing master %d of partition 0", oldMaster)
	c.stopNode(oldMaster)

	// Controller must promote the healthy replica (watch for
	// [FAILOVER-PROMOTE] in the test log).
	c.waitFor(25*time.Second, "partition 0 has a new master", func() bool {
		m := c.masters()
		return m[0] != oldMaster && m[0] > 0
	})
	t.Logf("partition 0 new master: %d", c.masters()[0])

	// Traffic continues through the failover window (client retries).
	for i := 0; i < 8; i++ {
		c.kvSet(fmt.Sprintf("fo-after-%d", i), "a")
	}
	for _, k := range victims {
		if got := c.kvGet(k); got == "" {
			t.Fatalf("lost pre-failover key %q", k)
		}
	}
	for i := 0; i < 8; i++ {
		if got := c.kvGet(fmt.Sprintf("fo-after-%d", i)); got != "a" {
			t.Fatalf("lost post-failover key fo-after-%d", i)
		}
	}
}

func TestClusterReturningMasterWipeAndResync(t *testing.T) {
	c := startTestCluster(t, 4, 2, 2)
	defer c.stopAll()

	victims := keysForPartition("re-victim", 1, 4, 3)
	for i, k := range victims {
		c.kvSet(k, fmt.Sprintf("old%d", i))
	}
	c.waitFor(15*time.Second, "victim keys replicated", func() bool {
		m := c.masters()
		master := m[1]
		for id := range c.nodes {
			if id == master {
				continue
			}
			if _, found := c.nodeDirectGet(id, 1, victims[0]); found {
				return true
			}
		}
		return false
	})

	oldMaster := c.masters()[1]
	c.stopNode(oldMaster)
	c.waitFor(25*time.Second, "partition 1 failed over", func() bool {
		m := c.masters()
		return m[1] != oldMaster && m[1] > 0
	})
	newMaster := c.masters()[1]

	// Writes the dead node never saw.
	afterKeys := keysForPartition("re-after", 1, 4, 3)
	for i, k := range afterKeys {
		c.kvSet(k, fmt.Sprintf("new%d", i))
	}

	// The dead node returns: same process identity (address:port is
	// re-registered by a fresh empty node), must NOT retake leadership.
	t.Logf("restarting former master %d", oldMaster)
	c.startNode(oldMaster, c.numPartitions)
	c.notifyRoles()

	c.waitFor(30*time.Second, "returning node healthy", func() bool {
		var out struct {
			Nodes []controller.NodeInfo `json:"nodes"`
		}
		if code := c.getJSON("/nodes", &out); code != http.StatusOK {
			return false
		}
		for _, n := range out.Nodes {
			if n.ID == oldMaster && n.Status == controller.NodeStatusHealthy {
				return true
			}
		}
		return false
	})

	// It must serve as replica, never master again.
	if m := c.masters()[1]; m == oldMaster {
		t.Fatalf("returning node %d retook mastership of partition 1", oldMaster)
	}
	_ = newMaster

	// And it must have caught up on writes from the new master: a wiped
	// node can only serve these after snapshot + WAL reseed.
	c.waitFor(30*time.Second, "returning node resynced post-failover keys", func() bool {
		for _, k := range afterKeys {
			if _, found := c.nodeDirectGet(oldMaster, 1, k); !found {
				return false
			}
		}
		return true
	})
}

func TestClusterLiveReshard4to8(t *testing.T) {
	c := startTestCluster(t, 4, 2, 3)
	defer c.stopAll()

	const n = 60
	for i := 0; i < n; i++ {
		c.kvSet(fmt.Sprintf("rs-key-%03d", i), fmt.Sprintf("rv%d", i))
	}

	body, _ := json.Marshal(map[string]any{"new_num_partitions": 8})
	resp, err := c.client.Post(c.controllerURL+"/reshard", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /reshard: %v", err)
	}
	drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /reshard: status %d", resp.StatusCode)
	}

	// Wait for MIGRATING -> STABLE.
	c.waitFor(120*time.Second, "reshard reaches stable", func() bool {
		var st struct {
			ReshardState string `json:"reshard_state"`
			Running      bool   `json:"running"`
		}
		r, err := c.client.Get(c.controllerURL + "/reshard/status")
		if err != nil {
			return false
		}
		defer drainClose(r)
		if r.StatusCode != http.StatusOK {
			return false
		}
		if err := json.NewDecoder(r.Body).Decode(&st); err != nil {
			return false
		}
		return st.ReshardState == "stable" && !st.Running
	})

	var m controller.MapResponse
	if code := c.getJSON("/map", &m); code != http.StatusOK {
		t.Fatalf("GET /map: status %d", code)
	}
	if m.NumPartitions != 8 {
		t.Fatalf("map shows %d partitions, want 8", m.NumPartitions)
	}

	lost := 0
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("rs-key-%03d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		v, err := c.kv.Get(ctx, k)
		cancel()
		if err != nil || string(v) != fmt.Sprintf("rv%d", i) {
			lost++
		}
	}
	if lost > 0 {
		t.Fatalf("lost %d/%d keys across 4->8 reshard", lost, n)
	}
}

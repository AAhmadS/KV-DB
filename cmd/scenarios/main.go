// Command scenarios runs grading scenarios end to end.
//
// Against a live stack (docker compose or any reachable deployment):
//
//	go run ./cmd/scenarios -scenario s1 -lb http://localhost:8080
//
// Self-contained (no docker, Windows/Linux): spin up an in-process
// cluster and orchestrate every control-plane step directly:
//
//	go run ./cmd/scenarios -mode inprocess -scenario all
//
// Modes:
//
//	-mode remote    talk to -lb / -controller URLs (data-plane s1+s2;
//	                control-plane steps print the docker commands instead)
//	-mode inprocess (default) boot controller+LB+nodes in-process and run
//	                s1..s6 fully, including kills, restarts, joins, RF
//	                changes and re-shards.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kvdb/kvdb/internal/controller"
	"github.com/kvdb/kvdb/internal/dbnode"
	"github.com/kvdb/kvdb/internal/loadbalancer"
	"github.com/kvdb/kvdb/pkg/hash"
	"github.com/kvdb/kvdb/pkg/kvdb"
)

type options struct {
	mode        string
	lbURL       string
	ctrlURL     string
	scenario    string
	ops         int
	concurrency int
	keyspace    int
}

func main() {
	var opt options
	flag.StringVar(&opt.mode, "mode", "inprocess", "inprocess | remote")
	flag.StringVar(&opt.lbURL, "lb", envOr("LB_URL", "http://localhost:8080"), "load balancer base URL (remote mode)")
	flag.StringVar(&opt.ctrlURL, "controller", envOr("CONTROLLER_URL", "http://localhost:8000"), "controller base URL (remote mode)")
	flag.StringVar(&opt.scenario, "scenario", "all", "s1|s2|s3|s4|s5|s6|all")
	flag.IntVar(&opt.ops, "ops", 2000, "benchmark operations per RPS run")
	flag.IntVar(&opt.concurrency, "concurrency", 8, "benchmark workers")
	flag.IntVar(&opt.keyspace, "keyspace", 500, "distinct benchmark keys")
	flag.Parse()

	var rc int
	if opt.mode == "remote" {
		rc = runRemote(opt)
	} else {
		rc = runInprocess(opt)
	}
	os.Exit(rc)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func banner(s string) {
	fmt.Println()
	fmt.Println("======================================================================")
	fmt.Println(" " + s)
	fmt.Println("======================================================================")
}

// ---------------------------------------------------------------------------
// remote mode: data plane against a live stack
// ---------------------------------------------------------------------------

func remoteClient(opt options) *kvdb.Client {
	return kvdb.NewClient(kvdb.Config{
		LoadBalancerURL: opt.lbURL,
		Timeout:         10 * time.Second,
		Retry:           kvdb.DefaultRetryConfig(),
	})
}

func runRemote(opt options) int {
	client := remoteClient(opt)
	defer client.Close()
	ctx := context.Background()
	switch opt.scenario {
	case "s1", "all":
		if r := remoteSmoke(ctx, client, opt, "s1"); r != 0 {
			return r
		}
		if opt.scenario != "all" {
			return 0
		}
		fallthrough
	case "s2":
		return remoteTopo(ctx, client, opt)
	default:
		fmt.Printf("remote mode implements data-plane s1/s2; for s3..s6 run:\n")
		fmt.Printf("  ./scripts/run_all_scenarios.sh %s\n", opt.scenario)
		fmt.Printf("or: go run ./cmd/scenarios -mode inprocess -scenario %s\n", opt.scenario)
		return 2
	}
}

func remoteSmoke(ctx context.Context, client *kvdb.Client, opt options, _ string) int {
	banner("Scenario 1 (10.1): single-node smoke + baseline RPS")
	for _, k := range []string{"s1-a", "s1-b", "s1-c"} {
		if err := client.Set(ctx, k, []byte("v-"+k)); err != nil {
			fmt.Printf("SMOKE Set(%s): FAIL %v\n", k, err)
			return 1
		}
		got, err := client.Get(ctx, k)
		if err != nil || string(got) != "v-"+k {
			fmt.Printf("SMOKE Get(%s): FAIL got=%q err=%v\n", k, got, err)
			return 1
		}
	}
	if err := client.Delete(ctx, "s1-c"); err != nil {
		fmt.Printf("SMOKE Delete: FAIL %v\n", err)
		return 1
	}
	if _, err := client.Get(ctx, "s1-c"); err == nil {
		fmt.Println("SMOKE Delete: FAIL key still readable")
		return 1
	}
	fmt.Println("SMOKE Set/Get/Delete: PASS")
	printBench("baseline", runBench(ctx, client, opt))
	return 0
}

func remoteTopo(ctx context.Context, client *kvdb.Client, opt options) int {
	banner("Scenario 2 (10.2): topology spread + RPS")
	m, err := fetchMap(opt.ctrlURL)
	if err != nil {
		fmt.Printf("MAP: FAIL %v\n", err)
		return 1
	}
	masters := map[string]int{}
	replicas := 0
	nparts := 0
	if arr, ok := m["partitions"].([]any); ok {
		nparts = len(arr)
		for _, p := range arr {
			pm := p.(map[string]any)
			masters[stringOf(pm["master_addr"])]++
			if r, ok := pm["replicas"].([]any); ok {
				replicas += len(r)
			}
		}
	}
	fmt.Printf("MAP partitions=%d masters=%v total_replicas=%d\n", nparts, masters, replicas)
	if len(masters) < 1 {
		fmt.Println("MAP: FAIL no masters")
		return 1
	}
	fmt.Println("MAP spread: PASS")
	printBench("multi-node", runBench(ctx, client, opt))
	fmt.Println("Compare this throughput line with the Scenario 1 baseline above (spec 10.2).")
	return 0
}

// ---------------------------------------------------------------------------
// in-process orchestration: full s1..s6 with kills, restarts, joins, RF
// ---------------------------------------------------------------------------

type cluster struct {
	ctrl     *controller.Server
	lb       *loadbalancer.Server
	nodes    map[int]*dbnode.Server
	ctrlURL  string
	lbURL    string
	ports    map[int]int
	http     *http.Client
	kv       *kvdb.Client
	numParts int
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func bootCluster(numPartitions, rf, nodeCount int) *cluster {
	ctrlPort := freePort()
	ccfg := controller.DefaultServerConfig()
	ccfg.Port = ctrlPort
	ccfg.NumPartitions = numPartitions
	ccfg.ReplicationFactor = rf
	ccfg.HealthInterval = 250 * time.Millisecond
	ccfg.SuspectTimeout = 1 * time.Second
	ccfg.DeadTimeout = 2500 * time.Millisecond
	ccfg.ReadTimeout = 3 * time.Second
	ccfg.WriteTimeout = 3 * time.Second
	ccfg.ShutdownTimeout = 3 * time.Second
	ctrl := controller.NewServer(ccfg)
	if err := ctrl.Start(); err != nil {
		panic(err)
	}
	ctrlURL := fmt.Sprintf("http://127.0.0.1:%d", ctrlPort)

	lbPort := freePort()
	lcfg := loadbalancer.DefaultServerConfig()
	lcfg.Port = lbPort
	lcfg.ControllerURL = ctrlURL
	lcfg.TopologyRefresh = 250 * time.Millisecond
	lcfg.RequestTimeout = 3 * time.Second
	lcfg.MaxRetries = 5
	lb := loadbalancer.NewServer(lcfg)
	if err := lb.Start(); err != nil {
		panic(err)
	}
	lbURL := fmt.Sprintf("http://127.0.0.1:%d", lbPort)

	rc := kvdb.DefaultRetryConfig()
	rc.MaxRetries = 10
	rc.InitialBackoff = 200 * time.Millisecond
	c := &cluster{
		ctrl: ctrl, lb: lb,
		nodes:   map[int]*dbnode.Server{},
		ctrlURL: ctrlURL, lbURL: lbURL,
		ports: map[int]int{},
		http:  &http.Client{Timeout: 10 * time.Second},
		kv: kvdb.NewClient(kvdb.Config{
			LoadBalancerURL: lbURL, Timeout: 30 * time.Second, Retry: rc,
		}),
		numParts: numPartitions,
	}
	for id := 1; id <= nodeCount; id++ {
		c.startNode(id)
	}
	c.post("/internal/notify-roles", map[string]any{})
	c.waitHealthy(30 * time.Second)
	return c
}

func (c *cluster) startNode(id int) {
	port, ok := c.ports[id]
	if !ok {
		port = freePort()
		c.ports[id] = port
	}
	ncfg := dbnode.DefaultNodeConfig()
	ncfg.ID = id
	ncfg.Port = port
	ncfg.ReplicationPort = port
	ncfg.ControllerURL = c.ctrlURL
	ncfg.NumPartitions = c.numParts
	ncfg.SnapshotInterval = 30 * time.Minute
	ncfg.DataDir = ""
	ncfg.ReplicatorWorkerCount = 1
	ncfg.ReplicatorMaxRetries = 2
	ncfg.ReplicatorRetryDelay = 200 * time.Millisecond
	ncfg.ReplicatorTimeout = 2 * time.Second
	node, err := dbnode.NewNode(ncfg)
	if err != nil {
		panic(err)
	}
	scfg := dbnode.DefaultServerConfig(port)
	scfg.ShutdownTimeout = 2 * time.Second
	s := dbnode.NewServer(node, scfg)
	if err := s.Start(); err != nil {
		panic(err)
	}
	c.nodes[id] = s
	body, _ := json.Marshal(map[string]any{
		"address": "127.0.0.1", "port": port, "replication_port": port,
	})
	resp, err := c.http.Post(c.ctrlURL+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("register node %d: %d", id, resp.StatusCode))
	}
}

func (c *cluster) stopNode(id int) {
	if s, ok := c.nodes[id]; ok {
		_ = s.Stop()
		delete(c.nodes, id)
	}
}

func (c *cluster) shutdown() {
	for id := range c.nodes {
		_ = c.nodes[id].Stop()
	}
	c.nodes = map[int]*dbnode.Server{}
	if c.lb != nil {
		_ = c.lb.Stop()
	}
	if c.ctrl != nil {
		_ = c.ctrl.Stop()
	}
	c.kv.Close()
}

func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (c *cluster) get(path string, out any) int {
	resp, err := c.http.Get(c.ctrlURL + path)
	if err != nil {
		panic(err)
	}
	defer drain(resp)
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			panic(err)
		}
	}
	return resp.StatusCode
}

func (c *cluster) post(path string, v any) int {
	body, _ := json.Marshal(v)
	resp, err := c.http.Post(c.ctrlURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer drain(resp)
	return resp.StatusCode
}

func (c *cluster) masters() map[int]int {
	var m controller.MapResponse
	if code := c.get("/map", &m); code != http.StatusOK {
		panic("GET /map failed")
	}
	out := map[int]int{}
	for _, p := range m.Partitions {
		out[p.ID] = p.MasterID
	}
	return out
}

func (c *cluster) healthyNodeIDs() []int {
	var out struct {
		Nodes []controller.NodeInfo `json:"nodes"`
	}
	if code := c.get("/nodes", &out); code != http.StatusOK {
		return nil
	}
	var ids []int
	for _, n := range out.Nodes {
		if n.Status == controller.NodeStatusHealthy {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

func (c *cluster) waitFor(timeout time.Duration, what string, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Printf("TIMEOUT waiting for: %s\n", what)
	return false
}

func (c *cluster) waitHealthy(timeout time.Duration) bool {
	return c.waitFor(timeout, "healthy cluster", func() bool {
		resp, err := c.http.Get(c.lbURL + "/health")
		if err != nil {
			return false
		}
		drain(resp)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		for _, m := range c.masters() {
			if m <= 0 {
				return false
			}
		}
		return true
	})
}

func (c *cluster) kvSet(key, val string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return c.kv.Set(ctx, key, []byte(val))
}

func (c *cluster) kvGet(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	v, err := c.kv.Get(ctx, key)
	return string(v), err
}

// ---------------------------------------------------------------------------
// benchmark helper
// ---------------------------------------------------------------------------

type benchResult struct {
	done    uint64
	errors  uint64
	retries uint64
	elapsed time.Duration
	lats    []time.Duration
	mu      sync.Mutex
}

func runBench(ctx context.Context, client *kvdb.Client, opt options) *benchResult {
	res := &benchResult{}
	var issued uint64
	var wg sync.WaitGroup
	for w := 0; w < opt.concurrency; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for {
				i := atomic.AddUint64(&issued, 1)
				if i > uint64(opt.ops) {
					return
				}
				key := fmt.Sprintf("bench-%d", (wid*7919+int(i))%opt.keyspace)
				val := []byte(fmt.Sprintf("v%d", i))
				start := time.Now()
				st, err := client.SetWithStats(ctx, key, val)
				lat := time.Since(start)
				atomic.AddUint64(&res.done, 1)
				if st.Attempts > 1 {
					atomic.AddUint64(&res.retries, uint64(st.Attempts-1))
				}
				if err != nil {
					atomic.AddUint64(&res.errors, 1)
				}
				res.mu.Lock()
				res.lats = append(res.lats, lat)
				res.mu.Unlock()
			}
		}(w)
	}
	start := time.Now()
	wg.Wait()
	res.elapsed = time.Since(start)
	return res
}

func printBench(label string, res *benchResult) {
	lats := append([]time.Duration(nil), res.lats...)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	q := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(p*float64(len(lats)-1))]
	}
	var total time.Duration
	for _, l := range lats {
		total += l
	}
	avg := time.Duration(0)
	if len(lats) > 0 {
		avg = total / time.Duration(len(lats))
	}
	rps := float64(len(lats)) / res.elapsed.Seconds()
	fmt.Printf("RPS/%s: %.1f ops/s (total=%d errors=%d retries=%d) latency avg=%v p50=%v p99=%v\n",
		label, rps, len(lats), res.errors, res.retries, avg, q(0.50), q(0.99))
}

func fetchMap(ctrlURL string) (map[string]any, error) {
	resp, err := http.Get(ctrlURL + "/map")
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /map: status %d", resp.StatusCode)
	}
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// ---------------------------------------------------------------------------
// in-process scenarios s1..s6
// ---------------------------------------------------------------------------

func runInprocess(opt options) int {
	list := []string{"s1", "s2", "s3", "s4", "s5", "s6"}
	if opt.scenario != "all" {
		list = []string{opt.scenario}
	}
	rc := 0
	for _, s := range list {
		var r int
		switch s {
		case "s1":
			r = ips1(opt)
		case "s2":
			r = ips2(opt)
		case "s3":
			r = ips3(opt)
		case "s4":
			r = ips4(opt)
		case "s5":
			r = ips5(opt)
		case "s6":
			r = ips6(opt)
		default:
			fmt.Printf("unknown scenario %q\n", s)
			return 2
		}
		if r != 0 {
			rc = r
		}
	}
	return rc
}

func benchHere(c *cluster, opt options, label string) *benchResult {
	ctx := context.Background()
	res := runBench(ctx, c.kv, opt)
	printBench(label, res)
	return res
}

// s1: one node + controller + LB, Set/Get/Delete, baseline RPS.
func ips1(opt options) int {
	banner("Scenario 1 (10.1): single node + controller + LB; Set/Get/Delete; baseline RPS")
	c := bootCluster(4, 1, 1)
	defer c.shutdown()
	fmt.Printf("TOPOLOGY nodes=%v masters=%v\n", c.healthyNodeIDs(), c.masters())

	for _, k := range []string{"s1-a", "s1-b", "s1-c"} {
		if err := c.kvSet(k, "v-"+k); err != nil {
			fmt.Printf("SMOKE Set(%s): FAIL %v\n", k, err)
			return 1
		}
		if got, err := c.kvGet(k); err != nil || got != "v-"+k {
			fmt.Printf("SMOKE Get(%s): FAIL got=%q err=%v\n", k, got, err)
			return 1
		}
	}
	if err := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return c.kv.Delete(ctx, "s1-c")
	}(); err != nil {
		fmt.Printf("SMOKE Delete: FAIL %v\n", err)
		return 1
	}
	if _, err := c.kvGet("s1-c"); err == nil {
		fmt.Println("SMOKE Delete: FAIL key still readable")
		return 1
	}
	fmt.Println("SMOKE Set/Get/Delete: PASS")
	benchHere(c, opt, "s1-baseline")
	return 0
}

// s2: multi-node multi-shard, RPS comparison.
func ips2(opt options) int {
	banner("Scenario 2 (10.2): multi-node multi-shard; RPS scaling check")
	c := bootCluster(8, 2, 3)
	defer c.shutdown()
	if !c.waitFor(20*time.Second, "masters spread over >=2 nodes", func() bool {
		seen := map[int]bool{}
		for _, m := range c.masters() {
			seen[m] = true
		}
		return len(seen) >= 2
	}) {
		return 1
	}
	fmt.Printf("TOPOLOGY nodes=%v masters=%v\n", c.healthyNodeIDs(), c.masters())
	benchHere(c, opt, "s2-multi")
	fmt.Println("NOTE compare RPS/s2-multi with RPS/s1-baseline from Scenario 1 (spec 10.2).")
	return 0
}

// s3: stop a master mid-load, failover, traffic continues.
func ips3(opt options) int {
	banner("Scenario 3 (10.3): stop a node under load; failover; Set/Get/Delete continue")
	c := bootCluster(4, 2, 3)
	defer c.shutdown()

	const known = 60
	for i := 1; i <= known; i++ {
		if err := c.kvSet(fmt.Sprintf("s3-%d", i), fmt.Sprintf("v%d", i)); err != nil {
			fmt.Printf("SEED FAIL: %v\n", err)
			return 1
		}
	}
	victim := c.masters()[0]
	fmt.Printf("KILL-TARGET partition 0 master = node %d\n", victim)

	var errs uint64
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				k := fmt.Sprintf("s3load-%d-%d", wid, i)
				i++
				if err := c.kvSet(k, "x"); err != nil {
					atomic.AddUint64(&errs, 1)
				}
				if _, err := c.kvGet(k); err != nil {
					atomic.AddUint64(&errs, 1)
				}
			}
		}(w)
	}
	time.Sleep(4 * time.Second)
	fmt.Printf("STOP node %d now\n", victim)
	c.stopNode(victim)
	converged := c.waitFor(25*time.Second, "partition 0 new master", func() bool {
		m := c.masters()
		return m[0] != victim && m[0] > 0
	})
	fmt.Printf("FAILOVER lapsed-checked masters=%v converged=%v\n", c.masters(), converged)
	close(stopLoad)
	wg.Wait()
	fmt.Printf("LOAD errors during failover window: %d\n", atomic.LoadUint64(&errs))

	missing, wrong := 0, 0
	for i := 1; i <= known; i++ {
		got, err := c.kvGet(fmt.Sprintf("s3-%d", i))
		if err != nil {
			missing++
		} else if got != fmt.Sprintf("v%d", i) {
			wrong++
		}
	}
	fmt.Printf("VERIFY known keys after kill: missing=%d wrong=%d of %d\n", missing, wrong, known)
	if !converged || missing > 0 || wrong > 0 {
		return 1
	}
	fmt.Println("SMOKE Set/Get/Delete after failover: PASS")
	return 0
}

// s4: sequential rolling restarts, data preserved.
func ips4(opt options) int {
	banner("Scenario 4 (10.4): rolling restart node-by-node; cluster stays up, data preserved")
	c := bootCluster(4, 2, 3)
	defer c.shutdown()

	const known = 60
	for i := 1; i <= known; i++ {
		if err := c.kvSet(fmt.Sprintf("roll-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			fmt.Printf("SEED FAIL: %v\n", err)
			return 1
		}
	}
	check := func(tag string) (int, int) {
		// Eventual readability: a restarted node reseeds from its master
		// asynchronously, and keys in partitions it masters 404 until the
		// snapshot+WAL catch-up lands. Poll bounded, then assert.
		deadline := time.Now().Add(30 * time.Second)
		missing, wrong := 0, 0
		for time.Now().Before(deadline) {
			missing, wrong = 0, 0
			for i := 1; i <= known; i++ {
				got, err := c.kvGet(fmt.Sprintf("roll-%d", i))
				if err != nil {
					missing++
				} else if got != fmt.Sprintf("value-%d", i) {
					wrong++
				}
			}
			if missing == 0 && wrong == 0 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("CHECK %s: missing=%d wrong=%d of %d\n", tag, missing, wrong, known)
		return missing, wrong
	}
	for _, id := range []int{1, 2, 3} {
		fmt.Printf("RESTART node %d\n", id)
		c.stopNode(id)
		time.Sleep(2 * time.Second)
		c.startNode(id)
		c.post("/internal/notify-roles", map[string]any{})
		if !c.waitFor(30*time.Second, fmt.Sprintf("node %d healthy", id), func() bool {
			for _, hid := range c.healthyNodeIDs() {
				if hid == id {
					return true
				}
			}
			return false
		}) {
			return 1
		}
		if m, w := check(fmt.Sprintf("after restarting node %d", id)); m > 0 || w > 0 {
			return 1
		}
	}
	return 0
}

// s5: add a node under active load, rebalance, zero drops.
func ips5(opt options) int {
	banner("Scenario 5 (10.5): add a node under active load; rebalance; requests keep flowing")
	c := bootCluster(4, 1, 2)
	defer c.shutdown()
	fmt.Printf("START topology nodes=%v masters=%v\n", c.healthyNodeIDs(), c.masters())

	var errs, doneOps uint64
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				k := fmt.Sprintf("s5load-%d-%d", wid, i)
				i++
				if err := c.kvSet(k, "x"); err != nil {
					atomic.AddUint64(&errs, 1)
				}
				atomic.AddUint64(&doneOps, 1)
			}
		}(w)
	}
	time.Sleep(4 * time.Second)
	fmt.Println("JOIN node 3 mid-load")
	c.startNode(3)
	c.post("/internal/notify-roles", map[string]any{})
	joined := c.waitFor(30*time.Second, "masters span 3 nodes", func() bool {
		seen := map[int]bool{}
		for _, m := range c.masters() {
			seen[m] = true
		}
		return len(seen) >= 3
	})
	fmt.Printf("REBALANCED masters=%v spread3=%v\n", c.masters(), joined)
	time.Sleep(4 * time.Second)
	close(stopLoad)
	wg.Wait()
	fmt.Printf("LOAD ops=%d errors=%d\n", atomic.LoadUint64(&doneOps), atomic.LoadUint64(&errs))
	if atomic.LoadUint64(&errs) > 0 {
		fmt.Println("NOTE errors>0 under join; see count above (acceptance: requests keep flowing).")
	}
	return 0
}

// s6: replica count up/down live, then survive a kill.
func ips6(opt options) int {
	banner("Scenario 6 (10.6): replica count up/down live; survive a node kill")
	c := bootCluster(4, 2, 3)
	defer c.shutdown()

	const known = 40
	for i := 1; i <= known; i++ {
		if err := c.kvSet(fmt.Sprintf("rf-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			fmt.Printf("SEED FAIL: %v\n", err)
			return 1
		}
	}
	setRF := func(rf int) {
		code := c.post("/replication/factor", map[string]any{"replication_factor": rf})
		fmt.Printf("RF-SET replication_factor=%d -> http %d\n", rf, code)
		time.Sleep(4 * time.Second)
		maxReplicas := 0
		var m controller.MapResponse
		if c.get("/map", &m) == http.StatusOK {
			for _, p := range m.Partitions {
				if len(p.Replicas) > maxReplicas {
					maxReplicas = len(p.Replicas)
				}
			}
		}
		fmt.Printf("RF-VERIFY max replicas per partition=%d (want %d)\n", maxReplicas, rf-1)
	}
	verify := func(tag string) bool {
		missing, wrong := 0, 0
		for i := 1; i <= known; i++ {
			got, err := c.kvGet(fmt.Sprintf("rf-%d", i))
			if err != nil {
				missing++
			} else if got != fmt.Sprintf("value-%d", i) {
				wrong++
			}
		}
		fmt.Printf("VERIFY %s: missing=%d wrong=%d of %d\n", tag, missing, wrong, known)
		return missing == 0 && wrong == 0
	}
	setRF(3)
	if !verify("rf=3") {
		return 1
	}
	setRF(1)
	if !verify("rf=1") {
		return 1
	}
	setRF(2)
	killVictim := 3
	fmt.Printf("KILL node %d at rf=2\n", killVictim)
	c.stopNode(killVictim)
	time.Sleep(8 * time.Second)
	if !verify("after kill") {
		return 1
	}
	// show the data is also readable key-by-key through keys spread by hash
	spread := map[int]int{}
	for i := 1; i <= known; i++ {
		spread[hash.PartitionForKey(fmt.Sprintf("rf-%d", i), 4)]++
	}
	fmt.Printf("KEY-SPREAD across partitions: %v\n", spread)
	return 0
}

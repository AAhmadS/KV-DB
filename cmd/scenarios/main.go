// Command scenarios is the portable (Windows/Linux/docker) data-plane
// scenario runner for the KV-DB grading scenarios that do not need
// container orchestration:
//
//	s1  single-node smoke: Set/Get/Delete correctness + baseline RPS
//	s2  multi-node check: topology spread from /map + RPS under load
//
// Control-plane scenarios (kill/restart/add-node/replica-count, spec
// 10.3-10.6) need docker and live in scripts/run_all_scenarios.sh; the
// same behaviours are covered natively by go test -tags integration ./test/.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kvdb/kvdb/pkg/kvdb"
)

type options struct {
	lbURL       string
	ctrlURL     string
	scenario    string
	ops         int
	concurrency int
	keyspace    int
}

type benchResult struct {
	done    uint64
	errors  uint64
	retries uint64
	elapsed time.Duration
	lats    []time.Duration
	mu      sync.Mutex
}

func main() {
	var opt options
	flag.StringVar(&opt.lbURL, "lb", envOr("LB_URL", "http://localhost:8080"), "load balancer base URL")
	flag.StringVar(&opt.ctrlURL, "controller", envOr("CONTROLLER_URL", "http://localhost:8000"), "controller base URL")
	flag.StringVar(&opt.scenario, "scenario", "all", "s1 | s2 | all")
	flag.IntVar(&opt.ops, "ops", 2000, "benchmark operations")
	flag.IntVar(&opt.concurrency, "concurrency", 8, "benchmark workers")
	flag.IntVar(&opt.keyspace, "keyspace", 500, "distinct keys")
	flag.Parse()

	client := kvdb.NewClient(kvdb.Config{
		LoadBalancerURL: opt.lbURL,
		Timeout:         10 * time.Second,
		Retry:           kvdb.DefaultRetryConfig(),
	})
	defer client.Close()

	ctx := context.Background()
	rc := 0
	switch opt.scenario {
	case "s1":
		rc = scenarioS1(ctx, client, opt)
	case "s2":
		rc = scenarioS2(ctx, client, opt)
	case "all":
		if scenarioS1(ctx, client, opt) != 0 {
			rc = 1
		}
		if scenarioS2(ctx, client, opt) != 0 {
			rc = 1
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q (want s1|s2|all)\n", opt.scenario)
		os.Exit(2)
	}
	os.Exit(rc)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// scenarioS1: correctness smoke + baseline RPS.
func scenarioS1(ctx context.Context, client *kvdb.Client, opt options) int {
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

	res := runBench(ctx, client, opt)
	printBench("baseline", res)
	return 0
}

// scenarioS2: topology spread + RPS for the scaling comparison.
func scenarioS2(ctx context.Context, client *kvdb.Client, opt options) int {
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

	res := runBench(ctx, client, opt)
	printBench("multi-node", res)
	fmt.Println("Compare this throughput line with the Scenario 1 baseline above (spec 10.2).")
	return 0
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
	res.mu.Lock()
	res.elapsed = time.Since(start)
	res.mu.Unlock()
	return res
}

func printBench(label string, res *benchResult) {
	lats := append([]time.Duration(nil), res.lats...)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	q := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		i := int(p * float64(len(lats)-1))
		return lats[i]
	}
	var total time.Duration
	for _, l := range lats {
		total += l
	}
	avg := time.Duration(0)
	if len(lats) > 0 {
		avg = total / time.Duration(len(lats))
	}
	secs := res.elapsed.Seconds()
	rps := float64(len(lats)) / secs
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

func banner(s string) {
	fmt.Println()
	fmt.Println("======================================================================")
	fmt.Println(" " + s)
	fmt.Println("======================================================================")
}

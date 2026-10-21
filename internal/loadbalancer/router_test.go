// Unit tests for the load balancer router: write-to-master routing,
// read strategies, retry-with-topology-refresh, and the dual-hash
// per-key decision. All tests use stub upstreams / httptest controllers,
// so they run untagged in milliseconds.
package loadbalancer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func okResp() *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}
}

func statusResp(code int) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(""))}
}

// routerWithMap builds a Router whose topology is pinned to cm.
func routerWithMap(cm *ClusterMap, cfg RouterConfig) *Router {
	tc := NewTopologyCache(DefaultTopologyCacheConfig("http://127.0.0.1:1"))
	tc.mu.Lock()
	tc.clusterMap = cm
	tc.mu.Unlock()
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	return NewRouter(tc, cfg)
}

func stableMap() *ClusterMap {
	return &ClusterMap{
		Partitions: []PartitionInfo{
			{ID: 0, MasterAddr: "m0:1", MasterID: 1, Replicas: []string{"r0a:1", "r0b:1"}},
			{ID: 1, MasterAddr: "m1:1", MasterID: 2, Replicas: nil},
		},
		NumPartitions: 2,
		ReshardState:  "stable",
		Version:       1,
	}
}

// keyForPartition finds a key hashing to want under n partitions.
func keyForPartition(prefix string, want, n int) string {
	for i := 0; ; i++ {
		k := prefix + string(rune('a'+i%26)) + string(rune('0'+i/26%10)) + string(rune('A'+i/260%26))
		if hashToPartition(k, n) == want {
			return k
		}
	}
}

func TestRouterWriteGoesToMaster(t *testing.T) {
	r := routerWithMap(stableMap(), DefaultRouterConfig())
	ctx := context.Background()
	for _, p := range []int{0, 1} {
		key := keyForPartition("w", p, 2)
		var got string
		resp, err := r.routeWithRetry(ctx, key, true, func(addr string, _ int) (*http.Response, error) {
			got = addr
			return okResp(), nil
		})
		if err != nil {
			t.Fatalf("route write %q: %v", key, err)
		}
		resp.Body.Close()
		want := stableMap().Partitions[p].MasterAddr
		if got != want {
			t.Fatalf("write %q routed to %s, want master %s", key, got, want)
		}
	}
}

func TestRouterReadMasterOnlyDefault(t *testing.T) {
	cfg := DefaultRouterConfig()
	cfg.ReadFromReplicas = false
	r := routerWithMap(stableMap(), cfg)
	ctx := context.Background()
	key := keyForPartition("r", 0, 2)
	for i := 0; i < 10; i++ {
		var got string
		resp, err := r.routeWithRetry(ctx, key, false, func(addr string, _ int) (*http.Response, error) {
			got = addr
			return okResp(), nil
		})
		if err != nil {
			t.Fatalf("route read: %v", err)
		}
		resp.Body.Close()
		if got != "m0:1" {
			t.Fatalf("master-only read went to %s", got)
		}
	}
}

func TestRouterReadRoundRobinReplicas(t *testing.T) {
	cfg := DefaultRouterConfig()
	cfg.ReadFromReplicas = true
	r := routerWithMap(stableMap(), cfg)
	ctx := context.Background()
	key := keyForPartition("rr", 0, 2)
	hits := map[string]int{}
	for i := 0; i < 30; i++ {
		var got string
		resp, err := r.routeWithRetry(ctx, key, false, func(addr string, _ int) (*http.Response, error) {
			got = addr
			return okResp(), nil
		})
		if err != nil {
			t.Fatalf("route read: %v", err)
		}
		resp.Body.Close()
		hits[got]++
	}
	for _, want := range []string{"m0:1", "r0a:1", "r0b:1"} {
		if hits[want] == 0 {
			t.Fatalf("round-robin never hit %s: %v", want, hits)
		}
	}
}

func TestRouterRetryRefreshesTopology(t *testing.T) {
	var calls int64
	var servedV2 int64
	ctrl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&calls, 1)
		master := "dead:1"
		if n >= 2 {
			master = "alive:2" // second fetch (the retry's refresh) sees failover
			atomic.StoreInt64(&servedV2, 1)
		}
		_ = json.NewEncoder(w).Encode(ClusterMap{
			Partitions:    []PartitionInfo{{ID: 0, MasterAddr: master, MasterID: 7}},
			NumPartitions: 1,
			ReshardState:  "stable",
			Version:       uint64(n),
		})
	}))
	defer ctrl.Close()

	tc := NewTopologyCache(TopologyCacheConfig{
		ControllerURL:   ctrl.URL,
		RefreshInterval: time.Hour, // manual refresh only
		RequestTimeout:  2 * time.Second,
	})
	if err := tc.Refresh(); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	cfg := DefaultRouterConfig()
	cfg.MaxRetries = 3
	r := NewRouter(tc, cfg)

	var addrs []string
	resp, err := r.routeWithRetry(context.Background(), "k", true,
		func(addr string, _ int) (*http.Response, error) {
			addrs = append(addrs, addr)
			if addr == "dead:1" {
				return statusResp(http.StatusServiceUnavailable), nil
			}
			return okResp(), nil
		})
	if err != nil {
		t.Fatalf("route with retry: %v", err)
	}
	resp.Body.Close()
	if len(addrs) != 2 || addrs[0] != "dead:1" || addrs[1] != "alive:2" {
		t.Fatalf("want [dead:1 alive:2], got %v", addrs)
	}
	if atomic.LoadInt64(&servedV2) == 0 {
		t.Fatal("retry never refreshed the topology")
	}
}

func TestDecideRouteForKeyTable(t *testing.T) {
	mkMap := func(state RangeState) *ClusterMap {
		return &ClusterMap{
			Partitions:        []PartitionInfo{{ID: 0, MasterAddr: "old:1"}},
			NumPartitions:     4,
			ReshardState:      "migrating",
			NextPartitions:    make([]PartitionInfo, 8),
			NextNumPartitions: 8,
			Ranges:            []RangeInfo{{NewPartitionID: 0, State: string(state)}},
		}
	}
	// Force every probe key into new-partition 0.
	key := ""
	for i := 0; i < 10000; i++ {
		k := "probe-key-" + string(rune(i))
		cm := mkMap(RangePending)
		if cm.NewPartitionFor(k) == 0 {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no probe key hashes to new partition 0")
	}

	cases := []struct {
		name    string
		cm      *ClusterMap
		isWrite bool
		want    RouteDecision
	}{
		{"nil map", nil, true, RouteOld},
		{"no reshard", stableMap(), true, RouteOld},
		{"pending write", mkMap(RangePending), true, RouteOld},
		{"pending read", mkMap(RangePending), false, RouteOld},
		{"transferring write", mkMap(RangeTransferring), true, RouteOld},
		{"transferring read", mkMap(RangeTransferring), false, RouteOld},
		{"handoff write waits", mkMap(RangeHandoff), true, RouteHandoff},
		{"handoff read old copy", mkMap(RangeHandoff), false, RouteOld},
		{"done write", mkMap(RangeDone), true, RouteNew},
		{"done read", mkMap(RangeDone), false, RouteNew},
	}
	for _, tc := range cases {
		if got := DecideRouteForKey(key, tc.cm, tc.isWrite); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

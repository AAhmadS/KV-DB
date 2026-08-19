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
	nethttp "net/http"
	syncpkg "sync"
	atomicpkg "sync/atomic"
	testingpkg "testing"
	timepkg "time"

	"github.com/kvdb/kvdb/internal/loadbalancer"
	"github.com/kvdb/kvdb/pkg/kvdb"
)

// Tests for spec section 13 - changing the number of partitions on a live
// cluster.
//
// Section 13 is the part of the spec this project had never executed: before
// this file existed, no test anywhere changed the partition count, and the
// controller's PrepareReshard deadlocked on its own mutex the first time it was
// called. These tests are what makes the claim "re-sharding works" checkable.
//
// There are three of them and they are deliberately different in kind:
//
//	TestReshard_Machinery_4to8_ReadsUnderLoad
//	    Runs the whole protocol end to end under continuous read load. Passes
//	    today. Proves the controller state machine, the freeze/handoff/announce
//	    exchange, the collapse, and that no key is lost.
//
//	TestReshard_GrowsUnderWriteLoad_4to8
//	    The same, under continuous WRITE load. This one needs the per-key
//	    routing decision that is reserved for a human (DecideRouteForKey in
//	    internal/loadbalancer/reshard_route.go), so it skips with a loud message
//	    until that function is written. It has never been observed to pass.
//
//	TestReshard_DecideRouteForKey_ThreeCases
//	    A unit test of that reserved function's three cases. Also skips.

const (
	reshardKeyCount = 600
	reshardTimeout  = 90 * timepkg.Second
)

func reshardKey(i int) string   { return fmtpkg.Sprintf("rs-key-%04d", i) }
func reshardValue(i int) string { return fmtpkg.Sprintf("rs-val-%04d", i) }

// startReshard asks the controller to change the partition count.
func startReshard(t *testingpkg.T, c *cluster, newNumPartitions int) {
	t.Helper()

	body, _ := encodingjson.Marshal(map[string]any{"new_num_partitions": newNumPartitions})
	resp, err := c.adminClient.Post(c.controllerURL+"/reshard", "application/json", bytespkg.NewReader(body))
	if err != nil {
		t.Fatalf("POST /reshard: %v", err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != nethttp.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /reshard returned %d: %s", resp.StatusCode, string(b))
	}
}

type reshardStatus struct {
	ReshardState      string `json:"reshard_state"`
	Running           bool   `json:"running"`
	OldNumPartitions  int    `json:"old_num_partitions"`
	NextNumPartitions int    `json:"next_num_partitions"`
	RangesTotal       int    `json:"ranges_total"`
	RangesDone        int    `json:"ranges_done"`
	LastError         string `json:"last_error"`
	DurationMS        int64  `json:"duration_ms"`
	Ranges            []struct {
		NewPartitionID int    `json:"new_partition_id"`
		State          string `json:"state"`
		KeysMoved      int    `json:"keys_moved"`
		Error          string `json:"error"`
	} `json:"ranges"`
}

func getReshardStatus(t *testingpkg.T, c *cluster) reshardStatus {
	t.Helper()

	resp, err := c.adminClient.Get(c.controllerURL + "/reshard/status")
	if err != nil {
		t.Fatalf("GET /reshard/status: %v", err)
	}
	defer drainAndClose(resp)

	var st reshardStatus
	if err := encodingjson.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode reshard status: %v", err)
	}
	return st
}

func getClusterNumPartitions(t *testingpkg.T, c *cluster) int {
	t.Helper()

	resp, err := c.adminClient.Get(c.controllerURL + "/map")
	if err != nil {
		t.Fatalf("GET /map: %v", err)
	}
	defer drainAndClose(resp)

	var m struct {
		NumPartitions int `json:"num_partitions"`
	}
	if err := encodingjson.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode map: %v", err)
	}
	return m.NumPartitions
}

// observedStates records which range states a poller actually saw, so the test
// can assert the intermediate state of spec 13.2 was real and not skipped.
type observedStates struct {
	mu   syncpkg.Mutex
	seen map[string]int
}

func newObservedStates() *observedStates {
	return &observedStates{seen: make(map[string]int)}
}

func (o *observedStates) record(state string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen[state]++
}

func (o *observedStates) snapshot() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]int, len(o.seen))
	for k, v := range o.seen {
		out[k] = v
	}
	return out
}

// waitForReshard polls until the cluster has collapsed back to a single formula,
// recording every range state it observes on the way through.
func waitForReshard(t *testingpkg.T, c *cluster, want int, obs *observedStates) (reshardStatus, int) {
	t.Helper()

	deadline := timepkg.Now().Add(reshardTimeout)
	var last reshardStatus
	peakKeysMoved := 0

	for timepkg.Now().Before(deadline) {
		last = getReshardStatus(t, c)
		obs.record(last.ReshardState)

		moved := 0
		for _, r := range last.Ranges {
			obs.record("range:" + r.State)
			moved += r.KeysMoved
			if r.Error != "" {
				t.Errorf("range %d reported an error: %s", r.NewPartitionID, r.Error)
			}
		}
		if moved > peakKeysMoved {
			peakKeysMoved = moved
		}

		if !last.Running && last.ReshardState == "stable" {
			if getClusterNumPartitions(t, c) == want {
				return last, peakKeysMoved
			}
			if last.LastError != "" {
				t.Fatalf("reshard aborted: %s", last.LastError)
			}
		}
		timepkg.Sleep(5 * timepkg.Millisecond)
	}

	t.Fatalf("reshard did not finish within %s; last status: %+v", reshardTimeout, last)
	return last, peakKeysMoved
}

// nodeStats reads one node's internal counters, which it publishes in the
// details of /health.
func nodeStats(t *testingpkg.T, c *cluster, nodeID int) map[string]any {
	t.Helper()

	resp, err := c.adminClient.Get(fmtpkg.Sprintf("http://127.0.0.1:%d/health", c.nodePorts[nodeID]))
	if err != nil {
		t.Fatalf("GET node %d /health: %v", nodeID, err)
	}
	defer drainAndClose(resp)

	var out struct {
		Details map[string]any `json:"details"`
	}
	if err := encodingjson.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode node %d health: %v", nodeID, err)
	}
	return out.Details
}

// readLoadResult counts what a reader fleet saw. wrongValue is the number that
// matters most: a read that returned a value other than the one written is a
// stale read from the wrong partition, which is the failure re-sharding must
// not have.
type readLoadResult struct {
	reads      int64
	notFound   int64
	wrongValue int64
	errors     int64
}

// runReadLoad hammers the pre-loaded key space and verifies every value.
func runReadLoad(ctx contextpkg.Context, client *kvdb.Client, concurrency int, res *readLoadResult) *syncpkg.WaitGroup {
	var wg syncpkg.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			i := worker
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				idx := i % reshardKeyCount
				i += concurrency

				got, err := client.GetString(ctx, reshardKey(idx))
				switch {
				case ctx.Err() != nil:
					return
				case errorspkg.Is(err, kvdb.ErrKeyNotFound):
					atomicpkg.AddInt64(&res.notFound, 1)
				case err != nil:
					atomicpkg.AddInt64(&res.errors, 1)
				case got != reshardValue(idx):
					atomicpkg.AddInt64(&res.wrongValue, 1)
				default:
					atomicpkg.AddInt64(&res.reads, 1)
				}
			}
		}(w)
	}

	return &wg
}

// TestReshard_Machinery_4to8_ReadsUnderLoad grows a live cluster from 4
// partitions to 8 while it is being read continuously, and checks the property
// that matters: every key is still there afterwards, with its own value.
//
// Read load, not write load, is what this test can assert on today. The load
// balancer's per-key old-formula/new-formula decision is reserved for a human
// (see DecideRouteForKey); until it exists the router falls back to probing
// both owners, which is correct for reads and unsafe for writes. So this test
// exercises every part of the machinery except that one decision.
func TestReshard_Machinery_4to8_ReadsUnderLoad(t *testingpkg.T) {
	c := startCluster(t, 4, 1, 3)
	defer c.Stop(t)

	clientCfg := kvdb.DefaultConfig(c.lbURL)
	clientCfg.Timeout = 5 * timepkg.Second
	clientCfg.Retry.MaxRetries = 6
	clientCfg.Retry.InitialBackoff = 50 * timepkg.Millisecond
	client := kvdb.NewClient(clientCfg)
	defer client.Close()

	loadCtx, loadCancel := contextpkg.WithTimeout(contextpkg.Background(), 2*timepkg.Minute)
	defer loadCancel()

	// --- seed -------------------------------------------------------------
	for i := 0; i < reshardKeyCount; i++ {
		if err := client.SetString(loadCtx, reshardKey(i), reshardValue(i)); err != nil {
			t.Fatalf("seed key %d: %v", i, err)
		}
	}
	t.Logf("seeded %d keys across 4 partitions", reshardKeyCount)

	// --- read load --------------------------------------------------------
	var res readLoadResult
	readCtx, readCancel := contextpkg.WithCancel(loadCtx)
	wg := runReadLoad(readCtx, client, 8, &res)

	// --- reshard ----------------------------------------------------------
	obs := newObservedStates()
	before := getClusterNumPartitions(t, c)
	if before != 4 {
		t.Fatalf("expected 4 partitions before reshard, got %d", before)
	}

	start := timepkg.Now()
	startReshard(t, c, 8)
	final, peakKeysMoved := waitForReshard(t, c, 8, obs)
	elapsed := timepkg.Since(start)

	readCancel()
	wg.Wait()

	// --- what happened ----------------------------------------------------
	t.Logf("reshard 4 -> 8 finished in %s (controller reported %d ms)", elapsed, final.DurationMS)
	t.Logf("range states observed while polling: %v", obs.snapshot())
	t.Logf("read load during the window: %d ok, %d not-found, %d wrong-value, %d errors",
		atomicpkg.LoadInt64(&res.reads),
		atomicpkg.LoadInt64(&res.notFound),
		atomicpkg.LoadInt64(&res.wrongValue),
		atomicpkg.LoadInt64(&res.errors))

	// The final status has no ranges: FinalizeReshard clears the table once the
	// map has collapsed. peakKeysMoved is the highest total observed while
	// polling. It counts key TRANSFERS, not distinct keys: a key that the bulk
	// copy ships and the final drain ships again is counted twice, so this
	// exceeding the seeded count is expected, not a bug.
	t.Logf("key transfers between nodes (bulk + final drain): %d, over %d seeded keys",
		peakKeysMoved, reshardKeyCount)

	// --- assertions -------------------------------------------------------

	// 1. The cluster really is on the new formula.
	if got := getClusterNumPartitions(t, c); got != 8 {
		t.Fatalf("expected 8 partitions after reshard, got %d", got)
	}

	// 2. The intermediate state of spec 13.2 actually existed. A reshard that
	//    jumped straight from stable to stable would pass every other check
	//    here while having done nothing.
	seen := obs.snapshot()
	if seen["migrating"] == 0 && seen["preparing"] == 0 {
		t.Errorf("never observed the intermediate state; saw only %v", seen)
	}

	// 3. Data really moved. Without this a reshard that renamed the partition
	//    count and moved nothing would satisfy every other assertion here.
	if peakKeysMoved == 0 {
		t.Errorf("no keys were physically transferred during the reshard")
	}

	// 4. Every node collapsed onto the new formula, and the 8 new partitions
	//    have exactly one master each.
	totalMasters := 0
	for id := range c.nodePorts {
		st := nodeStats(t, c, id)
		if n, ok := st["num_partitions"].(float64); !ok || int(n) != 8 {
			t.Errorf("node %d still reports num_partitions=%v, want 8", id, st["num_partitions"])
		}
		if m, ok := st["partitions_master"].(float64); ok {
			totalMasters += int(m)
		}
	}
	if totalMasters != 8 {
		t.Errorf("expected 8 partition masters across the cluster, found %d", totalMasters)
	}

	// 5. No read returned a value belonging to a different key. This is the
	//    stale-read property.
	if n := atomicpkg.LoadInt64(&res.wrongValue); n != 0 {
		t.Errorf("%d reads returned the wrong value during the reshard", n)
	}

	// 6. No key was lost. Checked directly rather than inferred from the load,
	//    because the load only samples.
	missing := 0
	wrong := 0
	verifyCtx, verifyCancel := contextpkg.WithTimeout(contextpkg.Background(), 60*timepkg.Second)
	defer verifyCancel()
	for i := 0; i < reshardKeyCount; i++ {
		got, err := client.GetString(verifyCtx, reshardKey(i))
		switch {
		case errorspkg.Is(err, kvdb.ErrKeyNotFound):
			missing++
			if missing <= 5 {
				t.Errorf("key %s is gone after the reshard", reshardKey(i))
			}
		case err != nil:
			t.Errorf("re-read %s: %v", reshardKey(i), err)
		case got != reshardValue(i):
			wrong++
			if wrong <= 5 {
				t.Errorf("key %s reads %q, want %q", reshardKey(i), got, reshardValue(i))
			}
		}
	}
	if missing != 0 || wrong != 0 {
		t.Fatalf("after reshard: %d/%d keys missing, %d with the wrong value",
			missing, reshardKeyCount, wrong)
	}
	t.Logf("all %d keys survived the reshard with their own values", reshardKeyCount)

	// 7. Writes still work on the collapsed map, and land in the new partition.
	if err := client.SetString(verifyCtx, "post-reshard", "ok"); err != nil {
		t.Fatalf("write after reshard: %v", err)
	}
	if got, err := client.GetString(verifyCtx, "post-reshard"); err != nil || got != "ok" {
		t.Fatalf("read back after reshard: got %q err %v", got, err)
	}
}

// TestReshard_GrowsUnderWriteLoad_4to8 is the headline test: grow the cluster
// from 4 partitions to 8 under continuous WRITE and read load, and assert that
// no key is lost and no read returns a stale value from the wrong partition.
//
// It cannot pass until DecideRouteForKey is written, and it does not pretend
// otherwise - it skips with the reason. The problem is specific and is exactly
// what that function is for: while a range is pending or transferring, the old
// master is still authoritative, and a write routed by the new formula lands on
// a node whose copy the bulk transfer then overwrites. The write is
// acknowledged and then silently disappears. No amount of retrying at the edges
// fixes that; the router has to know which formula owns the key.
func TestReshard_GrowsUnderWriteLoad_4to8(t *testingpkg.T) {
	if loadbalancer.DecideRouteForKey("probe", nil, true) == loadbalancer.RouteUnimplemented {
		t.Skip("SKIPPED: internal/loadbalancer.DecideRouteForKey is not implemented " +
			"(reserved TODO(sam)). Without it the router falls back to probing both " +
			"owners, which loses writes made to a range that has not migrated yet. " +
			"This test has never been observed to pass. Implement DecideRouteForKey " +
			"and it will run.")
	}

	c := startCluster(t, 4, 1, 3)
	defer c.Stop(t)

	clientCfg := kvdb.DefaultConfig(c.lbURL)
	clientCfg.Timeout = 5 * timepkg.Second
	clientCfg.Retry.MaxRetries = 8
	clientCfg.Retry.InitialBackoff = 50 * timepkg.Millisecond
	client := kvdb.NewClient(clientCfg)
	defer client.Close()

	ctx, cancel := contextpkg.WithTimeout(contextpkg.Background(), 3*timepkg.Minute)
	defer cancel()

	// Every key that was acknowledged, with the value that was acknowledged.
	// A write is only recorded after Set returns nil, so anything in here is a
	// write the cluster promised to keep.
	var ackedMu syncpkg.Mutex
	acked := make(map[string]string)

	var writes, writeErrs, readWrong int64

	writeCtx, stopWriters := contextpkg.WithCancel(ctx)
	var wg syncpkg.WaitGroup

	const writers = 6
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-writeCtx.Done():
					return
				default:
				}

				key := fmtpkg.Sprintf("wl-%d-%06d", worker, i)
				val := fmtpkg.Sprintf("v-%d-%06d", worker, i)

				if err := client.SetString(writeCtx, key, val); err != nil {
					if writeCtx.Err() != nil {
						return
					}
					atomicpkg.AddInt64(&writeErrs, 1)
					continue
				}
				atomicpkg.AddInt64(&writes, 1)

				ackedMu.Lock()
				acked[key] = val
				ackedMu.Unlock()

				// Read back immediately. During a reshard this is the read most
				// likely to be served by the wrong node.
				got, err := client.GetString(writeCtx, key)
				if err == nil && got != val {
					atomicpkg.AddInt64(&readWrong, 1)
				}
			}
		}(w)
	}

	// Let the write load establish itself, then reshard underneath it.
	timepkg.Sleep(500 * timepkg.Millisecond)

	obs := newObservedStates()
	start := timepkg.Now()
	startReshard(t, c, 8)
	final, peakKeysMoved := waitForReshard(t, c, 8, obs)
	elapsed := timepkg.Since(start)
	t.Logf("key transfers between nodes (bulk + final drain): %d", peakKeysMoved)

	// Keep writing for a moment after the collapse, then stop.
	timepkg.Sleep(500 * timepkg.Millisecond)
	stopWriters()
	wg.Wait()

	t.Logf("reshard 4 -> 8 under write load finished in %s (controller reported %d ms)",
		elapsed, final.DurationMS)
	t.Logf("write load: %d acknowledged, %d errors, %d read-back mismatches",
		atomicpkg.LoadInt64(&writes), atomicpkg.LoadInt64(&writeErrs), atomicpkg.LoadInt64(&readWrong))
	t.Logf("range states observed: %v", obs.snapshot())

	if got := getClusterNumPartitions(t, c); got != 8 {
		t.Fatalf("expected 8 partitions after reshard, got %d", got)
	}

	// Every acknowledged write must still be readable, with its own value.
	verifyCtx, verifyCancel := contextpkg.WithTimeout(contextpkg.Background(), 120*timepkg.Second)
	defer verifyCancel()

	ackedMu.Lock()
	defer ackedMu.Unlock()

	lost, stale := 0, 0
	for key, want := range acked {
		got, err := client.GetString(verifyCtx, key)
		switch {
		case errorspkg.Is(err, kvdb.ErrKeyNotFound):
			lost++
			if lost <= 5 {
				t.Errorf("acknowledged write %s was lost by the reshard", key)
			}
		case err != nil:
			t.Errorf("re-read %s: %v", key, err)
		case got != want:
			stale++
			if stale <= 5 {
				t.Errorf("key %s reads %q, want %q", key, got, want)
			}
		}
	}

	if lost != 0 || stale != 0 {
		t.Fatalf("of %d acknowledged writes: %d lost, %d stale", len(acked), lost, stale)
	}
	t.Logf("all %d acknowledged writes survived the reshard", len(acked))
}

// TestReshard_DecideRouteForKey_ThreeCases pins the contract of the reserved
// function: the three cases it must distinguish, stated as assertions rather
// than as prose.
//
// It builds a cluster map by hand - no cluster needed - representing a 4 -> 8
// reshard where one range has not moved, one is mid-handoff, and one is done.
func TestReshard_DecideRouteForKey_ThreeCases(t *testingpkg.T) {
	if loadbalancer.DecideRouteForKey("probe", nil, true) == loadbalancer.RouteUnimplemented {
		t.Skip("SKIPPED: internal/loadbalancer.DecideRouteForKey is not implemented " +
			"(reserved TODO(sam)). These are the assertions it must satisfy.")
	}

	// A key per new-partition id, found by search so the test does not depend
	// on a particular hash value being memorised.
	formula := &loadbalancer.ClusterMap{NextNumPartitions: 8}
	keyForNewPartition := func(p int) string {
		for i := 0; i < 100000; i++ {
			k := fmtpkg.Sprintf("probe-%d", i)
			if formula.NewPartitionFor(k) == p {
				return k
			}
		}
		t.Fatalf("no key found hashing to new partition %d", p)
		return ""
	}

	cm := &loadbalancer.ClusterMap{
		NumPartitions:     4,
		NextNumPartitions: 8,
		ReshardState:      "migrating",
		Partitions:        make([]loadbalancer.PartitionInfo, 4),
		NextPartitions:    make([]loadbalancer.PartitionInfo, 8),
	}
	for i := range cm.Partitions {
		cm.Partitions[i] = loadbalancer.PartitionInfo{ID: i, MasterAddr: fmtpkg.Sprintf("old-%d:1", i)}
	}
	for i := range cm.NextPartitions {
		cm.NextPartitions[i] = loadbalancer.PartitionInfo{ID: i, MasterAddr: fmtpkg.Sprintf("new-%d:1", i)}
	}

	cases := []struct {
		name      string
		partition int
		state     string
		isWrite   bool
		want      loadbalancer.RouteDecision
	}{
		{"pending, read", 1, "pending", false, loadbalancer.RouteOld},
		{"pending, write", 1, "pending", true, loadbalancer.RouteOld},
		{"transferring, read", 2, "transferring", false, loadbalancer.RouteOld},
		{"transferring, write", 2, "transferring", true, loadbalancer.RouteOld},
		{"handoff, read", 3, "handoff", false, loadbalancer.RouteOld},
		{"handoff, write", 3, "handoff", true, loadbalancer.RouteHandoff},
		{"done, read", 5, "done", false, loadbalancer.RouteNew},
		{"done, write", 5, "done", true, loadbalancer.RouteNew},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testingpkg.T) {
			cm.Ranges = []loadbalancer.RangeInfo{{NewPartitionID: tc.partition, State: tc.state}}
			key := keyForNewPartition(tc.partition)

			got := loadbalancer.DecideRouteForKey(key, cm, tc.isWrite)
			if got != tc.want {
				t.Errorf("DecideRouteForKey(%q, range %d %s, isWrite=%v) = %v, want %v",
					key, tc.partition, tc.state, tc.isWrite, got, tc.want)
			}
		})
	}

	// A nil map, and a map with no reshard in flight, must not claim the new
	// formula applies.
	if got := loadbalancer.DecideRouteForKey("anything", nil, true); got == loadbalancer.RouteNew {
		t.Errorf("DecideRouteForKey with a nil map returned RouteNew")
	}
	stable := &loadbalancer.ClusterMap{NumPartitions: 4, ReshardState: "stable"}
	if got := loadbalancer.DecideRouteForKey("anything", stable, true); got == loadbalancer.RouteNew {
		t.Errorf("DecideRouteForKey with no reshard in flight returned RouteNew")
	}
}

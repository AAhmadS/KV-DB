package cluster

// End-to-end: concurrent client -> LB -> node load across a master kill.
// Two phases separated by a barrier so the test asserts honest
// eventual-consistency semantics exactly:
//   - Phase A (pre-kill): writes are acked AND replicated (verified on the
//     replica), then the master dies. All of them must survive: 100%.
//   - Phase B (through the failover window): client backoff + LB
//     refresh+retry must keep the error rate < 5%, and all but a small
//     in-flight margin of acked writes must be readable afterwards.
import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kvdb/kvdb/pkg/hash"
)

func TestLoadContinuesDuringFailover(t *testing.T) {
	c := startTestCluster(t, 4, 2, 2)
	defer c.stopAll()

	const workers = 4
	const phaseAOps = 25
	const phaseBOps = 25

	victim := c.masters()[0]
	t.Logf("partition 0 master is node %d (kill target)", victim)

	var opErrors uint64
	phaseA := make([][][2]string, workers) // [key, value] acked pre-kill
	phaseB := make([][]string, workers)    // "key=value" acked post-kill

	var phaseADone uint64
	var gateB uint64 // closed until the kill

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			for i := 0; i < phaseAOps; i++ {
				key := fmt.Sprintf("e2eA-%d-%d", wid, i)
				val := fmt.Sprintf("va%d-%d", wid, i)
				if err := c.kv.Set(ctx, key, []byte(val)); err != nil {
					atomic.AddUint64(&opErrors, 1)
					continue
				}
				phaseA[wid] = append(phaseA[wid], [2]string{key, val})
				if got, err := c.kv.Get(ctx, key); err != nil || string(got) != val {
					atomic.AddUint64(&opErrors, 1)
				}
			}
			atomic.AddUint64(&phaseADone, 1)
			for atomic.LoadUint64(&gateB) == 0 {
				time.Sleep(20 * time.Millisecond)
			}
			for i := 0; i < phaseBOps; i++ {
				key := fmt.Sprintf("e2eB-%d-%d", wid, i)
				val := fmt.Sprintf("vb%d-%d", wid, i)
				if err := c.kv.Set(ctx, key, []byte(val)); err != nil {
					atomic.AddUint64(&opErrors, 1)
					continue
				}
				phaseB[wid] = append(phaseB[wid], key+"="+val)
				if got, err := c.kv.Get(ctx, key); err != nil || string(got) != val {
					atomic.AddUint64(&opErrors, 1)
				}
			}
		}(w)
	}

	// Wait for phase A, then verify every write actually replicated before
	// the kill: durability baseline, no excuses afterwards.
	c.waitFor(60*time.Second, "phase A complete", func() bool {
		return atomic.LoadUint64(&phaseADone) == workers
	})
	c.waitFor(30*time.Second, "phase A writes replicated", func() bool {
		m := c.masters()
		for wid := range phaseA {
			for _, kv := range phaseA[wid] {
				p := hash.PartitionForKey(kv[0], 4)
				master := m[p]
				for id := range c.nodes {
					if id == master {
						continue
					}
					if _, found := c.nodeDirectGet(id, p, kv[0]); !found {
						return false
					}
				}
			}
		}
		return true
	})

	t.Logf("killing node %d between phases", victim)
	c.stopNode(victim)
	atomic.StoreUint64(&gateB, 1) // phase B hammers through the failover
	wg.Wait()

	errs := atomic.LoadUint64(&opErrors)
	total := uint64(workers * (phaseAOps + phaseBOps) * 2)
	t.Logf("load errors: %d/%d ops", errs, total)
	if float64(errs) > 0.05*float64(total) {
		t.Fatalf("error rate too high across failover: %d/%d", errs, total)
	}
	c.waitFor(25*time.Second, "partition 0 has a new master", func() bool {
		m := c.masters()
		return m[0] != victim && m[0] > 0
	})
	time.Sleep(5 * time.Second) // let async replication converge

	// Phase A: replicated before the kill -> 100% must survive.
	lostA := 0
	countA := 0
	for wid := range phaseA {
		for _, kv := range phaseA[wid] {
			countA++
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			got, err := c.kv.Get(ctx, kv[0])
			cancel()
			if err != nil || string(got) != kv[1] {
				lostA++
			}
		}
	}
	t.Logf("phase A durable: %d/%d readable", countA-lostA, countA)
	if lostA > 0 {
		t.Fatalf("lost %d/%d replicated pre-kill writes", lostA, countA)
	}

	// Phase B: acked across the failover window -> bounded in-flight margin.
	var allB []string
	for _, s := range phaseB {
		allB = append(allB, s...)
	}
	missingB := 0
	for _, kv := range allB {
		key, want := splitE2E(kv)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		got, err := c.kv.Get(ctx, key)
		cancel()
		if err != nil || string(got) != want {
			missingB++
		}
	}
	t.Logf("phase B window: %d/%d readable", len(allB)-missingB, len(allB))
	if len(allB) == 0 {
		t.Fatal("no phase B writes acked")
	}
	if float64(missingB) > 0.05*float64(len(allB)) {
		t.Fatalf("lost %d/%d failover-window writes", missingB, len(allB))
	}
}

func splitE2E(kv string) (string, string) {
	for i := 0; i < len(kv); i++ {
		if kv[i] == '=' {
			return kv[:i], kv[i+1:]
		}
	}
	return kv, ""
}

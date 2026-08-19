package main

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kvdb/kvdb/pkg/kvdb"
)

type config struct {
	loadBalancerURL string
	ops             int
	duration        time.Duration
	concurrency     int
	keyspace        int
	valueBytes      int
	readFrac        float64
	deleteFrac      float64
	opTimeout       time.Duration
	seed            int64

	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

type opType uint8

const (
	opGet opType = iota
	opSet
	opDelete
)

type counters struct {
	started uint64
	done    uint64

	getOK     uint64
	getMiss   uint64
	getErr    uint64
	setOK     uint64
	setErr    uint64
	deleteOK  uint64
	deleteErr uint64

	// aborted counts operations that were still in flight when the run's own
	// deadline fired and cancelled their context. They are not server failures,
	// and counting them as errors makes every fixed-duration run look like it
	// ends in a small outage - which matters when the same counter is used to
	// measure a real one during failover.
	aborted uint64

	latencyTotalNs uint64
	latencyMinNs   uint64
	latencyMaxNs   uint64
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	client := kvdb.NewClient(kvdb.Config{
		LoadBalancerURL: cfg.loadBalancerURL,
		Timeout:         cfg.opTimeout,
		Retry: kvdb.RetryConfig{
			MaxRetries:           cfg.maxRetries,
			InitialBackoff:       cfg.initialBackoff,
			MaxBackoff:           cfg.maxBackoff,
			BackoffMultiplier:    2.0,
			JitterFraction:       0.2,
			RetryableStatusCodes: kvdb.DefaultRetryConfig().RetryableStatusCodes,
		},
	})
	defer client.Close()

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stopAt time.Time
	if cfg.duration > 0 {
		stopAt = time.Now().Add(cfg.duration)
	}

	var stats counters
	atomic.StoreUint64(&stats.latencyMinNs, math.MaxUint64)

	start := time.Now()

	var wg sync.WaitGroup
	workers := cfg.concurrency
	if workers <= 0 {
		workers = 1
	}
	for wid := 0; wid < workers; wid++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerRun(rootCtx, workerID, cfg, client, &stats, stopAt)
		}(wid)
	}

	// Optional periodic progress
	if cfg.duration > 0 {
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-rootCtx.Done():
					return
				case <-ticker.C:
					// Lightweight progress line
					done := atomic.LoadUint64(&stats.done)
					elapsed := time.Since(start).Seconds()
					if elapsed <= 0 {
						continue
					}
					fmt.Fprintf(os.Stderr, "\rprogress: %d ops (%.0f ops/s)", done, float64(done)/elapsed)
				}
			}
		}()
	}

	if cfg.duration > 0 {
		time.Sleep(time.Until(stopAt))
	}

	// Workers stop starting operations at stopAt on their own. The root context
	// is deliberately NOT cancelled here: cancelling it would abort whatever is
	// in flight at that instant and report it as a failure, which would put a
	// small fake outage at the end of every fixed-duration run. Per-operation
	// timeouts bound the drain.
	wg.Wait()
	cancel()
	if cfg.duration > 0 {
		fmt.Fprintln(os.Stderr)
	}

	printSummary(cfg, &stats, time.Since(start))
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.loadBalancerURL, "addr", "http://localhost:8080", "load balancer base URL")
	flag.IntVar(&cfg.ops, "ops", 1000, "total operations (ignored if -duration > 0)")
	flag.DurationVar(&cfg.duration, "duration", 0, "run for this duration (overrides -ops)")
	flag.IntVar(&cfg.concurrency, "concurrency", runtime.GOMAXPROCS(0), "number of concurrent workers")
	flag.IntVar(&cfg.keyspace, "keyspace", 1000, "number of distinct keys")
	flag.IntVar(&cfg.valueBytes, "value-bytes", 128, "value size for SET operations")
	flag.Float64Var(&cfg.readFrac, "read", 0.5, "fraction of GET operations (0.0-1.0)")
	flag.Float64Var(&cfg.deleteFrac, "delete", 0.0, "fraction of DELETE operations (0.0-1.0)")
	flag.DurationVar(&cfg.opTimeout, "timeout", 5*time.Second, "per-operation timeout")
	flag.Int64Var(&cfg.seed, "seed", 0, "random seed (0 = crypto-random)")

	flag.IntVar(&cfg.maxRetries, "retries", kvdb.DefaultRetryConfig().MaxRetries, "max retries per operation")
	flag.DurationVar(&cfg.initialBackoff, "backoff", kvdb.DefaultRetryConfig().InitialBackoff, "initial retry backoff")
	flag.DurationVar(&cfg.maxBackoff, "max-backoff", kvdb.DefaultRetryConfig().MaxBackoff, "max retry backoff")

	flag.Parse()
	if cfg.seed == 0 {
		cfg.seed = cryptoSeed()
	}
	return cfg
}

func validateConfig(cfg config) error {
	if cfg.loadBalancerURL == "" {
		return fmt.Errorf("-addr is required")
	}
	if cfg.duration <= 0 && cfg.ops <= 0 {
		return fmt.Errorf("-ops must be > 0 when -duration is 0")
	}
	if cfg.concurrency <= 0 {
		return fmt.Errorf("-concurrency must be > 0")
	}
	if cfg.keyspace <= 0 {
		return fmt.Errorf("-keyspace must be > 0")
	}
	if cfg.valueBytes < 0 {
		return fmt.Errorf("-value-bytes must be >= 0")
	}
	if cfg.readFrac < 0 || cfg.readFrac > 1 {
		return fmt.Errorf("-read must be between 0 and 1")
	}
	if cfg.deleteFrac < 0 || cfg.deleteFrac > 1 {
		return fmt.Errorf("-delete must be between 0 and 1")
	}
	if cfg.readFrac+cfg.deleteFrac > 1 {
		return fmt.Errorf("-read + -delete must be <= 1")
	}
	if cfg.opTimeout <= 0 {
		return fmt.Errorf("-timeout must be > 0")
	}
	if cfg.maxRetries < 0 {
		return fmt.Errorf("-retries must be >= 0")
	}
	return nil
}

func workerRun(rootCtx context.Context, workerID int, cfg config, client *kvdb.Client, stats *counters, stopAt time.Time) {
	rng := mrand.New(mrand.NewSource(cfg.seed + int64(workerID)*1000003))
	value := make([]byte, cfg.valueBytes)
	if len(value) > 0 {
		for i := range value {
			value[i] = byte(rng.Intn(256))
		}
	}

	for {
		if rootCtx.Err() != nil {
			return
		}
		if !stopAt.IsZero() && time.Now().After(stopAt) {
			return
		}

		idx := int(atomic.AddUint64(&stats.started, 1))
		if cfg.duration <= 0 {
			// Fixed-op mode
			if idx > cfg.ops {
				return
			}
		}

		key := fmt.Sprintf("key-%d", rng.Intn(cfg.keyspace))
		op := chooseOp(rng, cfg.readFrac, cfg.deleteFrac)

		opCtx, cancel := context.WithTimeout(rootCtx, cfg.opTimeout)
		start := time.Now()
		var err error
		switch op {
		case opGet:
			_, err = client.Get(opCtx, key)
			switch {
			case err == nil:
				atomic.AddUint64(&stats.getOK, 1)
			case err == kvdb.ErrKeyNotFound:
				atomic.AddUint64(&stats.getMiss, 1)
				err = nil
			case shutdownAbort(rootCtx, err):
				atomic.AddUint64(&stats.aborted, 1)
			default:
				atomic.AddUint64(&stats.getErr, 1)
			}
		case opDelete:
			err = client.Delete(opCtx, key)
			switch {
			case err == nil:
				atomic.AddUint64(&stats.deleteOK, 1)
			case shutdownAbort(rootCtx, err):
				atomic.AddUint64(&stats.aborted, 1)
			default:
				atomic.AddUint64(&stats.deleteErr, 1)
			}
		default:
			err = client.Set(opCtx, key, value)
			switch {
			case err == nil:
				atomic.AddUint64(&stats.setOK, 1)
			case shutdownAbort(rootCtx, err):
				atomic.AddUint64(&stats.aborted, 1)
			default:
				atomic.AddUint64(&stats.setErr, 1)
			}
		}
		cancel()

		lat := uint64(time.Since(start).Nanoseconds())
		atomic.AddUint64(&stats.latencyTotalNs, lat)
		atomicMin(&stats.latencyMinNs, lat)
		atomicMax(&stats.latencyMaxNs, lat)

		_ = err // errors are reflected in counters; keep loop hot.
		atomic.AddUint64(&stats.done, 1)
	}
}

// shutdownAbort reports whether err is only there because the run ended. It
// requires both that the root context is cancelled and that the error is a
// cancellation, so a genuine failure during the final second is still counted.
func shutdownAbort(rootCtx context.Context, err error) bool {
	if rootCtx.Err() == nil {
		return false
	}
	// The SDK reports a cancelled context as its own sentinel rather than
	// passing context.Canceled through, so both have to be checked.
	return errors.Is(err, context.Canceled) || errors.Is(err, kvdb.ErrContextCanceled)
}

func chooseOp(rng *mrand.Rand, readFrac, deleteFrac float64) opType {
	x := rng.Float64()
	if x < readFrac {
		return opGet
	}
	if x < readFrac+deleteFrac {
		return opDelete
	}
	return opSet
}

func atomicMin(dst *uint64, v uint64) {
	for {
		cur := atomic.LoadUint64(dst)
		if v >= cur {
			return
		}
		if atomic.CompareAndSwapUint64(dst, cur, v) {
			return
		}
	}
}

func atomicMax(dst *uint64, v uint64) {
	for {
		cur := atomic.LoadUint64(dst)
		if v <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(dst, cur, v) {
			return
		}
	}
}

func cryptoSeed() int64 {
	var b [8]byte
	_, err := io.ReadFull(cryptorand.Reader, b[:])
	if err != nil {
		return time.Now().UnixNano()
	}
	// Use signed int64 for rand.NewSource
	return int64(uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56)
}

func printSummary(cfg config, s *counters, elapsed time.Duration) {
	done := atomic.LoadUint64(&s.done)
	if done == 0 {
		fmt.Println("no operations executed")
		return
	}

	elapsedSec := elapsed.Seconds()
	opsPerSec := float64(done) / elapsedSec

	latTotal := atomic.LoadUint64(&s.latencyTotalNs)
	avgLat := time.Duration(int64(latTotal / done))
	minLat := time.Duration(int64(atomic.LoadUint64(&s.latencyMinNs)))
	maxLat := time.Duration(int64(atomic.LoadUint64(&s.latencyMaxNs)))

	getOK := atomic.LoadUint64(&s.getOK)
	getMiss := atomic.LoadUint64(&s.getMiss)
	getErr := atomic.LoadUint64(&s.getErr)
	setOK := atomic.LoadUint64(&s.setOK)
	setErr := atomic.LoadUint64(&s.setErr)
	delOK := atomic.LoadUint64(&s.deleteOK)
	delErr := atomic.LoadUint64(&s.deleteErr)
	aborted := atomic.LoadUint64(&s.aborted)

	fmt.Printf("addr: %s\n", cfg.loadBalancerURL)
	if cfg.duration > 0 {
		fmt.Printf("mode: duration=%v concurrency=%d\n", cfg.duration, cfg.concurrency)
	} else {
		fmt.Printf("mode: ops=%d concurrency=%d\n", cfg.ops, cfg.concurrency)
	}
	fmt.Printf("mix: read=%.2f delete=%.2f set=%.2f\n", cfg.readFrac, cfg.deleteFrac, 1.0-cfg.readFrac-cfg.deleteFrac)
	fmt.Printf("keys: %d value_bytes: %d\n", cfg.keyspace, cfg.valueBytes)
	fmt.Printf("elapsed: %v\n", elapsed)
	fmt.Printf("throughput: %.1f ops/s (total=%d)\n", opsPerSec, done)
	fmt.Printf("latency: avg=%v min=%v max=%v\n", avgLat, minLat, maxLat)
	fmt.Printf("get: ok=%d miss=%d err=%d\n", getOK, getMiss, getErr)
	fmt.Printf("set: ok=%d err=%d\n", setOK, setErr)
	fmt.Printf("delete: ok=%d err=%d\n", delOK, delErr)
	fmt.Printf("errors: %d  aborted_at_shutdown: %d\n", getErr+setErr+delErr, aborted)
}

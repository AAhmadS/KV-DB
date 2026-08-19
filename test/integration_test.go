//go:build integration_external
// +build integration_external

package test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kvdb/kvdb/pkg/kvdb"
)

const (
	defaultLoadBalancerURL = "http://localhost:8080"
	defaultControllerURL   = "http://localhost:8000"
	defaultNode1URL        = "http://localhost:8001"
	defaultNode2URL        = "http://localhost:8002"
	defaultNode3URL        = "http://localhost:8003"
)

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func TestIntegration_WriteAndRead(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Cluster is healthy, starting write test...")

	const numKeys = 1000
	keys := make([]string, numKeys)
	values := make([]string, numKeys)

	// Write 1000 keys
	t.Log("Writing 1000 keys...")
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%05d", i)
		values[i] = fmt.Sprintf("value-%05d", i)

		if err := client.SetString(ctx, keys[i], values[i]); err != nil {
			t.Fatalf("failed to set key %s: %v", keys[i], err)
		}

		if (i+1)%100 == 0 {
			t.Logf("Written %d keys", i+1)
		}
	}

	t.Log("All keys written, verifying reads...")

	// Read all keys and verify
	var readErrors int
	for i := 0; i < numKeys; i++ {
		val, err := client.GetString(ctx, keys[i])
		if err != nil {
			t.Errorf("failed to get key %s: %v", keys[i], err)
			readErrors++
			continue
		}

		if val != values[i] {
			t.Errorf("value mismatch for key %s: expected %q, got %q", keys[i], values[i], val)
			readErrors++
		}
	}

	if readErrors > 0 {
		t.Fatalf("had %d read errors", readErrors)
	}

	t.Log("All keys verified successfully!")
}

func TestIntegration_Failover(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	node1URL := getEnvOrDefault("NODE1_URL", defaultNode1URL)

	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Cluster is healthy, starting failover test...")

	const numKeys = 1000
	keys := make([]string, numKeys)
	values := make([]string, numKeys)

	// Write 1000 keys
	t.Log("Writing 1000 keys...")
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("failover-key-%05d", i)
		values[i] = fmt.Sprintf("failover-value-%05d", i)

		if err := client.SetString(ctx, keys[i], values[i]); err != nil {
			t.Fatalf("failed to set key %s: %v", keys[i], err)
		}

		if (i+1)%100 == 0 {
			t.Logf("Written %d keys", i+1)
		}
	}

	t.Log("All keys written, now killing node 1...")

	// Kill node 1 using the /die endpoint
	if err := killNode(node1URL); err != nil {
		t.Logf("Warning: failed to kill node (may already be dead): %v", err)
	}

	// Wait for controller to detect failure and failover
	t.Log("Waiting for failover to complete...")
	time.Sleep(15 * time.Second)

	t.Log("Verifying keys are still readable after failover...")

	// Try to read all keys - some may fail initially but should recover
	var successCount, failCount int
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Use multiple goroutines to read in parallel
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Retry a few times to allow for failover
			var lastErr error
			for attempt := 0; attempt < 5; attempt++ {
				val, err := client.GetString(ctx, keys[idx])
				if err == nil {
					if val == values[idx] {
						mu.Lock()
						successCount++
						mu.Unlock()
						return
					}
					lastErr = fmt.Errorf("value mismatch: expected %q, got %q", values[idx], val)
				} else {
					lastErr = err
				}
				time.Sleep(500 * time.Millisecond)
			}

			mu.Lock()
			failCount++
			mu.Unlock()
			t.Logf("Failed to read key %s after retries: %v", keys[idx], lastErr)
		}(i)
	}

	wg.Wait()

	t.Logf("Read results: %d success, %d failed", successCount, failCount)

	// We expect most keys to be readable due to replication
	// With replication factor of 2, data should survive 1 node failure
	successRate := float64(successCount) / float64(numKeys) * 100
	t.Logf("Success rate: %.2f%%", successRate)

	if successRate < 50 {
		t.Fatalf("Too many keys lost after failover: only %.2f%% readable", successRate)
	}

	t.Log("Failover test passed!")
}

func TestIntegration_ConcurrentWrites(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Cluster is healthy, starting concurrent write test...")

	const (
		numGoroutines    = 10
		keysPerGoroutine = 100
	)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*keysPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("concurrent-%d-%05d", goroutineID, i)
				value := fmt.Sprintf("value-%d-%05d", goroutineID, i)

				if err := client.SetString(ctx, key, value); err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to set key %s: %w", goroutineID, key, err)
					continue
				}

				// Immediately read back
				val, err := client.GetString(ctx, key)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to get key %s: %w", goroutineID, key, err)
					continue
				}

				if val != value {
					errors <- fmt.Errorf("goroutine %d: value mismatch for %s: expected %q, got %q", goroutineID, key, value, val)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	var errCount int
	for err := range errors {
		t.Error(err)
		errCount++
	}

	if errCount > 0 {
		t.Fatalf("had %d errors during concurrent writes", errCount)
	}

	t.Logf("Successfully wrote and verified %d keys concurrently!", numGoroutines*keysPerGoroutine)
}

func TestIntegration_DeleteKeys(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Testing delete operations...")

	const numKeys = 100

	// Write keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("delete-test-%05d", i)
		value := fmt.Sprintf("delete-value-%05d", i)

		if err := client.SetString(ctx, key, value); err != nil {
			t.Fatalf("failed to set key %s: %v", key, err)
		}
	}

	t.Log("Keys written, now deleting...")

	// Delete keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("delete-test-%05d", i)

		if err := client.Delete(ctx, key); err != nil {
			t.Errorf("failed to delete key %s: %v", key, err)
		}
	}

	t.Log("Keys deleted, verifying they're gone...")

	// Verify keys are deleted
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("delete-test-%05d", i)

		exists, err := client.Exists(ctx, key)
		if err != nil {
			t.Errorf("failed to check existence of key %s: %v", key, err)
			continue
		}

		if exists {
			t.Errorf("key %s should not exist after deletion", key)
		}
	}

	t.Log("Delete test passed!")
}

func waitForClusterHealth(ctx context.Context, lbURL string) error {
	healthURL := lbURL + "/health"

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func killNode(nodeURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeURL+"/die", nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Node may have died before responding
		return nil
	}
	defer resp.Body.Close()

	return nil
}

// Helper function to check node health directly
func checkNodeHealth(nodeURL string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL+"/health", nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// Benchmark for write performance
func BenchmarkWrite(b *testing.B) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-key-" + strconv.Itoa(i)
		value := "bench-value-" + strconv.Itoa(i)
		if err := client.SetString(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark for read performance
func BenchmarkRead(b *testing.B) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx := context.Background()

	// Setup - write a key to read
	key := "bench-read-key"
	if err := client.SetString(ctx, key, "bench-read-value"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.GetString(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

// TestIntegration_RetryDuringFailover verifies that client retries succeed
// during node failover, achieving >= 95% success rate.
func TestIntegration_RetryDuringFailover(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	node1URL := getEnvOrDefault("NODE1_URL", defaultNode1URL)

	// Configure client with aggressive retry settings for failover
	cfg := kvdb.Config{
		LoadBalancerURL: lbURL,
		Timeout:         10 * time.Second,
		Retry: kvdb.RetryConfig{
			MaxRetries:           5,
			InitialBackoff:       200 * time.Millisecond,
			MaxBackoff:           3 * time.Second,
			BackoffMultiplier:    2.0,
			JitterFraction:       0.2,
			RetryableStatusCodes: []int{502, 503, 504, 409, 421},
		},
	}
	client := kvdb.NewClient(cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Cluster is healthy, writing keys before failover...")

	const numKeys = 500
	keys := make([]string, numKeys)
	values := make([]string, numKeys)

	// Write keys before failover
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("retry-test-key-%05d", i)
		values[i] = fmt.Sprintf("retry-test-value-%05d", i)

		if err := client.SetString(ctx, keys[i], values[i]); err != nil {
			t.Fatalf("failed to set key %s: %v", keys[i], err)
		}
	}

	t.Log("Keys written, starting concurrent reads while killing node...")

	// Start concurrent reads
	var wg sync.WaitGroup
	results := make(chan bool, numKeys)
	retryStats := make(chan kvdb.RetryStats, numKeys)

	// Start reads in background
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Add some jitter to spread out requests
			time.Sleep(time.Duration(idx%50) * 10 * time.Millisecond)

			val, stats, err := client.GetWithStats(ctx, keys[idx])
			retryStats <- stats

			if err != nil {
				t.Logf("Key %s failed after %d attempts: %v", keys[idx], stats.Attempts, err)
				results <- false
				return
			}

			if string(val) != values[idx] {
				t.Logf("Key %s value mismatch", keys[idx])
				results <- false
				return
			}

			results <- true
		}(i)
	}

	// Kill node 1 after a short delay to let some reads start
	go func() {
		time.Sleep(500 * time.Millisecond)
		t.Log("Killing node 1...")
		if err := killNode(node1URL); err != nil {
			t.Logf("Warning: failed to kill node: %v", err)
		}
	}()

	wg.Wait()
	close(results)
	close(retryStats)

	// Count results
	var successCount, failCount int
	for success := range results {
		if success {
			successCount++
		} else {
			failCount++
		}
	}

	// Analyze retry stats
	var totalAttempts int
	var maxAttempts int
	var retriedCount int
	for stats := range retryStats {
		totalAttempts += stats.Attempts
		if stats.Attempts > maxAttempts {
			maxAttempts = stats.Attempts
		}
		if stats.Attempts > 1 {
			retriedCount++
		}
	}

	successRate := float64(successCount) / float64(numKeys) * 100
	t.Logf("Results: %d success, %d failed (%.2f%% success rate)", successCount, failCount, successRate)
	t.Logf("Retry stats: %d operations required retries, max attempts: %d, avg attempts: %.2f",
		retriedCount, maxAttempts, float64(totalAttempts)/float64(numKeys))

	// KPI: >= 95% success rate during failover with retry
	if successRate < 95 {
		t.Fatalf("Success rate %.2f%% is below 95%% threshold", successRate)
	}

	t.Log("Retry during failover test passed!")
}

// TestIntegration_ContextCancellation verifies retries stop on context cancel.
func TestIntegration_ContextCancellation(t *testing.T) {
	// Use a non-existent URL to force retries
	cfg := kvdb.Config{
		LoadBalancerURL: "http://localhost:59999", // Non-existent
		Timeout:         1 * time.Second,
		Retry: kvdb.RetryConfig{
			MaxRetries:           10, // Many retries
			InitialBackoff:       500 * time.Millisecond,
			MaxBackoff:           5 * time.Second,
			BackoffMultiplier:    2.0,
			JitterFraction:       0.1,
			RetryableStatusCodes: []int{502, 503, 504},
		},
	}
	client := kvdb.NewClient(cfg)
	defer client.Close()

	// Create a context that will be canceled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := client.Get(ctx, "test-key")
	elapsed := time.Since(start)

	// Verify we got a context error
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should have stopped due to context, not exhausted all retries
	// With 10 retries and 500ms+ backoff, exhausting retries would take >> 2s
	if elapsed > 5*time.Second {
		t.Fatalf("operation took too long (%v), retries may not have stopped on context cancel", elapsed)
	}

	t.Logf("Operation correctly canceled after %v", elapsed)
	t.Log("Context cancellation test passed!")
}

// TestIntegration_RetryWithStats verifies retry statistics are accurate.
func TestIntegration_RetryWithStats(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	client := kvdb.NewClient(kvdb.DefaultConfig(lbURL))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	// Test successful operation (should be 1 attempt)
	key := "stats-test-key"
	value := []byte("stats-test-value")

	stats, err := client.SetWithStats(ctx, key, value)
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	if !stats.Successful {
		t.Error("expected Successful=true")
	}

	if stats.Attempts != 1 {
		t.Errorf("expected 1 attempt for successful op, got %d", stats.Attempts)
	}

	t.Logf("Set completed: attempts=%d, time=%v", stats.Attempts, stats.TotalTime)

	// Test get with stats
	val, stats, err := client.GetWithStats(ctx, key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if string(val) != string(value) {
		t.Errorf("value mismatch: expected %q, got %q", value, val)
	}

	if !stats.Successful {
		t.Error("expected Successful=true for get")
	}

	t.Logf("Get completed: attempts=%d, time=%v", stats.Attempts, stats.TotalTime)
	t.Log("Retry stats test passed!")
}

// TestIntegration_WritesDuringNodeRestart verifies writes succeed during
// transient node unavailability via client retries.
func TestIntegration_WritesDuringNodeRestart(t *testing.T) {
	lbURL := getEnvOrDefault("LB_URL", defaultLoadBalancerURL)
	node2URL := getEnvOrDefault("NODE2_URL", defaultNode2URL)

	// Configure client with retry for transient failures
	cfg := kvdb.Config{
		LoadBalancerURL: lbURL,
		Timeout:         10 * time.Second,
		Retry: kvdb.RetryConfig{
			MaxRetries:           5,
			InitialBackoff:       300 * time.Millisecond,
			MaxBackoff:           5 * time.Second,
			BackoffMultiplier:    2.0,
			JitterFraction:       0.2,
			RetryableStatusCodes: []int{502, 503, 504, 409, 421},
		},
	}
	client := kvdb.NewClient(cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Wait for cluster to be healthy
	if err := waitForClusterHealth(ctx, lbURL); err != nil {
		t.Fatalf("cluster not healthy: %v", err)
	}

	t.Log("Starting concurrent writes while killing node 2...")

	const numWrites = 200
	var wg sync.WaitGroup
	results := make(chan error, numWrites)

	// Start writes in background
	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Spread out writes
			time.Sleep(time.Duration(idx*50) * time.Millisecond)

			key := fmt.Sprintf("restart-write-%05d", idx)
			value := fmt.Sprintf("restart-value-%05d", idx)

			err := client.SetString(ctx, key, value)
			results <- err
		}(i)
	}

	// Kill node 2 after some writes have started
	go func() {
		time.Sleep(2 * time.Second)
		t.Log("Killing node 2...")
		killNode(node2URL)
	}()

	wg.Wait()
	close(results)

	// Count successes and failures
	var successCount, failCount int
	for err := range results {
		if err == nil {
			successCount++
		} else {
			failCount++
			t.Logf("Write failed: %v", err)
		}
	}

	successRate := float64(successCount) / float64(numWrites) * 100
	t.Logf("Write results: %d success, %d failed (%.2f%% success rate)", successCount, failCount, successRate)

	// With retries and replication, we should achieve high success rate
	if successRate < 90 {
		t.Fatalf("Success rate %.2f%% is below 90%% threshold", successRate)
	}

	t.Log("Writes during node restart test passed!")
}

// TestIntegration_NoRetryConfig verifies operations fail immediately without retry.
func TestIntegration_NoRetryConfig(t *testing.T) {
	// Use no-retry config with non-existent URL
	cfg := kvdb.Config{
		LoadBalancerURL: "http://localhost:59999", // Non-existent
		Timeout:         1 * time.Second,
		Retry:           kvdb.NoRetryConfig(),
	}
	client := kvdb.NewClient(cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := client.Get(ctx, "test-key")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Without retries, should fail quickly (within timeout + small buffer)
	if elapsed > 3*time.Second {
		t.Fatalf("operation took too long without retries: %v", elapsed)
	}

	t.Logf("No-retry operation failed as expected after %v: %v", elapsed, err)
	t.Log("No-retry config test passed!")
}

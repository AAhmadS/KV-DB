package loadbalancer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"
)

type TopologyCache struct {
	mu              sync.RWMutex
	controllerURL   string
	clusterMap      *ClusterMap
	version         uint64
	refreshInterval time.Duration
	client          *http.Client
	stopCh          chan struct{}
	wg              sync.WaitGroup
	lastRefresh     time.Time

	// refreshMu serializes fetches. It is separate from mu so a slow controller
	// cannot block readers of the cached map.
	refreshMu sync.Mutex
}

type TopologyCacheConfig struct {
	ControllerURL   string
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
}

func DefaultTopologyCacheConfig(controllerURL string) TopologyCacheConfig {
	return TopologyCacheConfig{
		ControllerURL:   controllerURL,
		RefreshInterval: 5 * time.Second,
		RequestTimeout:  3 * time.Second,
	}
}

func NewTopologyCache(cfg TopologyCacheConfig) *TopologyCache {
	return &TopologyCache{
		controllerURL:   cfg.ControllerURL,
		refreshInterval: cfg.RefreshInterval,
		stopCh:          make(chan struct{}),
		client: &http.Client{
			Timeout: cfg.RequestTimeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          64,
				MaxIdleConnsPerHost:   16,
				MaxConnsPerHost:       4,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
	}
}

func (tc *TopologyCache) Start() error {
	if err := tc.Refresh(); err != nil {
		log.Printf("[topology] initial refresh failed: %v", err)
	}

	tc.wg.Add(1)
	go tc.refreshLoop()

	return nil
}

func (tc *TopologyCache) Stop() {
	close(tc.stopCh)
	tc.wg.Wait()
}

func (tc *TopologyCache) refreshLoop() {
	defer tc.wg.Done()

	ticker := time.NewTicker(tc.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tc.stopCh:
			return
		case <-ticker.C:
			if err := tc.Refresh(); err != nil {
				log.Printf("[topology] refresh failed: %v", err)
			}
		}
	}
}

func (tc *TopologyCache) Refresh() error {
	// Callers that arrive while a fetch is in flight WAIT for it. Returning
	// early instead - which is what a "already refreshing" flag does - is what
	// makes a retry useless: the router calls this precisely because the entry
	// it just used was wrong, and if it comes back without a newer map it reads
	// the same wrong entry again and burns the attempt. Under load every
	// in-flight request retries at once, so that is the common case, not the
	// rare one.
	arrived := time.Now()

	tc.refreshMu.Lock()
	defer tc.refreshMu.Unlock()

	// Whoever we queued behind has already fetched something newer than our own
	// arrival, so one fetch serves the whole burst.
	tc.mu.RLock()
	alreadyFresh := tc.lastRefresh.After(arrived)
	tc.mu.RUnlock()
	if alreadyFresh {
		return nil
	}

	url := fmt.Sprintf("%s/map", tc.controllerURL)

	ctx, cancel := context.WithTimeout(context.Background(), tc.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := tc.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch map: %w", err)
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("controller returned status %d", resp.StatusCode)
	}

	var clusterMap ClusterMap
	if err := json.NewDecoder(resp.Body).Decode(&clusterMap); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	tc.mu.Lock()
	if clusterMap.Version > tc.version {
		tc.clusterMap = &clusterMap
		tc.version = clusterMap.Version
		log.Printf("[topology] updated to version %d (%d partitions, reshard=%s)",
			clusterMap.Version, clusterMap.NumPartitions, clusterMap.ReshardState)
	}
	tc.lastRefresh = time.Now()
	tc.mu.Unlock()

	return nil
}

func (tc *TopologyCache) ForceRefresh() error {
	return tc.Refresh()
}

func (tc *TopologyCache) GetClusterMap() *ClusterMap {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return nil
	}

	copy := *tc.clusterMap
	return &copy
}

func (tc *TopologyCache) GetPartition(partitionID int) (*PartitionInfo, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return nil, false
	}

	if partitionID < 0 || partitionID >= len(tc.clusterMap.Partitions) {
		return nil, false
	}

	p := tc.clusterMap.Partitions[partitionID]
	return &p, true
}

func (tc *TopologyCache) NumPartitions() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return 0
	}

	return tc.clusterMap.NumPartitions
}

func (tc *TopologyCache) Version() uint64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.version
}

func (tc *TopologyCache) ReshardState() string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return "unknown"
	}

	return tc.clusterMap.ReshardState
}

// IsResharding reports whether a re-shard is in flight at all, which is wider
// than IsMigrating: the intermediate state opens at "preparing", before the
// first key moves, and does not close until the map has collapsed.
func (tc *TopologyCache) IsResharding() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return false
	}
	switch tc.clusterMap.ReshardState {
	case "preparing", "migrating", "completing":
		return tc.clusterMap.NextNumPartitions > 0
	default:
		return false
	}
}

func (tc *TopologyCache) IsMigrating() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil {
		return false
	}

	return tc.clusterMap.ReshardState == "migrating"
}

func (tc *TopologyCache) GetNextPartition(partitionID int) (*PartitionInfo, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if tc.clusterMap == nil || tc.clusterMap.NextPartitions == nil {
		return nil, false
	}

	if partitionID < 0 || partitionID >= len(tc.clusterMap.NextPartitions) {
		return nil, false
	}

	p := tc.clusterMap.NextPartitions[partitionID]
	return &p, true
}

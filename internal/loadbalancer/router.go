package loadbalancer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"
)

type upstreamErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// upstreamCall issues one request to one node. partitionID is the partition the
// router decided this key belongs to; it travels as ?partition= so the node
// uses the router's answer instead of recomputing it with a partition count
// that may be a step behind during a re-shard. A negative id omits the
// parameter and lets the node use its own formula.
type upstreamCall func(addr string, partitionID int) (*http.Response, error)

type Router struct {
	topology   *TopologyCache
	client     *http.Client
	maxRetries int

	// readFromReplicas spreads Get across the master and its replicas instead
	// of always using the master. Spec 2.3 allows either. It raises read
	// throughput and gives up read-your-writes, because replication is
	// asynchronous: a Get that lands on a replica can miss a Set that was
	// acknowledged microseconds earlier.
	readFromReplicas bool

	rrMu       sync.Mutex
	rrCounters map[int]*uint64

	deciderMu sync.RWMutex
	decider   ReshardRouteDecider
}

type RouterConfig struct {
	RequestTimeout   time.Duration
	MaxRetries       int
	ReadFromReplicas bool
}

func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		RequestTimeout: 10 * time.Second,
		MaxRetries:     2,
	}
}

func NewRouter(topology *TopologyCache, cfg RouterConfig) *Router {
	return &Router{
		topology:         topology,
		maxRetries:       cfg.MaxRetries,
		readFromReplicas: cfg.ReadFromReplicas,
		rrCounters:       make(map[int]*uint64),
		decider:          DecideRouteForKey,
		client: &http.Client{
			Timeout: cfg.RequestTimeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          512,
				MaxIdleConnsPerHost:   128,
				MaxConnsPerHost:       16,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
	}
}

// SetReshardDecider replaces the per-key old-formula/new-formula decision. The
// default is DecideRouteForKey; a test substitutes a known policy here to
// exercise the routing machinery independently of that function.
func (r *Router) SetReshardDecider(d ReshardRouteDecider) {
	r.deciderMu.Lock()
	defer r.deciderMu.Unlock()
	if d == nil {
		d = DecideRouteForKey
	}
	r.decider = d
}

func (r *Router) decide(key string, cm *ClusterMap, isWrite bool) RouteDecision {
	r.deciderMu.RLock()
	d := r.decider
	r.deciderMu.RUnlock()
	if d == nil {
		return RouteUnimplemented
	}
	return d(key, cm, isWrite)
}

func (r *Router) hashKey(key string) int {
	return hashToPartition(key, r.topology.NumPartitions())
}

func (r *Router) hashKeyWithPartitions(key string, numPartitions int) int {
	return hashToPartition(key, numPartitions)
}

func (r *Router) getNodeForWrite(key string) (string, int, error) {
	partitionID := r.hashKey(key)

	partition, ok := r.topology.GetPartition(partitionID)
	if !ok {
		return "", partitionID, ErrPartitionNotFound
	}

	if partition.MasterAddr == "" {
		return "", partitionID, ErrNoHealthyNode
	}

	return partition.MasterAddr, partitionID, nil
}

func (r *Router) getNodeForRead(key string) (string, int, error) {
	partitionID := r.hashKey(key)

	partition, ok := r.topology.GetPartition(partitionID)
	if !ok {
		return "", partitionID, ErrPartitionNotFound
	}

	// By default reads go to the master, which preserves read-your-writes:
	// replication is asynchronous, so a replica may not have the write yet.
	// With ReadFromReplicas the master and its replicas share the read load
	// round-robin instead.
	if r.readFromReplicas && len(partition.Replicas) > 0 {
		candidates := make([]string, 0, len(partition.Replicas)+1)
		if partition.MasterAddr != "" {
			candidates = append(candidates, partition.MasterAddr)
		}
		candidates = append(candidates, partition.Replicas...)

		counter := r.getCounter(partitionID)
		idx := int(atomic.AddUint64(counter, 1)) % len(candidates)
		return candidates[idx], partitionID, nil
	}

	if partition.MasterAddr != "" {
		return partition.MasterAddr, partitionID, nil
	}
	if len(partition.Replicas) == 0 {
		return "", partitionID, ErrNoHealthyNode
	}

	counter := r.getCounter(partitionID)
	idx := int(atomic.AddUint64(counter, 1)) % len(partition.Replicas)
	return partition.Replicas[idx], partitionID, nil
}

// pickFromTable resolves one partition id in one partition table to an address.
func (r *Router) pickFromTable(table []PartitionInfo, partitionID int, isWrite bool) string {
	if partitionID < 0 || partitionID >= len(table) {
		return ""
	}
	partition := table[partitionID]

	if !isWrite && r.readFromReplicas && len(partition.Replicas) > 0 {
		candidates := make([]string, 0, len(partition.Replicas)+1)
		if partition.MasterAddr != "" {
			candidates = append(candidates, partition.MasterAddr)
		}
		candidates = append(candidates, partition.Replicas...)

		counter := r.getCounter(partitionID)
		return candidates[int(atomic.AddUint64(counter, 1))%len(candidates)]
	}

	if partition.MasterAddr != "" {
		return partition.MasterAddr
	}
	if isWrite || len(partition.Replicas) == 0 {
		return ""
	}

	counter := r.getCounter(partitionID)
	idx := int(atomic.AddUint64(counter, 1)) % len(partition.Replicas)
	return partition.Replicas[idx]
}

func (r *Router) getNodesForDualHash(key string, isWrite bool) (newOwner string, newPartition int, oldOwner string, oldPartition int) {
	newPartition, oldPartition = -1, -1

	clusterMap := r.topology.GetClusterMap()
	if clusterMap == nil {
		return "", newPartition, "", oldPartition
	}

	oldPartition = r.hashKeyWithPartitions(key, clusterMap.NumPartitions)
	oldOwner = r.pickFromTable(clusterMap.Partitions, oldPartition, isWrite)

	if len(clusterMap.NextPartitions) == 0 {
		return "", -1, oldOwner, oldPartition
	}

	newPartition = r.hashKeyWithPartitions(key, len(clusterMap.NextPartitions))
	newOwner = r.pickFromTable(clusterMap.NextPartitions, newPartition, isWrite)

	return newOwner, newPartition, oldOwner, oldPartition
}

func (r *Router) getCounter(partitionID int) *uint64 {
	r.rrMu.Lock()
	defer r.rrMu.Unlock()

	if counter, ok := r.rrCounters[partitionID]; ok {
		return counter
	}

	counter := new(uint64)
	r.rrCounters[partitionID] = counter
	return counter
}

// withPartition appends the router's partition decision to an upstream URL.
func withPartition(rawURL string, partitionID int) string {
	if partitionID < 0 {
		return rawURL
	}
	return fmt.Sprintf("%s&partition=%d", rawURL, partitionID)
}

func (r *Router) RouteSet(ctx context.Context, key string, value []byte) (*http.Response, error) {
	return r.routeWithRetry(ctx, key, true, func(addr string, partitionID int) (*http.Response, error) {
		u := withPartition(fmt.Sprintf("http://%s/set?key=%s", addr, url.QueryEscape(key)), partitionID)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		return r.client.Do(req)
	})
}

func (r *Router) RouteGet(ctx context.Context, key string) (*http.Response, error) {
	return r.routeWithRetry(ctx, key, false, func(addr string, partitionID int) (*http.Response, error) {
		u := withPartition(fmt.Sprintf("http://%s/get?key=%s", addr, url.QueryEscape(key)), partitionID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		return r.client.Do(req)
	})
}

func (r *Router) RouteDelete(ctx context.Context, key string) (*http.Response, error) {
	return r.routeWithRetry(ctx, key, true, func(addr string, partitionID int) (*http.Response, error) {
		u := withPartition(fmt.Sprintf("http://%s/delete?key=%s", addr, url.QueryEscape(key)), partitionID)
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
		if err != nil {
			return nil, err
		}
		return r.client.Do(req)
	})
}

func (r *Router) routeWithRetry(ctx context.Context, key string, isWrite bool, doRequest upstreamCall) (*http.Response, error) {
	if r.topology.IsResharding() {
		return r.routeDuringReshard(ctx, key, isWrite, doRequest)
	}

	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[router] retrying request for key=%s (attempt %d)", key, attempt+1)
			if err := r.topology.ForceRefresh(); err != nil {
				log.Printf("[router] topology refresh failed: %v", err)
			}
		}

		var addr string
		var partitionID int
		var err error

		if isWrite {
			addr, partitionID, err = r.getNodeForWrite(key)
		} else {
			addr, partitionID, err = r.getNodeForRead(key)
		}

		if err != nil {
			lastErr = err
			continue
		}

		resp, err := doRequest(addr, partitionID)
		if err != nil {
			log.Printf("[router] request to %s failed: %v", addr, err)
			lastErr = err
			continue
		}

		retry, reason := r.shouldRetryResponse(resp)
		if retry {
			netutil.DrainAndClose(resp)
			lastErr = fmt.Errorf("node %s returned %d: %w", addr, resp.StatusCode, reason)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

// routeDuringReshard is the request path while both formulas are live (spec
// 13.2). The per-key choice between them is DecideRouteForKey; everything here
// is the machinery that acts on its answer.
func (r *Router) routeDuringReshard(ctx context.Context, key string, isWrite bool, doRequest upstreamCall) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			if err := r.topology.ForceRefresh(); err != nil {
				log.Printf("[router] topology refresh failed: %v", err)
			}
		}

		clusterMap := r.topology.GetClusterMap()
		if clusterMap == nil {
			return nil, ErrClusterMapEmpty
		}

		switch decision := r.decide(key, clusterMap, isWrite); decision {
		case RouteUnimplemented:
			// DecideRouteForKey has not been written yet. Fall back to probing
			// both owners, which is correct for reads and NOT safe for writes -
			// see the TODO in reshard_route.go.
			return r.routeWithDualHash(ctx, key, isWrite, doRequest)

		case RouteHandoff:
			// Nobody may accept this request: the old master has frozen the
			// range and the new one has not announced readiness. The window is
			// milliseconds, so refresh and retry; if it outlives our retries the
			// client's own backoff covers the rest.
			lastErr = ErrRangeInHandoff
			continue

		case RouteOld:
			partitionID := clusterMap.OldPartitionFor(key)
			addr := r.pickFromTable(clusterMap.Partitions, partitionID, isWrite)
			if addr == "" {
				lastErr = ErrNoHealthyNode
				continue
			}
			resp, err := r.attempt(doRequest, addr, partitionID)
			if err != nil {
				lastErr = err
				continue
			}
			return resp, nil

		case RouteNew:
			partitionID := clusterMap.NewPartitionFor(key)
			addr := r.pickFromTable(clusterMap.NextPartitions, partitionID, isWrite)
			if addr == "" {
				lastErr = ErrNoHealthyNode
				continue
			}
			resp, err := r.attempt(doRequest, addr, partitionID)
			if err != nil {
				lastErr = err
				continue
			}
			return resp, nil

		default:
			return nil, fmt.Errorf("unknown route decision %d for key=%s", decision, key)
		}
	}

	return nil, fmt.Errorf("reshard routing exhausted retries for key=%s: %w", key, lastErr)
}

// attempt issues one upstream call and reports a retryable outcome as an error
// so the caller's loop can refresh the map and try again.
func (r *Router) attempt(doRequest upstreamCall, addr string, partitionID int) (*http.Response, error) {
	resp, err := doRequest(addr, partitionID)
	if err != nil {
		return nil, fmt.Errorf("request to %s: %w", addr, err)
	}

	retry, reason := r.shouldRetryResponse(resp)
	if retry {
		netutil.DrainAndClose(resp)
		return nil, fmt.Errorf("node %s returned %d: %w", addr, resp.StatusCode, reason)
	}
	return resp, nil
}

func (r *Router) shouldRetryResponse(resp *http.Response) (bool, error) {
	switch resp.StatusCode {
	case http.StatusMisdirectedRequest, http.StatusServiceUnavailable, http.StatusBadGateway:
		return true, fmt.Errorf("retryable status")
	case http.StatusConflict:
		// dbnode returns 409 with JSON {code:"NOT_MASTER"} when the LB routed to
		// a stale master, and the same code when a range has already moved to a
		// new master during a re-shard. Both mean: refresh the map and retry.
		body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
		if err != nil {
			resp.Body = io.NopCloser(bytes.NewReader(body))
			return false, nil
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))

		var er upstreamErrorResponse
		if err := json.Unmarshal(body, &er); err != nil {
			return false, nil
		}
		if er.Code == "NOT_MASTER" {
			return true, fmt.Errorf("not master")
		}
		return false, nil
	default:
		return false, nil
	}
}

// routeWithDualHash is the fallback used while DecideRouteForKey is unwritten:
// try the new owner, then the old one. It is correct for reads, because at most
// one of the two holds the key and a miss falls through. It is NOT correct for
// writes: during the pending phase the new owner accepts a write that the bulk
// copy then overwrites, silently losing it.
func (r *Router) routeWithDualHash(ctx context.Context, key string, isWrite bool, doRequest upstreamCall) (*http.Response, error) {
	for refreshAttempt := 0; refreshAttempt < 2; refreshAttempt++ {
		newOwner, newPartition, oldOwner, oldPartition := r.getNodesForDualHash(key, isWrite)

		if newOwner == "" && oldOwner == "" {
			return nil, ErrNoHealthyNode
		}

		// A 404 from the new owner is not an answer while a reshard is running:
		// the key may simply not have moved yet. Hold it and try the old owner,
		// and only fall back to it if the old owner cannot do better.
		var notFound *http.Response

		if newOwner != "" {
			resp, err := doRequest(newOwner, newPartition)
			switch {
			case err != nil:
				log.Printf("[router] new owner %s failed: %v, trying old owner", newOwner, err)
			case resp.StatusCode == http.StatusNotFound:
				notFound = resp
			case r.isSuccessResponse(resp):
				return resp, nil
			default:
				retry, _ := r.shouldRetryResponse(resp)
				netutil.DrainAndClose(resp)
				if retry {
					if err := r.topology.ForceRefresh(); err != nil {
						log.Printf("[router] topology refresh failed: %v", err)
					}
					continue
				}
			}
		}

		if oldOwner != "" && oldOwner != newOwner {
			resp, err := doRequest(oldOwner, oldPartition)
			if err != nil {
				log.Printf("[router] old owner %s failed: %v", oldOwner, err)
				if notFound != nil {
					return notFound, nil
				}
				return nil, fmt.Errorf("both owners failed: %w", err)
			}
			retry, _ := r.shouldRetryResponse(resp)
			if retry {
				netutil.DrainAndClose(resp)
				if err := r.topology.ForceRefresh(); err != nil {
					log.Printf("[router] topology refresh failed: %v", err)
				}
				continue
			}
			if resp.StatusCode == http.StatusNotFound && notFound != nil {
				netutil.DrainAndClose(resp)
				return notFound, nil
			}
			if notFound != nil {
				netutil.DrainAndClose(notFound)
			}
			return resp, nil
		}

		if notFound != nil {
			return notFound, nil
		}
		break
	}

	return nil, fmt.Errorf("dual-hash routing failed for key=%s", key)
}

func (r *Router) isSuccessResponse(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 300 ||
		resp.StatusCode == http.StatusNotFound
}

func (r *Router) ProxyRequest(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (r *Router) GetPartitionForKey(key string) int {
	return r.hashKey(key)
}

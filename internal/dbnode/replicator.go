package dbnode

import (
	"bytes"
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

type ReplicationEntry struct {
	PartitionID int
	Sequence    uint64
	Operation   string
	Key         string
	Value       []byte
	Version     uint64
	Timestamp   int64
	Replicas    []string
}

type Replicator struct {
	mu          sync.RWMutex
	nodeID      int
	queue       chan ReplicationEntry
	client      *http.Client
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	workerCount int
	retryDelay  time.Duration
	maxRetries  int
}

type ReplicatorConfig struct {
	NodeID      int
	QueueSize   int
	WorkerCount int
	RetryDelay  time.Duration
	MaxRetries  int
	Timeout     time.Duration
}

func DefaultReplicatorConfig(nodeID int) ReplicatorConfig {
	return ReplicatorConfig{
		NodeID:      nodeID,
		QueueSize:   10000,
		WorkerCount: 4,
		RetryDelay:  100 * time.Millisecond,
		MaxRetries:  3,
		Timeout:     5 * time.Second,
	}
}

func NewReplicator(cfg ReplicatorConfig) *Replicator {
	r := &Replicator{
		nodeID:      cfg.NodeID,
		queue:       make(chan ReplicationEntry, cfg.QueueSize),
		stopCh:      make(chan struct{}),
		workerCount: cfg.WorkerCount,
		retryDelay:  cfg.RetryDelay,
		maxRetries:  cfg.MaxRetries,
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   64,
				MaxConnsPerHost:       8,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
	}
	return r
}

func (r *Replicator) Start() {
	for i := 0; i < r.workerCount; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}
}

func (r *Replicator) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()
}

func (r *Replicator) Enqueue(entry ReplicationEntry) bool {
	select {
	case r.queue <- entry:
		return true
	default:
		log.Printf("[replicator] queue full, dropping entry seq=%d key=%s", entry.Sequence, entry.Key)
		return false
	}
}

func (r *Replicator) QueueLen() int {
	return len(r.queue)
}

func (r *Replicator) worker(id int) {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case entry := <-r.queue:
			r.replicateToAll(entry)
		}
	}
}

func (r *Replicator) replicateToAll(entry ReplicationEntry) {
	var wg sync.WaitGroup

	for _, replica := range entry.Replicas {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			r.replicateToOne(addr, entry)
		}(replica)
	}

	wg.Wait()
}

func (r *Replicator) replicateToOne(addr string, entry ReplicationEntry) {
	req := ReplicateRequest{
		PartitionID: entry.PartitionID,
		Sequence:    entry.Sequence,
		Operation:   entry.Operation,
		Key:         entry.Key,
		Value:       entry.Value,
		Version:     entry.Version,
		Timestamp:   entry.Timestamp,
	}

	body, err := json.Marshal(req)
	if err != nil {
		log.Printf("[replicator] marshal error: %v", err)
		return
	}

	url := fmt.Sprintf("http://%s/replicate", addr)

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-r.stopCh:
				return
			case <-time.After(r.retryDelay * time.Duration(attempt)):
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.client.Timeout)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			cancel()
			continue
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := r.client.Do(httpReq)
		cancel()

		if err != nil {
			log.Printf("[replicator] send to %s failed (attempt %d): %v", addr, attempt+1, err)
			continue
		}

		netutil.DrainAndClose(resp)

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}

		log.Printf("[replicator] send to %s got status %d (attempt %d)", addr, resp.StatusCode, attempt+1)
	}

	log.Printf("[replicator] failed to replicate to %s after %d attempts", addr, r.maxRetries+1)
}

type SnapshotSender struct {
	client  *http.Client
	timeout time.Duration
}

func NewSnapshotSender(timeout time.Duration) *SnapshotSender {
	return &SnapshotSender{
		client: &http.Client{
			Timeout: timeout,
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
		timeout: timeout,
	}
}

func (s *SnapshotSender) Send(addr string, partitionID int, data []byte) error {
	url := fmt.Sprintf("http://%s/snapshot?partition_id=%d", addr, partitionID)

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send snapshot: %w", err)
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot rejected with status %d", resp.StatusCode)
	}

	return nil
}

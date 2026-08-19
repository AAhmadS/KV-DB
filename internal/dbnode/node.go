package dbnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"

	"github.com/kvdb/kvdb/pkg/storage"
)

type NodeConfig struct {
	ID               int
	Port             int
	ReplicationPort  int
	DataDir          string
	ControllerURL    string
	WALSyncMode      string
	SnapshotInterval time.Duration
	NumPartitions    int

	// Replication tuning (optional). Zero values use defaults.
	ReplicatorWorkerCount int
	ReplicatorMaxRetries  int
	ReplicatorRetryDelay  time.Duration
	ReplicatorTimeout     time.Duration
}

func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		ID:               1,
		Port:             8001,
		ReplicationPort:  9001,
		DataDir:          "/data",
		ControllerURL:    "http://controller:8000",
		WALSyncMode:      "async",
		SnapshotInterval: 5 * time.Minute,
		NumPartitions:    16,

		ReplicatorWorkerCount: 0,
		ReplicatorMaxRetries:  0,
		ReplicatorRetryDelay:  0,
		ReplicatorTimeout:     0,
	}
}

type Node struct {
	mu               sync.RWMutex
	config           NodeConfig
	engine           *storage.Engine
	replicator       *Replicator
	migrator         *Migrator
	partitions       map[int]*PartitionRole
	partitionWAL     map[int]*PartitionWAL
	lastAppliedSeq   map[int]uint64
	controllerClient *http.Client
	partitionLocksMu sync.Mutex
	partitionLocks   map[int]*sync.RWMutex
	numPartitions    int
	startTime        time.Time

	// frozenRanges holds the hash ranges this node has stopped accepting
	// writes for because they are mid-handoff to a new master (spec 13.3).
	// Guarded by frozenMu, deliberately NOT by n.mu: the write path consults
	// it on every Set/Delete and must not contend with topology updates.
	frozenMu     sync.RWMutex
	frozenRanges map[string]frozenRange
}

// frozenRange is one entry of the freeze set: "keys whose hash modulo NewNum
// equals NewPartitionID". It is expressed in the NEW formula because that is
// the formula that defines the key set being handed over.
type frozenRange struct {
	NewNum         int
	NewPartitionID int
	Since          time.Time

	// Moved is set once the new master has announced readiness for this range.
	// A frozen range is read-only here; a moved range is not ours at all any
	// more, and requests for it must be bounced so a load balancer working
	// from a stale map corrects itself instead of reading a stale value.
	Moved bool
}

func NewNode(cfg NodeConfig) (*Node, error) {
	engineCfg := storage.Config{
		DataDir:          cfg.DataDir,
		WALSyncMode:      cfg.WALSyncMode,
		SnapshotInterval: cfg.SnapshotInterval,
		MemTableMaxSize:  64 * 1024 * 1024,
		InMemory:         true,
	}

	engine, err := storage.NewEngine(engineCfg)
	if err != nil {
		return nil, fmt.Errorf("create storage engine: %w", err)
	}

	replCfg := DefaultReplicatorConfig(cfg.ID)
	if cfg.ReplicatorWorkerCount > 0 {
		replCfg.WorkerCount = cfg.ReplicatorWorkerCount
	}
	if cfg.ReplicatorMaxRetries > 0 {
		replCfg.MaxRetries = cfg.ReplicatorMaxRetries
	}
	if cfg.ReplicatorRetryDelay > 0 {
		replCfg.RetryDelay = cfg.ReplicatorRetryDelay
	}
	if cfg.ReplicatorTimeout > 0 {
		replCfg.Timeout = cfg.ReplicatorTimeout
	}
	replicator := NewReplicator(replCfg)

	n := &Node{
		config:         cfg,
		engine:         engine,
		replicator:     replicator,
		partitions:     make(map[int]*PartitionRole),
		partitionWAL:   make(map[int]*PartitionWAL),
		lastAppliedSeq: make(map[int]uint64),
		controllerClient: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          128,
				MaxIdleConnsPerHost:   32,
				MaxConnsPerHost:       8,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		partitionLocks: make(map[int]*sync.RWMutex),
		numPartitions:  cfg.NumPartitions,
		startTime:      time.Now(),
		frozenRanges:   make(map[string]frozenRange),
	}

	n.migrator = NewMigrator(n)

	return n, nil
}

func (n *Node) Migrator() *Migrator {
	return n.migrator
}

// UpdateNumPartitions adopts a new partition count (spec 13.4: the controller
// announces that the new formula is now the only formula) and sheds any
// partition role whose id no longer exists under that count. Without the shed,
// shrinking the cluster would leave a node advertising ownership of partitions
// that the map no longer contains.
func (n *Node) UpdateNumPartitions(newNumPartitions int) {
	if newNumPartitions <= 0 {
		return
	}

	n.mu.Lock()
	n.numPartitions = newNumPartitions
	stale := make([]int, 0)
	for id := range n.partitions {
		if id >= newNumPartitions {
			stale = append(stale, id)
		}
	}
	n.mu.Unlock()

	for _, id := range stale {
		n.RemovePartition(id)
	}
}

// NumPartitions reports the partition count this node currently hashes with.
func (n *Node) NumPartitions() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.numPartitions
}

func (n *Node) Start() error {
	n.replicator.Start()
	return nil
}

func (n *Node) Stop() error {
	n.replicator.Stop()
	return n.engine.Close()
}

func (n *Node) ID() int {
	return n.config.ID
}

func (n *Node) SetPartitionRole(partitionID int, role Role, replicas []string) {
	lock := n.getPartitionLock(partitionID)
	lock.Lock()
	defer lock.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions[partitionID] = &PartitionRole{
		PartitionID: partitionID,
		Role:        role,
		Replicas:    replicas,
	}

	if _, ok := n.partitionWAL[partitionID]; !ok {
		n.partitionWAL[partitionID] = &PartitionWAL{}
	}
	if _, ok := n.lastAppliedSeq[partitionID]; !ok {
		n.lastAppliedSeq[partitionID] = 0
	}
}

func (n *Node) RemovePartition(partitionID int) {
	lock := n.getPartitionLock(partitionID)
	lock.Lock()
	defer lock.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitions, partitionID)
	delete(n.partitionWAL, partitionID)
	delete(n.lastAppliedSeq, partitionID)

	n.partitionLocksMu.Lock()
	delete(n.partitionLocks, partitionID)
	n.partitionLocksMu.Unlock()
}

func (n *Node) getPartitionLock(partitionID int) *sync.RWMutex {
	n.partitionLocksMu.Lock()
	defer n.partitionLocksMu.Unlock()
	if l, ok := n.partitionLocks[partitionID]; ok {
		return l
	}
	l := &sync.RWMutex{}
	n.partitionLocks[partitionID] = l
	return l
}

func (n *Node) appendPartitionWAL(partitionID int, op, key string, value []byte, version uint64, timestamp int64) uint64 {
	n.mu.Lock()
	w, ok := n.partitionWAL[partitionID]
	if !ok {
		w = &PartitionWAL{}
		n.partitionWAL[partitionID] = w
	}
	n.mu.Unlock()

	return w.Append(op, key, value, version, timestamp)
}

func (n *Node) partitionWALSequence(partitionID int) uint64 {
	n.mu.RLock()
	w := n.partitionWAL[partitionID]
	n.mu.RUnlock()
	if w == nil {
		return 0
	}
	return w.Sequence()
}

func (n *Node) resetPartitionWAL(partitionID int, sequence uint64) {
	n.mu.Lock()
	w, ok := n.partitionWAL[partitionID]
	if !ok {
		w = &PartitionWAL{}
		n.partitionWAL[partitionID] = w
	}
	n.mu.Unlock()

	w.Reset(sequence)
}

func (n *Node) GetWALEntries(partitionID int, afterSeq uint64, limit int) ([]PartitionWALEntry, uint64, error) {
	if !n.OwnsPartition(partitionID) {
		return nil, 0, ErrPartitionNotOwned
	}

	n.mu.RLock()
	w := n.partitionWAL[partitionID]
	n.mu.RUnlock()
	if w == nil {
		return nil, 0, nil
	}

	return w.EntriesSince(afterSeq, limit), w.Sequence(), nil
}

func (n *Node) GetPartitionRole(partitionID int) (*PartitionRole, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	pr, ok := n.partitions[partitionID]
	return pr, ok
}

func (n *Node) OwnsPartition(partitionID int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.partitions[partitionID]
	return ok
}

func (n *Node) IsMaster(partitionID int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	pr, ok := n.partitions[partitionID]
	return ok && pr.Role == RoleMaster
}

// partitionForKey applies this node's current formula. It reads numPartitions
// under the lock because a reshard rewrites it underneath live requests.
func (n *Node) partitionForKey(key string) int {
	return hashKeyToPartition(key, n.NumPartitions())
}

// Set applies this node's own formula to find the partition. Requests arriving
// from the load balancer use SetInPartition instead, because during a reshard
// the load balancer is the component that knows which of the two formulas
// applies to a given key (spec 13.2).
func (n *Node) Set(key string, value []byte) error {
	return n.SetInPartition(n.partitionForKey(key), key, value)
}

// SetInPartition writes key into an explicitly named partition.
func (n *Node) SetInPartition(partitionID int, key string, value []byte) error {
	if n.IsRangeFrozenForKey(key) {
		return ErrRangeFrozen
	}

	lock := n.getPartitionLock(partitionID)
	lock.Lock()
	defer lock.Unlock()

	n.mu.RLock()
	pr, ok := n.partitions[partitionID]
	n.mu.RUnlock()

	if !ok {
		return ErrPartitionNotOwned
	}

	if pr.Role != RoleMaster {
		return ErrNotMaster
	}

	if err := n.engine.Put(key, value); err != nil {
		return fmt.Errorf("storage put: %w", err)
	}

	entry, err := n.engine.GetEntry(key)
	if err != nil {
		return nil
	}

	walSeq := n.appendPartitionWAL(partitionID, "SET", key, value, entry.Version, entry.Timestamp)
	n.mu.Lock()
	n.lastAppliedSeq[partitionID] = walSeq
	n.mu.Unlock()
	if len(pr.Replicas) > 0 {
		n.replicator.Enqueue(ReplicationEntry{
			PartitionID: partitionID,
			Sequence:    walSeq,
			Operation:   "SET",
			Key:         key,
			Value:       value,
			Version:     entry.Version,
			Timestamp:   entry.Timestamp,
			Replicas:    pr.Replicas,
		})
	}

	return nil
}

// Get resolves the partition with this node's own formula.
func (n *Node) Get(key string) ([]byte, uint64, error) {
	return n.GetInPartition(n.partitionForKey(key), key)
}

// GetInPartition reads key from an explicitly named partition. Reads are
// allowed while a range is frozen: a frozen range is not accepting writes, so
// the copy held here is by definition the last consistent one.
func (n *Node) GetInPartition(partitionID int, key string) ([]byte, uint64, error) {
	lock := n.getPartitionLock(partitionID)
	lock.RLock()
	defer lock.RUnlock()

	if !n.OwnsPartition(partitionID) {
		return nil, 0, ErrPartitionNotOwned
	}

	entry, err := n.engine.GetEntry(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, err
	}

	if entry.Deleted {
		return nil, 0, ErrKeyNotFound
	}

	return entry.Value, entry.Version, nil
}

// Delete resolves the partition with this node's own formula.
func (n *Node) Delete(key string) error {
	return n.DeleteInPartition(n.partitionForKey(key), key)
}

// DeleteInPartition removes key from an explicitly named partition.
func (n *Node) DeleteInPartition(partitionID int, key string) error {
	if n.IsRangeFrozenForKey(key) {
		return ErrRangeFrozen
	}

	lock := n.getPartitionLock(partitionID)
	lock.Lock()
	defer lock.Unlock()

	n.mu.RLock()
	pr, ok := n.partitions[partitionID]
	n.mu.RUnlock()

	if !ok {
		return ErrPartitionNotOwned
	}

	if pr.Role != RoleMaster {
		return ErrNotMaster
	}

	if err := n.engine.Delete(key); err != nil {
		return fmt.Errorf("storage delete: %w", err)
	}

	entry, err := n.engine.GetEntry(key)
	if err != nil {
		return nil
	}

	walSeq := n.appendPartitionWAL(partitionID, "DELETE", key, nil, entry.Version, entry.Timestamp)
	n.mu.Lock()
	n.lastAppliedSeq[partitionID] = walSeq
	n.mu.Unlock()
	if len(pr.Replicas) > 0 {
		n.replicator.Enqueue(ReplicationEntry{
			PartitionID: partitionID,
			Sequence:    walSeq,
			Operation:   "DELETE",
			Key:         key,
			Value:       nil,
			Version:     entry.Version,
			Timestamp:   entry.Timestamp,
			Replicas:    pr.Replicas,
		})
	}

	return nil
}

func (n *Node) ApplyReplication(req *ReplicateRequest) error {
	lock := n.getPartitionLock(req.PartitionID)
	lock.Lock()
	defer lock.Unlock()

	if !n.OwnsPartition(req.PartitionID) {
		return ErrPartitionNotOwned
	}

	// Legacy/compat: if a replica receives entries without a WAL sequence, apply best-effort.
	if req.Sequence == 0 {
		return n.applyReplicationEntry(req.PartitionID, 0, req.Operation, req.Key, req.Value, req.Version, req.Timestamp)
	}

	n.mu.Lock()
	last, ok := n.lastAppliedSeq[req.PartitionID]
	if !ok {
		last = 0
		n.lastAppliedSeq[req.PartitionID] = 0
	}
	n.mu.Unlock()

	if req.Sequence <= last {
		return nil
	}

	// If we detect a gap, pull the missing WAL entries from the current master
	// before applying this one. Eventual consistency (spec 4) means a failed
	// catch-up must NOT reject the stream: we log the hole and apply what we
	// have, rather than wedging the replica behind an unreachable master.
	if req.Sequence > last+1 {
		if err := n.catchUpPartitionLocked(req.PartitionID, last, req.Sequence-1); err != nil {
			log.Printf("[node %d] partition %d: catch-up %d..%d failed: %v (applying seq %d anyway)",
				n.config.ID, req.PartitionID, last+1, req.Sequence-1, err, req.Sequence)
		}
		// Refresh last after catch-up.
		n.mu.Lock()
		last = n.lastAppliedSeq[req.PartitionID]
		n.mu.Unlock()
		if req.Sequence <= last {
			return nil
		}
	}

	return n.applyReplicationEntry(req.PartitionID, req.Sequence, req.Operation, req.Key, req.Value, req.Version, req.Timestamp)
}

func (n *Node) applyReplicationEntry(partitionID int, sequence uint64, operation, key string, value []byte, version uint64, timestamp int64) error {
	switch operation {
	case "SET":
		if err := n.engine.Put(key, value); err != nil {
			return err
		}
	case "DELETE":
		if err := n.engine.Delete(key); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown operation: %s", operation)
	}

	n.mu.RLock()
	w := n.partitionWAL[partitionID]
	n.mu.RUnlock()
	if w == nil {
		n.mu.Lock()
		w = &PartitionWAL{}
		n.partitionWAL[partitionID] = w
		n.mu.Unlock()
	}

	if sequence == 0 {
		seq := w.Append(operation, key, value, version, timestamp)
		n.mu.Lock()
		n.lastAppliedSeq[partitionID] = seq
		n.mu.Unlock()
		return nil
	}

	w.AppendWithSequence(sequence, operation, key, value, version, timestamp)
	n.mu.Lock()
	if prev := n.lastAppliedSeq[partitionID]; sequence > prev {
		n.lastAppliedSeq[partitionID] = sequence
	}
	n.mu.Unlock()
	return nil
}

type controllerMapResponse struct {
	Partitions []struct {
		ID         int    `json:"id"`
		MasterAddr string `json:"master_addr"`
	} `json:"partitions"`
}

func (n *Node) masterAddrForPartition(partitionID int) (string, error) {
	url := strings.TrimRight(n.config.ControllerURL, "/") + "/map"

	ctx, cancel := context.WithTimeout(context.Background(), n.controllerClient.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := n.controllerClient.Do(req)
	if err != nil {
		return "", err
	}
	defer netutil.DrainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("controller returned %d", resp.StatusCode)
	}

	var m controllerMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return "", err
	}
	if partitionID < 0 || partitionID >= len(m.Partitions) {
		return "", fmt.Errorf("partition %d out of range", partitionID)
	}
	return m.Partitions[partitionID].MasterAddr, nil
}

type walShipResponse struct {
	PartitionID    int                 `json:"partition_id"`
	LatestSequence uint64              `json:"latest_sequence"`
	Entries        []PartitionWALEntry `json:"entries"`
}

func (n *Node) fetchWALEntriesFromMaster(partitionID int, afterSeq uint64, limit int) ([]PartitionWALEntry, uint64, error) {
	masterAddr, err := n.masterAddrForPartition(partitionID)
	if err != nil {
		return nil, 0, err
	}
	if masterAddr == "" {
		return nil, 0, fmt.Errorf("no master for partition %d", partitionID)
	}

	url := fmt.Sprintf("http://%s/internal/wal?partition_id=%d&after_seq=%d&limit=%d", masterAddr, partitionID, afterSeq, limit)

	ctx, cancel := context.WithTimeout(context.Background(), n.controllerClient.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := n.controllerClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer netutil.DrainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("wal fetch returned %d: %s", resp.StatusCode, string(body))
	}

	var out walShipResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, err
	}
	return out.Entries, out.LatestSequence, nil
}

func (n *Node) catchUpPartitionLocked(partitionID int, fromSeq uint64, toSeq uint64) error {
	after := fromSeq
	for {
		if after >= toSeq {
			return nil
		}

		entries, latest, err := n.fetchWALEntriesFromMaster(partitionID, after, 1000)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return fmt.Errorf("unable to catch up partition %d: missing WAL entries after seq %d", partitionID, after)
		}

		for _, e := range entries {
			if e.Sequence <= after {
				continue
			}
			if e.Sequence > toSeq {
				break
			}
			if err := n.applyReplicationEntry(partitionID, e.Sequence, e.Operation, e.Key, e.Value, e.Version, e.Timestamp); err != nil {
				return err
			}
			after = e.Sequence
			if after >= toSeq {
				return nil
			}
		}

		if after >= latest {
			return fmt.Errorf("unable to catch up partition %d: master WAL ended at %d before %d", partitionID, latest, toSeq)
		}
	}
}

func (n *Node) ApplySnapshot(partitionID int, data []byte) error {
	return n.ApplySnapshotWithSequence(partitionID, data, 0)
}

func (n *Node) ApplySnapshotWithSequence(partitionID int, data []byte, walSeq uint64) error {
	lock := n.getPartitionLock(partitionID)
	lock.Lock()
	defer lock.Unlock()

	if !n.OwnsPartition(partitionID) {
		return ErrPartitionNotOwned
	}

	// Replace partition contents: delete any existing keys for this partition first,
	// then apply the provided snapshot.
	existing, err := n.engine.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot existing: %w", err)
	}
	for _, entry := range existing.Entries {
		if n.partitionForKey(entry.Key) != partitionID {
			continue
		}
		_ = n.engine.Delete(entry.Key)
	}

	snap, err := storage.ReadSnapshot(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	for _, entry := range snap.Entries {
		if entry.Deleted {
			if err := n.engine.Delete(entry.Key); err != nil {
				return err
			}
		} else {
			if err := n.engine.Put(entry.Key, entry.Value); err != nil {
				return err
			}
		}
	}
	// Reset local WAL after a snapshot reseed. If we know the master's WAL sequence,
	// advance to it so we don't attempt to catch up from 0.
	n.resetPartitionWAL(partitionID, walSeq)
	n.mu.Lock()
	n.lastAppliedSeq[partitionID] = walSeq
	n.mu.Unlock()

	return nil
}

func (n *Node) CreatePartitionSnapshot(partitionID int) ([]byte, error) {
	lock := n.getPartitionLock(partitionID)
	lock.RLock()
	defer lock.RUnlock()

	if !n.OwnsPartition(partitionID) {
		return nil, ErrPartitionNotOwned
	}

	snap, err := n.engine.Snapshot()
	if err != nil {
		return nil, err
	}

	filtered := make([]*storage.Entry, 0)
	for _, entry := range snap.Entries {
		if n.partitionForKey(entry.Key) == partitionID {
			filtered = append(filtered, entry)
		}
	}

	var buf bytes.Buffer
	if err := (&storage.Snapshot{Sequence: snap.Sequence, Entries: filtered}).Encode(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (n *Node) CreateSnapshot() ([]byte, error) {
	snap, err := n.engine.Snapshot()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := snap.Encode(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (n *Node) Stats() map[string]any {
	engineStats := n.engine.Stats()

	n.mu.RLock()
	partitionCount := len(n.partitions)
	masterCount := 0
	replicaCount := 0
	partitionWALLens := make(map[int]int, len(n.partitionWAL))
	for _, pr := range n.partitions {
		if pr.Role == RoleMaster {
			masterCount++
		} else {
			replicaCount++
		}
	}
	for partitionID, wal := range n.partitionWAL {
		partitionWALLens[partitionID] = wal.Len()
	}
	n.mu.RUnlock()

	return map[string]any{
		"node_id":            n.config.ID,
		"uptime_seconds":     int(time.Since(n.startTime).Seconds()),
		"partitions_total":   partitionCount,
		"partitions_master":  masterCount,
		"partitions_replica": replicaCount,
		"memtable_size":      engineStats.MemTableSize,
		"memtable_entries":   engineStats.MemTableEntries,
		"immutable_count":    engineStats.ImmutableCount,
		"wal_sequence":       engineStats.WALSequence,
		"replication_queue":  n.replicator.QueueLen(),
		"partition_wal":      partitionWALLens,
		"num_partitions":     n.NumPartitions(),
		"frozen_ranges":      n.FrozenRangeCount(),
	}
}

func (n *Node) HealthStatus() string {
	if n.engine == nil {
		return "unhealthy"
	}
	return "healthy"
}

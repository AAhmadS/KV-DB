package dbnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"

	"github.com/kvdb/kvdb/pkg/storage"
)

type MigrationTask struct {
	SourceNodeID   int    `json:"source_node_id"`
	TargetNodeAddr string `json:"target_node_addr"`
	OldPartitionID int    `json:"old_partition_id"`
	NewPartitionID int    `json:"new_partition_id"`
	OldNumParts    int    `json:"old_num_partitions"`
	NewNumParts    int    `json:"new_num_partitions"`
}

type MigrationResult struct {
	Status       string `json:"status"`
	KeysMigrated int    `json:"keys_migrated"`
	Error        string `json:"error,omitempty"`
}

type KeyBatch struct {
	Keys      []KeyValue `json:"keys"`
	Partition int        `json:"partition"`
}

type KeyValue struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Version   uint64 `json:"version"`
	Timestamp int64  `json:"timestamp"`
	Deleted   bool   `json:"deleted"`
}

type Migrator struct {
	mu          sync.Mutex
	node        *Node
	client      *http.Client
	batchSize   int
	inProgress  bool
	currentTask *MigrationTask
}

func NewMigrator(node *Node) *Migrator {
	return &Migrator{
		node:      node,
		batchSize: 100,
		client: &http.Client{
			Timeout: 30 * time.Second,
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
	}
}

func (m *Migrator) ExecuteMigration(task MigrationTask) (*MigrationResult, error) {
	m.mu.Lock()
	if m.inProgress {
		m.mu.Unlock()
		return nil, fmt.Errorf("migration already in progress")
	}
	m.inProgress = true
	m.currentTask = &task
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.inProgress = false
		m.currentTask = nil
		m.mu.Unlock()
	}()

	log.Printf("[migration] starting: partition %d -> %s (new partition %d)",
		task.OldPartitionID, task.TargetNodeAddr, task.NewPartitionID)

	keysToMigrate := m.findKeysToMigrate(task)
	if len(keysToMigrate) == 0 {
		log.Printf("[migration] no keys to migrate for partition %d", task.OldPartitionID)
		return &MigrationResult{
			Status:       "ok",
			KeysMigrated: 0,
		}, nil
	}

	log.Printf("[migration] found %d keys to migrate", len(keysToMigrate))

	migrated := 0
	for i := 0; i < len(keysToMigrate); i += m.batchSize {
		end := i + m.batchSize
		if end > len(keysToMigrate) {
			end = len(keysToMigrate)
		}

		batch := keysToMigrate[i:end]
		if err := m.sendBatch(task.TargetNodeAddr, task.NewPartitionID, batch); err != nil {
			return &MigrationResult{
				Status:       "error",
				KeysMigrated: migrated,
				Error:        err.Error(),
			}, err
		}

		migrated += len(batch)
		log.Printf("[migration] sent batch %d/%d (%d keys)",
			(i/m.batchSize)+1, (len(keysToMigrate)+m.batchSize-1)/m.batchSize, len(batch))
	}

	return &MigrationResult{
		Status:       "ok",
		KeysMigrated: migrated,
	}, nil
}

func (m *Migrator) findKeysToMigrate(task MigrationTask) []KeyValue {
	snap, err := m.node.engine.Snapshot()
	if err != nil {
		log.Printf("[migration] failed to get snapshot: %v", err)
		return nil
	}

	keysToMigrate := make([]KeyValue, 0)

	for _, entry := range snap.Entries {
		oldPartition := hashKeyToPartition(entry.Key, task.OldNumParts)
		if oldPartition != task.OldPartitionID {
			continue
		}

		newPartition := hashKeyToPartition(entry.Key, task.NewNumParts)
		if newPartition == task.NewPartitionID {
			keysToMigrate = append(keysToMigrate, KeyValue{
				Key:       entry.Key,
				Value:     entry.Value,
				Version:   entry.Version,
				Timestamp: entry.Timestamp,
				Deleted:   entry.Deleted,
			})
		}
	}

	return keysToMigrate
}

func hashKeyToPartition(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

func (m *Migrator) sendBatch(targetAddr string, partitionID int, keys []KeyValue) error {
	batch := KeyBatch{
		Keys:      keys,
		Partition: partitionID,
	}

	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/receive-migration", targetAddr)

	ctx, cancel := context.WithTimeout(context.Background(), m.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("target returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (m *Migrator) ReceiveMigration(batch KeyBatch) error {
	log.Printf("[migration] receiving batch of %d keys for partition %d", len(batch.Keys), batch.Partition)

	for _, kv := range batch.Keys {
		if kv.Deleted {
			if err := m.node.engine.Delete(kv.Key); err != nil {
				return fmt.Errorf("delete key %s: %w", kv.Key, err)
			}
		} else {
			if err := m.node.engine.Put(kv.Key, kv.Value); err != nil {
				return fmt.Errorf("put key %s: %w", kv.Key, err)
			}
		}
	}

	return nil
}

func (m *Migrator) IsInProgress() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inProgress
}

func (m *Migrator) GetCurrentTask() *MigrationTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentTask == nil {
		return nil
	}
	task := *m.currentTask
	return &task
}

func (n *Node) ExportPartitionData(partitionID int) ([]byte, error) {
	snap, err := n.engine.Snapshot()
	if err != nil {
		return nil, err
	}

	entries := make([]*storage.Entry, 0)
	for _, entry := range snap.Entries {
		partition := hashKeyToPartition(entry.Key, n.numPartitions)
		if partition == partitionID {
			entries = append(entries, entry)
		}
	}

	exportSnap := &storage.Snapshot{
		Sequence: snap.Sequence,
		Entries:  entries,
	}

	var buf bytes.Buffer
	if err := exportSnap.Encode(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (n *Node) ImportPartitionData(partitionID int, data []byte) error {
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

	return nil
}

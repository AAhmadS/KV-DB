// Package storage – Phase 1: pure in-memory LSM partition.
//
// Architectural truths enforced here:
//  1. Zero disk I/O: everything lives in RAM (maps + slices). Nothing in
//     this file touches the filesystem.
//  2. In-memory WAL + multi-MemTable LSM exist so the Master can snapshot
//     for a Replica WITHOUT blocking concurrent Set/Delete: Flush() briefly
//     swaps ActiveMemTable out, then CreateSnapshot() copies the frozen
//     ImmutableMemTables lock-free (under RLock) while new writes land in
//     a fresh ActiveMemTable and the WAL tail.
//
// Layout per partition:
//   - ActiveMemTable:      mutable map[string]Entry receiving live writes.
//   - ImmutableMemTables:   slice of frozen read-only maps, oldest first.
//   - WAL:                  in-memory slice []LogEntry, LSNs 1,2,3,...
//   - CheckpointLSN:        LSN watermarks – everything <= CheckpointLSN is
//                            captured in ImmutableMemTables; everything after
//                            is catch-up tail for replicas.
//
// Tombstones: Delete() writes Entry{Deleted:true} into ActiveMemTable so a
// Get() never resurrects an older value from an immutable layer. Compaction
// strips tombstones once layers are merged.
package storage

import (
	"fmt"
	"log"
	"sync"
)

// Role of a Partition on a given node. Master serves writes; Replica is
// read-only and receives the replication stream. Roles are reassigned by
// the Controller on failover (Phase 2).
type Role string

const (
	RoleMaster  Role = "MASTER"
	RoleReplica Role = "REPLICA"
)

// Operation codes stored in the in-memory WAL.
const (
	OpSet = "SET"
	OpDel = "DEL"
)

// LogEntry is one record of the in-memory Write-Ahead Log.
// LSN is a dense 1-based sequence number scoped to this Partition.
type LogEntry struct {
	LSN   uint64 `json:"lsn"`
	Op    string `json:"op"` // OpSet or OpDel
	Key   string `json:"key"`
	Value string `json:"value"` // empty for OpDel
}

// SnapshotData is what the Master streams to a new/returning Replica:
// the frozen immutable layers up to CheckpointLSN, plus the WAL needed
// for catch-up on writes that arrived during the transfer.
type SnapshotData struct {
	CheckpointLSN uint64
	Layers        []map[string]Entry
	WAL           []LogEntry
}

// DefaultFlushThreshold is the number of entries in ActiveMemTable that
// triggers an automatic Flush(). Small on purpose so tests and demos
// actually exercise the LSM path.
const DefaultFlushThreshold = 512

// Partition is ONE atomic shard unit: never split fractionally across
// nodes. A cluster has P partitions (0..P-1) spread over N nodes; each
// node holds full partitions as Master or Replica.
//
// Concurrency: every field is guarded by mu. Writers take Lock (briefly),
// readers take RLock. Replication-stream applies are serialised by the
// same mutex, so replicas apply entries without write conflicts.
type Partition struct {
	mu sync.RWMutex

	ID   int
	Role Role

	// ActiveMemTable receives live Set/Delete updates.
	ActiveMemTable map[string]Entry
	// ImmutableMemTables are frozen layers, oldest first.
	ImmutableMemTables []map[string]Entry
	// WAL is the in-memory write-ahead log, LSNs dense from 1.
	WAL []LogEntry

	// CheckpointLSN is the LSN of the last entry captured by the most
	// recent Flush(). WAL entries with LSN > CheckpointLSN are the
	// catch-up tail for replicas.
	CheckpointLSN uint64

	nextLSN        uint64 // next LSN to assign (== len(WAL)+1 while dense)
	flushThreshold int
}

// NewPartition creates an empty in-memory partition. flushThreshold <= 0
// selects DefaultFlushThreshold.
func NewPartition(id int, role Role, flushThreshold int) *Partition {
	if flushThreshold <= 0 {
		flushThreshold = DefaultFlushThreshold
	}
	return &Partition{
		ID:               id,
		Role:             role,
		ActiveMemTable:   make(map[string]Entry),
		flushThreshold:   flushThreshold,
		nextLSN:          1,
	}
}

// Set writes key=value on the Master path: append WAL, update Active.
// Returns the assigned LSN. Auto-flushes when Active hits the threshold.
func (p *Partition) Set(key, value string) uint64 {
	p.mu.Lock()
	lsn := p.nextLSN
	p.nextLSN++
	p.WAL = append(p.WAL, LogEntry{LSN: lsn, Op: OpSet, Key: key, Value: value})
	p.ActiveMemTable[key] = Entry{Key: key, Value: []byte(value)}
	needFlush := len(p.ActiveMemTable) >= p.flushThreshold
	p.mu.Unlock()

	log.Printf("[WAL-APPEND] partition=%d lsn=%d op=%s key=%q", p.ID, lsn, OpSet, key)
	if needFlush {
		p.Flush()
	}
	return lsn
}

// Delete writes a tombstone Entry{Value:"", Deleted:true} into Active so
// Get() cannot resurrect an older value from an immutable layer.
// Returns the assigned LSN.
func (p *Partition) Delete(key string) uint64 {
	p.mu.Lock()
	lsn := p.nextLSN
	p.nextLSN++
	p.WAL = append(p.WAL, LogEntry{LSN: lsn, Op: OpDel, Key: key})
	p.ActiveMemTable[key] = Entry{Key: key, Deleted: true}
	needFlush := len(p.ActiveMemTable) >= p.flushThreshold
	p.mu.Unlock()

	log.Printf("[WAL-APPEND] partition=%d lsn=%d op=%s key=%q (tombstone)", p.ID, lsn, OpDel, key)
	if needFlush {
		p.Flush()
	}
	return lsn
}

// Get searches ActiveMemTable first, then ImmutableMemTables newest to
// oldest. The FIRST layer containing the key wins: a tombstone there
// means "deleted" even if older layers hold a value.
func (p *Partition) Get(key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if e, ok := p.ActiveMemTable[key]; ok {
		if e.Deleted {
			return "", false
		}
		return string(e.Value), true
	}
	for i := len(p.ImmutableMemTables) - 1; i >= 0; i-- {
		layer := p.ImmutableMemTables[i]
		if e2, hit := layer[key]; hit {
			if e2.Deleted {
				return "", false
			}
			return string(e2.Value), true
		}
	}
	return "", false
}

// Flush freezes ActiveMemTable: briefly locks, appends it to
// ImmutableMemTables, records CheckpointLSN, allocates a fresh Active.
// Called automatically on threshold and explicitly before snapshots.
func (p *Partition) Flush() {
	p.mu.Lock()
	if len(p.ActiveMemTable) == 0 {
		p.mu.Unlock()
		return
	}
	frozen := p.ActiveMemTable
	p.ImmutableMemTables = append(p.ImmutableMemTables, frozen)
	p.ActiveMemTable = make(map[string]Entry)
	if len(p.WAL) > 0 {
		p.CheckpointLSN = p.WAL[len(p.WAL)-1].LSN
	}
	layers := len(p.ImmutableMemTables)
	ckpt := p.CheckpointLSN
	p.mu.Unlock()

	log.Printf("[LSM-FLUSH] partition=%d checkpointLSN=%d immutableLayers=%d", p.ID, ckpt, layers)
}

// Compact merges all ImmutableMemTables (oldest→newest, newer wins) into
// a single read-only layer and strips tombstones: after a full merge no
// older layer remains that a tombstone needs to shadow, so a tombstoned
// key is dropped entirely. ActiveMemTable is untouched.
func (p *Partition) Compact() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.ImmutableMemTables) <= 1 {
		return // nothing (or only one layer) to merge
	}
	merged := make(map[string]Entry)
	for _, layer := range p.ImmutableMemTables {
		for k, e := range layer {
			merged[k] = e // newer layers overwrite older
		}
	}
	// Strip tombstones: key deleted and no older layer left to shadow.
	for k, e := range merged {
		if e.Deleted {
			delete(merged, k)
		}
	}
	p.ImmutableMemTables = []map[string]Entry{merged}
	log.Printf("[LSM-COMPACT] partition=%d mergedLayers=%d liveKeys=%d", p.ID, len(p.ImmutableMemTables), len(merged))
}

// CreateSnapshot flushes Active (so the snapshot is point-in-time
// complete) then copies ImmutableMemTables + WAL under RLock. New writes
// landing after the copy go to the fresh Active and extend WAL beyond
// CheckpointLSN – the replica fetches those via WALSince(ckpt).
func (p *Partition) CreateSnapshot() SnapshotData {
	p.Flush() // brief write lock; no-op if Active empty

	p.mu.RLock()
	defer p.mu.RUnlock()

	layers := make([]map[string]Entry, len(p.ImmutableMemTables))
	for i, layer := range p.ImmutableMemTables {
		cp := make(map[string]Entry, len(layer))
		for k, e := range layer {
			if e.Value != nil {
				v := make([]byte, len(e.Value))
				copy(v, e.Value)
				e.Value = v
			}
			cp[k] = e
		}
		layers[i] = cp
	}
	wal := make([]LogEntry, len(p.WAL))
	copy(wal, p.WAL)
	ckpt := p.CheckpointLSN

	log.Printf("[REPL-STREAM] partition=%d snapshot checkpointLSN=%d layers=%d walEntries=%d",
		p.ID, ckpt, len(layers), len(wal))
	return SnapshotData{CheckpointLSN: ckpt, Layers: layers, WAL: wal}
}

// WALSince returns a copy of WAL entries with LSN > lsn: the catch-up
// tail a replica needs after receiving a snapshot at CheckpointLSN.
func (p *Partition) WALSince(lsn uint64) []LogEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var tail []LogEntry
	for _, e := range p.WAL {
		if e.LSN > lsn {
			tail = append(tail, e)
		}
	}
	return tail
}

// ApplyLogEntry applies one streamed replication entry on a Replica:
// sequential, no write conflicts. LSN is preserved so the replica WAL
// stays identical to the master WAL.
func (p *Partition) ApplyLogEntry(e LogEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyLocked(e)
}

// applyLocked assumes mu is held.
func (p *Partition) applyLocked(e LogEntry) {
	p.WAL = append(p.WAL, e)
	if e.LSN >= p.nextLSN {
		p.nextLSN = e.LSN + 1
	}
	switch e.Op {
	case OpSet:
		p.ActiveMemTable[e.Key] = Entry{Key: e.Key, Value: []byte(e.Value)}
	case OpDel:
		p.ActiveMemTable[e.Key] = Entry{Key: e.Key, Deleted: true}
	}
}

// ApplySnapshotAndWAL wipes local state (returning former-master rule:
// never reuse stale data) then installs the snapshot layers and replays
// the catch-up tail. After this the replica serves the same reads as the
// master at tail end.
func (p *Partition) ApplySnapshotAndWAL(snap SnapshotData, tail []LogEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ActiveMemTable = make(map[string]Entry)
	p.ImmutableMemTables = make([]map[string]Entry, len(snap.Layers))
	for i, layer := range snap.Layers {
		cp := make(map[string]Entry, len(layer))
		for k, e := range layer {
			if e.Value != nil {
				v := make([]byte, len(e.Value))
				copy(v, e.Value)
				e.Value = v
			}
			cp[k] = e
		}
		p.ImmutableMemTables[i] = cp
	}
	p.WAL = make([]LogEntry, len(snap.WAL))
	copy(p.WAL, snap.WAL)
	p.CheckpointLSN = snap.CheckpointLSN
	if len(p.WAL) > 0 {
		p.nextLSN = p.WAL[len(p.WAL)-1].LSN + 1
	} else {
		p.nextLSN = 1
	}
	for _, e := range tail {
		p.applyLocked(e) // advances nextLSN, lands in fresh Active
	}
	log.Printf("[REPL-STREAM] partition=%d applied snapshot ckpt=%d layers=%d + tail=%d entries (nextLSN=%d)",
		p.ID, snap.CheckpointLSN, len(snap.Layers), len(tail), p.nextLSN)
}

// Reset wipes ALL local in-memory state. Used when a failed node returns:
// a former master must Drop/Reset and re-sync from the new master,
// never take leadership back with stale data.
func (p *Partition) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ActiveMemTable = make(map[string]Entry)
	p.ImmutableMemTables = nil
	p.WAL = nil
	p.CheckpointLSN = 0
	p.nextLSN = 1
	log.Printf("[FAILOVER-RESET] partition=%d state wiped, awaiting snapshot from new master", p.ID)
}

// SetRole assigns MASTER or REPLICA (Controller-driven on failover).
func (p *Partition) SetRole(r Role) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Role = r
}

// Stats for tests / telemetry. Field copies only.
type PartitionStats struct {
	ActiveKeys     int
	ImmutableCount int
	ImmutableKeys  int
	WALLen         int
	CheckpointLSN  uint64
	NextLSN        uint64
	Role           Role
}

// Stats returns a point-in-time copy of partition counters.
func (p *Partition) Stats() PartitionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	keys := 0
	for _, l := range p.ImmutableMemTables {
		keys += len(l)
	}
	return PartitionStats{
		ActiveKeys:     len(p.ActiveMemTable),
		ImmutableCount: len(p.ImmutableMemTables),
		ImmutableKeys:  keys,
		WALLen:         len(p.WAL),
		CheckpointLSN:  p.CheckpointLSN,
		NextLSN:        p.nextLSN,
		Role:           p.Role,
	}
}

var _ = fmt.Sprintf // keep fmt import if telemetry formatting changes

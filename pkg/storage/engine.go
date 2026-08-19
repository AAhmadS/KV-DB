package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrEngineClosed = errors.New("engine closed")
)

type Config struct {
	DataDir          string
	WALSyncMode      string
	SnapshotInterval time.Duration
	MemTableMaxSize  int64
	InMemory         bool
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:          dataDir,
		WALSyncMode:      "async",
		SnapshotInterval: 5 * time.Minute,
		MemTableMaxSize:  64 * 1024 * 1024,
		InMemory:         false,
	}
}

type Engine struct {
	mu         sync.RWMutex
	config     Config
	wal        WALStore
	memtable   *MemTable
	immutables []*ImmutableTable
	version    atomic.Uint64
	closed     atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewEngine(cfg Config) (*Engine, error) {
	var wal WALStore
	if cfg.InMemory {
		wal = NewMemoryWAL()
	} else {
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return nil, fmt.Errorf("create data dir: %w", err)
		}

		walPath := filepath.Join(cfg.DataDir, "wal.log")
		fileWAL, err := OpenWAL(walPath, cfg.WALSyncMode)
		if err != nil {
			return nil, fmt.Errorf("open WAL: %w", err)
		}
		wal = fileWAL
	}

	e := &Engine{
		config:   cfg,
		wal:      wal,
		memtable: NewMemTable(),
		stopCh:   make(chan struct{}),
	}

	if !cfg.InMemory {
		if err := e.recover(); err != nil {
			wal.Close()
			return nil, fmt.Errorf("recovery: %w", err)
		}

		if cfg.SnapshotInterval > 0 {
			e.wg.Add(1)
			go e.snapshotLoop()
		}
	}

	return e, nil
}

func (e *Engine) recover() error {
	snapPath := filepath.Join(e.config.DataDir, "snapshot.dat")
	if snap, err := LoadSnapshot(snapPath); err == nil {
		for _, entry := range snap.Entries {
			if entry.Deleted {
				e.memtable.Delete(entry.Key, entry.Version, entry.Timestamp)
			} else {
				e.memtable.Put(entry.Key, entry.Value, entry.Version, entry.Timestamp)
			}
		}
		e.version.Store(snap.Sequence)
	}

	return e.wal.Replay(func(entry WALEntry) error {
		if entry.Sequence <= e.version.Load() {
			return nil
		}

		switch entry.Operation {
		case OpPut:
			e.memtable.Put(entry.Key, entry.Value, entry.Version, entry.Timestamp)
		case OpDelete:
			e.memtable.Delete(entry.Key, entry.Version, entry.Timestamp)
		}

		e.version.Store(entry.Sequence)
		return nil
	})
}

func (e *Engine) Put(key string, value []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	version := e.version.Add(1)
	timestamp := time.Now().UnixNano()

	seq, err := e.wal.Append(OpPut, key, value, version, timestamp)
	if err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	e.mu.Lock()
	e.memtable.Put(key, value, version, timestamp)
	shouldFreeze := e.memtable.Size() >= e.config.MemTableMaxSize
	e.mu.Unlock()

	if shouldFreeze {
		e.freezeMemTable()
	}

	_ = seq
	return nil
}

func (e *Engine) Delete(key string) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	version := e.version.Add(1)
	timestamp := time.Now().UnixNano()

	_, err := e.wal.Append(OpDelete, key, nil, version, timestamp)
	if err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	e.mu.Lock()
	e.memtable.Delete(key, version, timestamp)
	e.mu.Unlock()

	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if entry, ok := e.memtable.GetEntry(key); ok {
		if entry.Deleted {
			return nil, ErrKeyNotFound
		}
		return entry.Value, nil
	}

	for i := len(e.immutables) - 1; i >= 0; i-- {
		if entry, ok := e.immutables[i].GetEntry(key); ok {
			if entry.Deleted {
				return nil, ErrKeyNotFound
			}
			return entry.Value, nil
		}
	}

	return nil, ErrKeyNotFound
}

func (e *Engine) GetEntry(key string) (*Entry, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if entry, ok := e.memtable.GetEntry(key); ok {
		return entry, nil
	}

	for i := len(e.immutables) - 1; i >= 0; i-- {
		if entry, ok := e.immutables[i].GetEntry(key); ok {
			return entry, nil
		}
	}

	return nil, ErrKeyNotFound
}

func (e *Engine) freezeMemTable() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.memtable.Len() == 0 {
		return
	}

	imm := e.memtable.Freeze()
	e.immutables = append(e.immutables, imm)
}

func (e *Engine) Snapshot() (*Snapshot, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	merged := make(map[string]*Entry)

	for _, imm := range e.immutables {
		for _, entry := range imm.Entries() {
			merged[entry.Key] = entry
		}
	}

	for _, entry := range e.memtable.Entries() {
		merged[entry.Key] = entry
	}

	entries := make([]*Entry, 0, len(merged))
	for _, e := range merged {
		entries = append(entries, e)
	}

	return &Snapshot{
		Sequence: e.wal.Sequence(),
		Entries:  entries,
	}, nil
}

func (e *Engine) SnapshotTo(w io.Writer) error {
	snap, err := e.Snapshot()
	if err != nil {
		return err
	}
	return snap.Encode(w)
}

func (e *Engine) SaveSnapshot() error {
	if e.config.InMemory {
		// Memory-only mode: no disk persistence.
		return nil
	}

	snap, err := e.Snapshot()
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(e.config.DataDir, "snapshot.tmp")
	if err := SaveSnapshot(tmpPath, snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	snapPath := filepath.Join(e.config.DataDir, "snapshot.dat")
	if err := os.Rename(tmpPath, snapPath); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}

	e.mu.Lock()
	e.immutables = nil
	e.mu.Unlock()

	return e.wal.Truncate(snap.Sequence)
}

func (e *Engine) snapshotLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			_ = e.SaveSnapshot()
		}
	}
}

func (e *Engine) Sync() error {
	return e.wal.Sync()
}

func (e *Engine) Close() error {
	if e.closed.Swap(true) {
		return nil
	}

	close(e.stopCh)
	e.wg.Wait()

	if !e.config.InMemory {
		if err := e.SaveSnapshot(); err != nil {
			e.wal.Close()
			return err
		}
	}

	return e.wal.Close()
}

func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var immSize int64
	for _, imm := range e.immutables {
		for _, entry := range imm.Entries() {
			immSize += int64(len(entry.Key) + len(entry.Value))
		}
	}

	return EngineStats{
		MemTableSize:    e.memtable.Size(),
		MemTableEntries: e.memtable.Len(),
		ImmutableCount:  len(e.immutables),
		ImmutableSize:   immSize,
		WALSequence:     e.wal.Sequence(),
		Version:         e.version.Load(),
	}
}

type EngineStats struct {
	MemTableSize    int64
	MemTableEntries int
	ImmutableCount  int
	ImmutableSize   int64
	WALSequence     uint64
	Version         uint64
}

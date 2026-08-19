package dbnode

import "sync"

type PartitionWALEntry struct {
	Sequence  uint64
	Operation string
	Key       string
	Value     []byte
	Version   uint64
	Timestamp int64
}

type PartitionWAL struct {
	mu       sync.Mutex
	sequence uint64
	entries  []PartitionWALEntry
}

func (w *PartitionWAL) Reset(sequence uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sequence = sequence
	w.entries = nil
}

func (w *PartitionWAL) Append(operation, key string, value []byte, version uint64, timestamp int64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequence++
	seq := w.sequence

	w.appendLocked(seq, operation, key, value, version, timestamp)
	return seq
}

func (w *PartitionWAL) AppendWithSequence(sequence uint64, operation, key string, value []byte, version uint64, timestamp int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if sequence > w.sequence {
		w.sequence = sequence
	}

	// Ignore duplicates or out-of-order entries.
	if len(w.entries) > 0 {
		last := w.entries[len(w.entries)-1].Sequence
		if sequence <= last {
			return
		}
	}

	w.appendLocked(sequence, operation, key, value, version, timestamp)
}

func (w *PartitionWAL) appendLocked(sequence uint64, operation, key string, value []byte, version uint64, timestamp int64) {

	var copied []byte
	if value != nil {
		copied = make([]byte, len(value))
		copy(copied, value)
	}

	w.entries = append(w.entries, PartitionWALEntry{
		Sequence:  sequence,
		Operation: operation,
		Key:       key,
		Value:     copied,
		Version:   version,
		Timestamp: timestamp,
	})
}

func (w *PartitionWAL) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

func (w *PartitionWAL) EntriesSince(afterSeq uint64, limit int) []PartitionWALEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if limit <= 0 {
		limit = 1000
	}

	out := make([]PartitionWALEntry, 0)
	for _, e := range w.entries {
		if e.Sequence <= afterSeq {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (w *PartitionWAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

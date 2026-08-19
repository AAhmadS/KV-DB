package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"
)

type Entry struct {
	Key       string
	Value     []byte
	Version   uint64
	Timestamp int64
	Deleted   bool
}

type MemTable struct {
	mu      sync.RWMutex
	data    map[string]*Entry
	size    int64
	version uint64
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]*Entry),
	}
}

func (m *MemTable) Put(key string, value []byte, version uint64, timestamp int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.data[key]
	if old != nil {
		m.size -= int64(len(old.Key) + len(old.Value))
	}

	m.data[key] = &Entry{
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: timestamp,
		Deleted:   false,
	}
	m.size += int64(len(key) + len(value))

	if version > m.version {
		m.version = version
	}
}

func (m *MemTable) Delete(key string, version uint64, timestamp int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = &Entry{
		Key:       key,
		Value:     nil,
		Version:   version,
		Timestamp: timestamp,
		Deleted:   true,
	}

	if version > m.version {
		m.version = version
	}
}

func (m *MemTable) Get(key string) (*Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	if !ok || entry.Deleted {
		return nil, false
	}
	return entry, true
}

func (m *MemTable) GetEntry(key string) (*Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	return entry, ok
}

func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *MemTable) Version() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

func (m *MemTable) Entries() []*Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]*Entry, 0, len(m.data))
	for _, e := range m.data {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	return entries
}

func (m *MemTable) Clone() *MemTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clone := &MemTable{
		data:    make(map[string]*Entry, len(m.data)),
		size:    m.size,
		version: m.version,
	}

	for k, v := range m.data {
		entry := *v
		if v.Value != nil {
			entry.Value = make([]byte, len(v.Value))
			copy(entry.Value, v.Value)
		}
		clone.data[k] = &entry
	}
	return clone
}

type ImmutableTable struct {
	data    map[string]*Entry
	version uint64
}

func (m *MemTable) Freeze() *ImmutableTable {
	m.mu.Lock()
	defer m.mu.Unlock()

	imm := &ImmutableTable{
		data:    m.data,
		version: m.version,
	}

	m.data = make(map[string]*Entry)
	m.size = 0
	m.version = 0

	return imm
}

func (t *ImmutableTable) Get(key string) (*Entry, bool) {
	entry, ok := t.data[key]
	if !ok || entry.Deleted {
		return nil, false
	}
	return entry, true
}

func (t *ImmutableTable) GetEntry(key string) (*Entry, bool) {
	entry, ok := t.data[key]
	return entry, ok
}

func (t *ImmutableTable) Entries() []*Entry {
	entries := make([]*Entry, 0, len(t.data))
	for _, e := range t.data {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	return entries
}

func (t *ImmutableTable) Version() uint64 {
	return t.version
}

type Snapshot struct {
	Sequence uint64
	Entries  []*Entry
}

// Encode serialises the snapshot (gzip + length-prefixed entries) to w.
//
// NOTE: deliberately NOT named WriteTo. A method named WriteTo(io.Writer) must
// satisfy io.WriterTo, i.e. return (int64, error); the old name made this type
// look like an io.WriterTo to every reader and to go vet while behaving
// differently. See go vet 'stdmethods'.
func (s *Snapshot) Encode(w io.Writer) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()

	bw := bufio.NewWriter(gw)

	if err := binary.Write(bw, binary.BigEndian, s.Sequence); err != nil {
		return err
	}

	if err := binary.Write(bw, binary.BigEndian, uint64(len(s.Entries))); err != nil {
		return err
	}

	for _, e := range s.Entries {
		if err := writeEntry(bw, e); err != nil {
			return err
		}
	}

	return bw.Flush()
}

func ReadSnapshot(r io.Reader) (*Snapshot, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	br := bufio.NewReader(gr)

	var seq uint64
	if err := binary.Read(br, binary.BigEndian, &seq); err != nil {
		return nil, err
	}

	var count uint64
	if err := binary.Read(br, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	entries := make([]*Entry, 0, count)
	for i := uint64(0); i < count; i++ {
		e, err := readEntry(br)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	return &Snapshot{
		Sequence: seq,
		Entries:  entries,
	}, nil
}

func writeEntry(w *bufio.Writer, e *Entry) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(e.Key))); err != nil {
		return err
	}
	if _, err := w.WriteString(e.Key); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(e.Value))); err != nil {
		return err
	}
	if _, err := w.Write(e.Value); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, e.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, e.Timestamp); err != nil {
		return err
	}

	deleted := byte(0)
	if e.Deleted {
		deleted = 1
	}
	return w.WriteByte(deleted)
}

func readEntry(r *bufio.Reader) (*Entry, error) {
	var keyLen uint32
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	var valLen uint32
	if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
		return nil, err
	}

	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}

	var version uint64
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var timestamp int64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, err
	}

	deleted, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	return &Entry{
		Key:       string(key),
		Value:     value,
		Version:   version,
		Timestamp: timestamp,
		Deleted:   deleted == 1,
	}, nil
}

func SaveSnapshot(path string, snap *Snapshot) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return snap.Encode(f)
}

func LoadSnapshot(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadSnapshot(f)
}

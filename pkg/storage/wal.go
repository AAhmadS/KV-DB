package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrCorruptedWAL = errors.New("corrupted WAL entry")
	ErrWALClosed    = errors.New("WAL is closed")
)

type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
)

type WALEntry struct {
	Sequence  uint64
	Operation OpType
	Key       string
	Value     []byte
	Version   uint64
	Timestamp int64
}

// WALStore is the minimal interface the Engine needs for write-ahead logging.
// In memory-only mode, this can be implemented without disk I/O.
type WALStore interface {
	Append(op OpType, key string, value []byte, version uint64, timestamp int64) (uint64, error)
	Replay(fn func(WALEntry) error) error
	Sequence() uint64
	Sync() error
	Close() error
	Truncate(afterSeq uint64) error
}

type MemoryWAL struct {
	mu       sync.Mutex
	sequence uint64
}

func NewMemoryWAL() *MemoryWAL {
	return &MemoryWAL{}
}

func (w *MemoryWAL) Append(op OpType, key string, value []byte, version uint64, timestamp int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sequence++
	return w.sequence, nil
}

func (w *MemoryWAL) Replay(fn func(WALEntry) error) error {
	// Nothing to replay in memory-only mode.
	return nil
}

func (w *MemoryWAL) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

func (w *MemoryWAL) Sync() error {
	return nil
}

func (w *MemoryWAL) Close() error {
	return nil
}

func (w *MemoryWAL) Truncate(afterSeq uint64) error {
	return nil
}

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	sequence uint64
	syncMode string
	closed   bool
}

func OpenWAL(path string, syncMode string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:     file,
		writer:   bufio.NewWriter(file),
		syncMode: syncMode,
	}

	return wal, nil
}

func (w *WAL) Append(op OpType, key string, value []byte, version uint64, timestamp int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrWALClosed
	}

	w.sequence++
	entry := WALEntry{
		Sequence:  w.sequence,
		Operation: op,
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: timestamp,
	}

	if err := w.writeEntry(&entry); err != nil {
		return 0, err
	}

	if err := w.writer.Flush(); err != nil {
		return 0, err
	}

	if w.syncMode == "sync" {
		if err := w.file.Sync(); err != nil {
			return 0, err
		}
	}

	return w.sequence, nil
}

func (w *WAL) writeEntry(e *WALEntry) error {
	buf := make([]byte, 0, 64+len(e.Key)+len(e.Value))

	buf = binary.BigEndian.AppendUint64(buf, e.Sequence)
	buf = append(buf, byte(e.Operation))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Key)))
	buf = append(buf, e.Key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Value)))
	buf = append(buf, e.Value...)
	buf = binary.BigEndian.AppendUint64(buf, e.Version)
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Timestamp))
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))

	_, err := w.writer.Write(buf)
	return err
}

func (w *WAL) Replay(fn func(WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	var maxSeq uint64

	for {
		entry, err := w.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := fn(*entry); err != nil {
			return err
		}

		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}
	}

	w.sequence = maxSeq

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

func (w *WAL) readEntry(r *bufio.Reader) (*WALEntry, error) {
	header := make([]byte, 8+1+4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	seq := binary.BigEndian.Uint64(header[0:8])
	op := OpType(header[8])
	keyLen := binary.BigEndian.Uint32(header[9:13])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	valLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, valLenBuf); err != nil {
		return nil, err
	}
	valLen := binary.BigEndian.Uint32(valLenBuf)

	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}

	tail := make([]byte, 8+8+4)
	if _, err := io.ReadFull(r, tail); err != nil {
		return nil, err
	}

	version := binary.BigEndian.Uint64(tail[0:8])
	timestamp := int64(binary.BigEndian.Uint64(tail[8:16]))
	checksum := binary.BigEndian.Uint32(tail[16:20])

	buf := make([]byte, 0, len(header)+len(key)+4+len(value)+16)
	buf = append(buf, header...)
	buf = append(buf, key...)
	buf = append(buf, valLenBuf...)
	buf = append(buf, value...)
	buf = append(buf, tail[:16]...)

	if crc32.ChecksumIEEE(buf) != checksum {
		return nil, ErrCorruptedWAL
	}

	return &WALEntry{
		Sequence:  seq,
		Operation: op,
		Key:       string(key),
		Value:     value,
		Version:   version,
		Timestamp: timestamp,
	}, nil
}

func (w *WAL) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *WAL) Truncate(afterSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if afterSeq >= w.sequence {
		return w.file.Truncate(0)
	}
	return nil
}

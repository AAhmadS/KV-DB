package hash

import (
	"hash/fnv"
)

// HashKey returns the deterministic FNV-1a 32-bit hash of key.
// FNV-1a is used because it is fast, deterministic across processes
// (unlike Go's map hash), and has good distribution for short keys.
func HashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// PartitionForKey maps a key to a partition in [0, numPartitions).
// It is the single place where the hash-modulo routing decision lives,
// so re-sharding (Phase 2) only needs to change numPartitions.
func PartitionForKey(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	return int(HashKey(key) % uint32(numPartitions))
}

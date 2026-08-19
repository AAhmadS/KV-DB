package loadbalancer

import "errors"

var (
	ErrNoHealthyNode     = errors.New("no healthy node available")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrClusterMapEmpty   = errors.New("cluster map is empty")
	ErrKeyRequired       = errors.New("key is required")

	// ErrRangeInHandoff is returned while a key's hash range is mid-handoff
	// (spec 13.3): the old master has stopped accepting writes for it and the
	// new master has not announced readiness yet, so no node may take the
	// write. The load balancer surfaces this as 503 so the client retries.
	ErrRangeInHandoff = errors.New("hash range is mid-handoff; retry")
)

type PartitionInfo struct {
	ID         int      `json:"id"`
	MasterAddr string   `json:"master_addr"`
	MasterID   int      `json:"master_id"`
	Replicas   []string `json:"replicas"`
}

// RangeInfo is the controller's per-range migration state as published in /map.
// One entry per NEW partition; see internal/controller/reshard_state.go.
type RangeInfo struct {
	NewPartitionID int    `json:"new_partition_id"`
	State          string `json:"state"`
	KeysMoved      int    `json:"keys_moved"`
}

type ClusterMap struct {
	Partitions     []PartitionInfo `json:"partitions"`
	NumPartitions  int             `json:"num_partitions"`
	ReshardState   string          `json:"reshard_state"`
	Version        uint64          `json:"version"`
	NextPartitions []PartitionInfo `json:"next_partitions,omitempty"`

	// NextNumPartitions and Ranges are only populated while a re-shard is in
	// flight. They are the whole input to the routing decision in
	// reshard_route.go.
	NextNumPartitions int         `json:"next_num_partitions,omitempty"`
	Ranges            []RangeInfo `json:"ranges,omitempty"`
}

type HealthResponse struct {
	Status    string         `json:"status"`
	Component string         `json:"component"`
	Details   map[string]any `json:"details"`
}

type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

type ProxyResponse struct {
	Data    any    `json:"data,omitempty"`
	Version uint64 `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
	Code    string `json:"code,omitempty"`
}

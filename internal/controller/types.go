package controller

import "errors"

var (
	ErrNodeNotFound        = errors.New("node not found")
	ErrPartitionNotFound   = errors.New("partition not found")
	ErrNoHealthyReplica    = errors.New("no healthy replica available")
	ErrReshardInProgress   = errors.New("reshard already in progress")
	ErrInvalidReshardState = errors.New("invalid reshard state")
)

type RegisterRequest struct {
	Address         string `json:"address"`
	Port            int    `json:"port"`
	ReplicationPort int    `json:"replication_port"`
}

type RegisterResponse struct {
	NodeID        int    `json:"node_id"`
	NumPartitions int    `json:"num_partitions"`
	Message       string `json:"message"`
}

type HeartbeatRequest struct {
	NodeID int `json:"node_id"`
}

type HeartbeatResponse struct {
	Acknowledged bool   `json:"acknowledged"`
	Version      uint64 `json:"version"`
}

type ReshardRequest struct {
	NewNumPartitions int `json:"new_num_partitions"`
}

type RemoveNodeRequest struct {
	NodeID int `json:"node_id"`
}

type MovePartitionRequest struct {
	PartitionID  int `json:"partition_id"`
	TargetNodeID int `json:"target_node_id"`
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

type PartitionInfo struct {
	ID         int      `json:"id"`
	MasterAddr string   `json:"master_addr"`
	MasterID   int      `json:"master_id"`
	Replicas   []string `json:"replicas"`
}

type MapResponse struct {
	Partitions     []PartitionInfo `json:"partitions"`
	NumPartitions  int             `json:"num_partitions"`
	ReshardState   string          `json:"reshard_state"`
	Version        uint64          `json:"version"`
	NextPartitions []PartitionInfo `json:"next_partitions,omitempty"`

	// NextNumPartitions and Ranges only appear while a reshard is in flight.
	// Together they are exactly what the load balancer needs to answer, for a
	// given key, "may I use the new formula for this one yet?" (spec 13.2).
	NextNumPartitions int         `json:"next_num_partitions,omitempty"`
	Ranges            []RangeInfo `json:"ranges,omitempty"`
}

// RangeInfo is one hash range's migration state as seen by the load balancer.
// State is one of: pending, transferring, handoff, done.
type RangeInfo struct {
	NewPartitionID int    `json:"new_partition_id"`
	State          string `json:"state"`
	KeysMoved      int    `json:"keys_moved"`
}

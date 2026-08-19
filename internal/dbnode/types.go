package dbnode

import "errors"

type Role int

const (
	RoleReplica Role = iota
	RoleMaster
)

func (r Role) String() string {
	switch r {
	case RoleMaster:
		return "master"
	case RoleReplica:
		return "replica"
	default:
		return "unknown"
	}
}

type PartitionRole struct {
	PartitionID int
	Role        Role
	Replicas    []string
}

type ReplicateRequest struct {
	PartitionID int    `json:"partition_id"`
	Sequence    uint64 `json:"sequence"`
	Operation   string `json:"operation"`
	Key         string `json:"key"`
	Value       []byte `json:"value"`
	Version     uint64 `json:"version"`
	Timestamp   int64  `json:"timestamp"`
}

type SnapshotRequest struct {
	PartitionID int `json:"partition_id"`
}

type SetRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type GetResponse struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Details string `json:"details,omitempty"`
}

type HealthResponse struct {
	Status    string         `json:"status"`
	Component string         `json:"component"`
	NodeID    int            `json:"node_id"`
	Role      string         `json:"role"`
	Details   map[string]any `json:"details"`
}

var (
	ErrNotMaster         = errors.New("node is not master for this partition")
	ErrPartitionNotOwned = errors.New("partition not owned by this node")
	ErrKeyNotFound       = errors.New("key not found")
	ErrInvalidRequest    = errors.New("invalid request")
	// ErrRangeFrozen is returned during the handoff window of spec 13.3: this
	// node was the old master for the key, has stopped accepting writes for
	// its hash range, and the new master has not taken over yet. It is a
	// retryable condition, not a failure.
	ErrRangeFrozen = errors.New("hash range is frozen for handoff")
)

const (
	CodeNotMaster         = "NOT_MASTER"
	CodePartitionNotOwned = "PARTITION_NOT_OWNED"
	CodeKeyNotFound       = "KEY_NOT_FOUND"
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeInternalError     = "INTERNAL_ERROR"
	CodeRangeFrozen       = "RANGE_FROZEN"
)

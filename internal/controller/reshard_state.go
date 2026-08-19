package controller

import (
	"fmt"
	"sort"
	"time"
)

// RangeState tracks one hash range through the intermediate dual-formula state
// described in spec section 13.
//
// A "range" here is one *new* partition: the set of keys K for which
// hash(K) % newNumPartitions == NewPartitionID. That is the correct granularity
// for routing, because the load balancer's question is always "for this key,
// under the new formula, may I use the new formula yet?".
//
// The four states are the four answers, and they are what makes the
// intermediate state observable instead of merely asserted:
//
//	pending      nothing has moved. The OLD formula owns these keys.
//	transferring a bulk copy is in flight. The OLD formula still owns them:
//	             the old master is still accepting writes, so its copy is the
//	             authoritative one and the copy on the target is already stale.
//	handoff      the old master has stopped accepting writes for this key set
//	             (spec 13.3) and the new master has not announced readiness yet.
//	             Nobody may accept a write. Reads may still be served by the old
//	             master, whose copy is frozen and therefore consistent.
//	done         the new master has announced readiness to the controller.
//	             The NEW formula owns these keys.
type RangeState int

const (
	RangePending RangeState = iota
	RangeTransferring
	RangeHandoff
	RangeDone
)

func (s RangeState) String() string {
	switch s {
	case RangePending:
		return "pending"
	case RangeTransferring:
		return "transferring"
	case RangeHandoff:
		return "handoff"
	case RangeDone:
		return "done"
	default:
		return "unknown"
	}
}

// ParseRangeState is the inverse of String. Unknown input is treated as pending,
// which is the safe direction: pending means "keep using the old formula".
func ParseRangeState(s string) RangeState {
	switch s {
	case "transferring":
		return RangeTransferring
	case "handoff":
		return RangeHandoff
	case "done":
		return RangeDone
	default:
		return RangePending
	}
}

// MigrationRange is the controller's record for one hash range in flight.
type MigrationRange struct {
	NewPartitionID int        `json:"new_partition_id"`
	SourceParts    []int      `json:"source_partitions"`
	State          RangeState `json:"-"`
	StateName      string     `json:"state"`
	KeysMoved      int        `json:"keys_moved"`
	Error          string     `json:"error,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// sourcePartitionsFor returns every OLD partition that can contain a key
// belonging to NEW partition newPart.
//
// A key K lands in old partition o and new partition p, so we need every o for
// which some h satisfies h == o (mod oldN) and h == p (mod newN). By the Chinese
// Remainder Theorem that pair is solvable exactly when o == p (mod gcd(oldN,newN)).
//
// Worked examples, both of which appear in the spec:
//
//	4 -> 8: gcd = 4, so new partition 5 is fed only by old partition 1.
//	8 -> 4: gcd = 4, so new partition 1 is fed by old partitions 1 and 5.
func sourcePartitionsFor(oldN, newN, newPart int) []int {
	if oldN <= 0 || newN <= 0 {
		return nil
	}
	g := gcd(oldN, newN)
	out := make([]int, 0, oldN/g+1)
	for o := 0; o < oldN; o++ {
		if ((o-newPart)%g+g)%g == 0 {
			out = append(out, o)
		}
	}
	return out
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

// --- ClusterState range accessors -------------------------------------------
//
// All of these take cs.mu themselves; none of them may be called from a context
// that already holds it. (An earlier version of PrepareReshard called the
// exported GetHealthyNodes while holding the write lock, which deadlocked on
// every single invocation - sync.RWMutex is not reentrant - and meant section 13
// had never once executed.)

// SetRangeState advances one range. It refuses to move a range backwards so a
// late or duplicated callback cannot un-finish a completed handoff.
func (cs *ClusterState) SetRangeState(newPartitionID int, st RangeState) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	r, ok := cs.ranges[newPartitionID]
	if !ok {
		return fmt.Errorf("%w: range %d", ErrPartitionNotFound, newPartitionID)
	}
	if st < r.State {
		return fmt.Errorf("%w: range %d cannot go %s -> %s",
			ErrInvalidReshardState, newPartitionID, r.State, st)
	}
	r.State = st
	r.StateName = st.String()
	r.UpdatedAt = time.Now()
	cs.version++
	return nil
}

// SetRangeError records why a range could not complete. It does not change state.
func (cs *ClusterState) SetRangeError(newPartitionID int, msg string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if r, ok := cs.ranges[newPartitionID]; ok {
		r.Error = msg
		r.UpdatedAt = time.Now()
		cs.version++
	}
}

// AddRangeKeys accumulates how many keys have physically moved for a range.
func (cs *ClusterState) AddRangeKeys(newPartitionID, keys int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if r, ok := cs.ranges[newPartitionID]; ok {
		r.KeysMoved += keys
		r.UpdatedAt = time.Now()
	}
}

// GetRangeState reports one range's state and whether the range exists.
func (cs *ClusterState) GetRangeState(newPartitionID int) (RangeState, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	r, ok := cs.ranges[newPartitionID]
	if !ok {
		return RangePending, false
	}
	return r.State, true
}

// GetRanges returns a copy of every range record, ordered by new partition id.
func (cs *ClusterState) GetRanges() []*MigrationRange {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.copyRangesLocked()
}

func (cs *ClusterState) copyRangesLocked() []*MigrationRange {
	out := make([]*MigrationRange, 0, len(cs.ranges))
	for _, r := range cs.ranges {
		c := *r
		c.StateName = r.State.String()
		c.SourceParts = append([]int(nil), r.SourceParts...)
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NewPartitionID < out[j].NewPartitionID })
	return out
}

// AllRangesDone reports whether every hash range has transferred, which is the
// precondition for collapsing to a single formula (spec 13.4).
func (cs *ClusterState) AllRangesDone() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if len(cs.ranges) == 0 {
		return false
	}
	for _, r := range cs.ranges {
		if r.State != RangeDone {
			return false
		}
	}
	return true
}

// OldNumPartitions is the partition count the OLD formula uses while a reshard
// is in flight, and 0 when the cluster is stable.
func (cs *ClusterState) OldNumPartitions() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.reshardState == ReshardStateStable {
		return 0
	}
	return cs.partitions.NumPartitions
}

// NextNumPartitions is the partition count the NEW formula uses while a reshard
// is in flight, and 0 when the cluster is stable.
func (cs *ClusterState) NextNumPartitions() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.nextPartitions == nil {
		return 0
	}
	return cs.nextPartitions.NumPartitions
}

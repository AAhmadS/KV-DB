package loadbalancer

import "hash/fnv"

// Routing during a re-shard (spec section 13.2 and 13.3).
//
// While the partition count is changing there are two live formulas:
//
//	oldPartition = hash(key) % oldNumPartitions
//	newPartition = hash(key) % newNumPartitions
//
// and the load balancer is the component that has to decide, per key, which one
// applies right now. The spec is explicit about why this sits here and not in
// the nodes: "the load balancer must know that if a key belongs to partition X
// under the new formula but its data is still in the previous partition, it has
// to direct the request to the old partition."
//
// The controller publishes, in /map, the state of every hash range. The load
// balancer turns that into a per-key decision.

// RangeState mirrors the controller's per-range state as it appears in /map.
type RangeState string

const (
	RangePending      RangeState = "pending"
	RangeTransferring RangeState = "transferring"
	RangeHandoff      RangeState = "handoff"
	RangeDone         RangeState = "done"
)

// RouteDecision is the answer for one key.
//
// RouteUnimplemented is the zero value on purpose: an unwritten or accidentally
// empty decision function degrades to the legacy probe-both fallback rather
// than silently choosing a formula.
type RouteDecision int

const (
	RouteUnimplemented RouteDecision = iota
	RouteOld
	RouteNew
	RouteHandoff
)

func (d RouteDecision) String() string {
	switch d {
	case RouteOld:
		return "old"
	case RouteNew:
		return "new"
	case RouteHandoff:
		return "handoff"
	default:
		return "unimplemented"
	}
}

// ReshardRouteDecider is the pluggable form of the decision, so a test can
// drive the routing machinery with a known policy.
type ReshardRouteDecider func(key string, cm *ClusterMap, isWrite bool) RouteDecision

// DecideRouteForKey chooses which partitioning formula applies to one key while
// a re-shard is in flight. Implemented: the three cases below; the surrounding
// machinery (controller range states, node freeze/handoff/announce, router
// acting on all four results) was already finished.
//
//	Inputs
//	  key      the key being read or written
//	  cm       the cluster map as most recently fetched from the controller.
//	           cm.NumPartitions      is the OLD partition count
//	           cm.NextNumPartitions  is the NEW partition count
//	           cm.Partitions         is the OLD table (index = old partition id)
//	           cm.NextPartitions     is the NEW table (index = new partition id)
//	           cm.RangeStateFor(p)   is the migration state of NEW partition p
//	  isWrite  true for Set and Delete, false for Get
//
//	Helpers you already have
//	  cm.OldPartitionFor(key)  ==  hash(key) % cm.NumPartitions
//	  cm.NewPartitionFor(key)  ==  hash(key) % cm.NextNumPartitions
//	  cm.RangeStateFor(p)      ==  pending | transferring | handoff | done
//
// THE THREE CASES THIS MUST DISTINGUISH
//
//  1. The key's range has NOT moved yet - RangePending or RangeTransferring.
//     The old master is still accepting writes, so its copy is the only
//     authoritative one; a bulk copy that is already in flight is stale by
//     definition. Route by the OLD formula.  ->  RouteOld
//
//  2. The key's range is mid-handoff - RangeHandoff.
//     The old master has stopped accepting writes for this key set and the new
//     master has not yet announced readiness, so no node may accept a write
//     for it. A read is still safe from the old master, because frozen data is
//     by definition the last consistent version.
//     ->  RouteHandoff for a write, RouteOld for a read
//
//  3. The key's range HAS moved - RangeDone.
//     The new master announced to the controller that it applied everything
//     the old master sent. Route by the NEW formula.  ->  RouteNew
//
// Edge cases worth deciding explicitly rather than by accident:
//   - cm is nil, or cm.NextNumPartitions is 0 (no reshard in flight)
//   - RangeStateFor returns a state for a partition id that is out of range
//   - the old and new formulas happen to agree on the owner for this key,
//     which is common and lets you skip the whole question
//
// Returning RouteUnimplemented keeps the legacy fallback active, which probes
// the new owner and then the old owner. That fallback is safe for reads and
// NOT safe for writes - it can write to a node that is about to be overwritten
// by the final drain - which is precisely why this function exists. It is now
// only returned if a custom decider is installed that yields it.
// ===========================================================================
func DecideRouteForKey(key string, cm *ClusterMap, isWrite bool) RouteDecision {
	// No re-shard in flight: the old (only) formula is authoritative.
	if cm == nil || (cm.NextNumPartitions <= 0 && len(cm.NextPartitions) == 0) {
		return RouteOld
	}

	switch cm.RangeStateFor(cm.NewPartitionFor(key)) {
	case RangeDone:
		// New master announced it applied everything: new formula.
		return RouteNew
	case RangeHandoff:
		// Old master frozen, new master not ready yet. Writes must wait
		// (the caller's retry loop + client backoff cover the window);
		// reads are still safe from the frozen old copy.
		if isWrite {
			return RouteHandoff
		}
		return RouteOld
	default:
		// Pending or transferring: the old master still accepts writes,
		// so its copy is the only authoritative one. Old formula.
		return RouteOld
	}
}

// --- inputs to the decision -------------------------------------------------

// hashToPartition is the partitioning formula from the spec: FNV-1a over the
// key, modulo the partition count. Every component computes it identically;
// see internal/dbnode.hashKeyToPartition and internal/controller.
func hashToPartition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

// OldPartitionFor is hash(key) % oldNumPartitions.
func (cm *ClusterMap) OldPartitionFor(key string) int {
	if cm == nil {
		return 0
	}
	return hashToPartition(key, cm.NumPartitions)
}

// NewPartitionFor is hash(key) % newNumPartitions. It is 0 when no re-shard is
// in flight, which callers must not confuse with partition 0 - check
// NextNumPartitions first.
func (cm *ClusterMap) NewPartitionFor(key string) int {
	if cm == nil {
		return 0
	}
	n := cm.NextNumPartitions
	if n <= 0 {
		n = len(cm.NextPartitions)
	}
	return hashToPartition(key, n)
}

// RangeStateFor returns the migration state of one NEW partition. An id the
// controller has not published is reported as pending, which is the safe
// direction: pending means "keep using the old formula".
func (cm *ClusterMap) RangeStateFor(newPartitionID int) RangeState {
	if cm == nil {
		return RangePending
	}
	for i := range cm.Ranges {
		if cm.Ranges[i].NewPartitionID == newPartitionID {
			switch cm.Ranges[i].State {
			case string(RangeTransferring):
				return RangeTransferring
			case string(RangeHandoff):
				return RangeHandoff
			case string(RangeDone):
				return RangeDone
			default:
				return RangePending
			}
		}
	}
	return RangePending
}

package controller

import (
	"log"
	"net/http"
	"time"
)

// recoverRejoinedNode brings a node that has just registered *again* back into
// the cluster without losing the data it used to serve.
//
// Nodes hold their data in memory (spec 1: "in-memory"), so a process that
// restarts comes back empty. It also comes back on the same address and port,
// so the controller re-adopts it under the same id and it is immediately
// healthy again. Nothing in the failure path fires: from the controller's point
// of view the node never went away, because the restart is usually faster than
// the heartbeat timeout.
//
// The consequence, measured in test scenario 4 (restart the nodes one after
// another): the returning node is still master of its partitions, and those
// partitions are now empty. Every key it mastered reads as missing, even though
// a replica still holds it. Restart all three nodes in turn and the whole
// dataset is gone, while the cluster reports itself perfectly healthy.
//
// So a rejoin is treated as what it is - a node that lost its memory:
//
//	partitions it mastered   -> promote a healthy replica, then reseed it as one
//	partitions it replicated -> reseed from the current master
//	no healthy replica       -> nothing can be done; say so in the log
//
// The last case is a real data loss and is logged as one. It happens when the
// replication factor is 1, or when the only replica is also down.
func recoverRejoinedNode(client *http.Client, state *ClusterState, failover *FailoverManager, nodeID int) {
	clusterMap := state.GetClusterMap()
	if _, ok := clusterMap.Nodes[nodeID]; !ok {
		return
	}

	log.Printf("[rejoin] node %d re-registered; it holds no data, moving its partitions to replicas", nodeID)

	promoted, reseeded, lost := 0, 0, 0

	for _, assignment := range clusterMap.Current.Assignments {
		partitionID := assignment.PartitionID

		switch {
		case assignment.MasterID == nodeID:
			replicaID, ok := healthyReplicaFor(state, assignment, nodeID)
			if !ok {
				log.Printf("[rejoin] partition %d: node %d is master and no healthy replica holds a copy - the data for this partition is gone",
					partitionID, nodeID)
				lost++
				continue
			}
			if err := failover.TriggerManualFailover(partitionID, replicaID); err != nil {
				log.Printf("[rejoin] partition %d: promoting node %d failed: %v", partitionID, replicaID, err)
				continue
			}
			promoted++
			// The rejoined node is a replica of this partition now, so the same
			// reseed as any other replica applies.
			if reseedFromMaster(client, state, failover, partitionID, nodeID) {
				reseeded++
			}

		case holdsReplica(assignment, nodeID):
			if reseedFromMaster(client, state, failover, partitionID, nodeID) {
				reseeded++
			}
		}
	}

	log.Printf("[rejoin] node %d: %d partitions promoted away, %d reseeded, %d unrecoverable",
		nodeID, promoted, reseeded, lost)
}

func holdsReplica(assignment *PartitionAssignment, nodeID int) bool {
	for _, rid := range assignment.ReplicaIDs {
		if rid == nodeID {
			return true
		}
	}
	return false
}

func healthyReplicaFor(state *ClusterState, assignment *PartitionAssignment, excludeNodeID int) (int, bool) {
	for _, rid := range assignment.ReplicaIDs {
		if rid == excludeNodeID {
			continue
		}
		if node, ok := state.GetNode(rid); ok && node.Status == NodeStatusHealthy {
			return rid, true
		}
	}
	return 0, false
}

// reseedFromMaster copies one partition from its current master to the target.
// It re-announces roles first so the target knows it is a replica of this
// partition; a node refuses a snapshot for a partition it holds no role in.
func reseedFromMaster(client *http.Client, state *ClusterState, failover *FailoverManager, partitionID, targetID int) bool {
	masterID, ok := state.GetPartitionMaster(partitionID)
	if !ok || masterID == targetID {
		return false
	}

	master, ok := state.GetNode(masterID)
	if !ok || master.Status != NodeStatusHealthy {
		return false
	}

	target, ok := state.GetNode(targetID)
	if !ok || target.Status != NodeStatusHealthy {
		return false
	}

	if err := failover.NotifyPartitionRoles(partitionID); err != nil {
		log.Printf("[rejoin] partition %d: announcing roles failed: %v", partitionID, err)
		return false
	}

	if err := copyPartitionSnapshot(client, partitionID, master, target); err != nil {
		log.Printf("[rejoin] partition %d: reseed from node %d to node %d failed: %v",
			partitionID, masterID, targetID, err)
		return false
	}

	return true
}

// newRejoinHTTPClient is the client used for the snapshot copies above. A
// rejoin can move a whole partition, so it gets the same generous timeout as
// the rebalancer rather than the short one used for health checks.
func newRejoinHTTPClient() *http.Client {
	return newControllerHTTPClient(30 * time.Second)
}

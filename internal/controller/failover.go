package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"
)

type FailoverManager struct {
	mu     sync.Mutex
	state  *ClusterState
	client *http.Client
}

func NewFailoverManager(state *ClusterState) *FailoverManager {
	return &FailoverManager{
		state:  state,
		client: newControllerHTTPClient(5 * time.Second),
	}
}

func (f *FailoverManager) HandleNodeFailure(nodeID int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Printf("[failover] handling failure of node %d", nodeID)

	clusterMap := f.state.GetClusterMap()

	for _, assignment := range clusterMap.Current.Assignments {
		if assignment.MasterID == nodeID {
			f.promoteReplicaForPartition(assignment.PartitionID, nodeID)
		}
	}
}

func (f *FailoverManager) promoteReplicaForPartition(partitionID int, failedNodeID int) {
	replicas := f.state.GetPartitionReplicas(partitionID)

	var newMaster *NodeInfo
	for _, replicaID := range replicas {
		node, ok := f.state.GetNode(replicaID)
		if ok && node.Status == NodeStatusHealthy {
			newMaster = node
			break
		}
	}

	if newMaster == nil {
		log.Printf("[failover] partition %d: no healthy replica available for promotion", partitionID)
		return
	}

	log.Printf("[failover] partition %d: promoting node %d to master (was replica of failed node %d)",
		partitionID, newMaster.ID, failedNodeID)

	if !f.state.PromoteReplica(partitionID, newMaster.ID) {
		log.Printf("[failover] partition %d: failed to promote node %d", partitionID, newMaster.ID)
		return
	}

	log.Printf("[FAILOVER-PROMOTE] partition=%d newMaster=%d failedNode=%d",
		partitionID, newMaster.ID, failedNodeID)

	f.notifyNodeRoleChange(newMaster, partitionID, true)

	for _, replicaID := range replicas {
		if replicaID == newMaster.ID || replicaID == failedNodeID {
			continue
		}
		if node, ok := f.state.GetNode(replicaID); ok && node.Status == NodeStatusHealthy {
			f.notifyNodeRoleChange(node, partitionID, false)
		}
	}

	log.Printf("[failover] partition %d: failover complete, new master is node %d", partitionID, newMaster.ID)
}

func (f *FailoverManager) notifyNodeRoleChange(node *NodeInfo, partitionID int, isMaster bool) {
	url := fmt.Sprintf("http://%s:%d/internal/role", node.Address, node.Port)

	body := map[string]any{
		"partition_id": partitionID,
		"is_master":    isMaster,
		"replicas":     f.getReplicaAddresses(partitionID, node.ID),
	}

	data, err := json.Marshal(body)
	if err != nil {
		log.Printf("[failover] failed to marshal role change: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		log.Printf("[failover] failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		log.Printf("[failover] failed to notify node %d: %v", node.ID, err)
		return
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		log.Printf("[failover] node %d returned status %d for role change", node.ID, resp.StatusCode)
		return
	}

	// Announced role changes are logged because they are the only record of who
	// was told what, and in what order. When a node and the map disagree about
	// who masters a partition, this is the trail that shows which announcement
	// was overtaken.
	role := "replica"
	if isMaster {
		role = "master"
	}
	log.Printf("[failover] node %d <- partition %d role=%s", node.ID, partitionID, role)
}

func (f *FailoverManager) getReplicaAddresses(partitionID int, excludeNodeID int) []string {
	replicas := f.state.GetPartitionReplicas(partitionID)
	addresses := make([]string, 0)

	for _, rid := range replicas {
		if rid == excludeNodeID {
			continue
		}
		if node, ok := f.state.GetNode(rid); ok && node.Status == NodeStatusHealthy {
			addresses = append(addresses, fmt.Sprintf("%s:%d", node.Address, node.ReplicationPort))
		}
	}

	return addresses
}

func (f *FailoverManager) TriggerManualFailover(partitionID int, newMasterID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	node, ok := f.state.GetNode(newMasterID)
	if !ok {
		return ErrNodeNotFound
	}

	if node.Status != NodeStatusHealthy {
		return fmt.Errorf("node %d is not healthy", newMasterID)
	}

	oldMasterID, _ := f.state.GetPartitionMaster(partitionID)

	if !f.state.PromoteReplica(partitionID, newMasterID) {
		return fmt.Errorf("failed to promote node %d for partition %d", newMasterID, partitionID)
	}

	f.notifyNodeRoleChange(node, partitionID, true)

	if oldNode, ok := f.state.GetNode(oldMasterID); ok && oldNode.Status == NodeStatusHealthy {
		f.notifyNodeRoleChange(oldNode, partitionID, false)
	}

	log.Printf("[failover] manual failover complete: partition %d master changed from %d to %d",
		partitionID, oldMasterID, newMasterID)

	return nil
}

func (f *FailoverManager) NotifyPartitionRoles(partitionID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	clusterMap := f.state.GetClusterMap()
	if partitionID < 0 || partitionID >= len(clusterMap.Current.Assignments) {
		return ErrPartitionNotFound
	}

	assignment := clusterMap.Current.Assignments[partitionID]
	if assignment.MasterID < 0 {
		return ErrPartitionNotFound
	}

	masterNode, ok := f.state.GetNode(assignment.MasterID)
	if ok && masterNode.Status == NodeStatusHealthy {
		f.notifyNodeRoleChange(masterNode, partitionID, true)
	}

	holders := make(map[int]struct{}, len(assignment.ReplicaIDs)+1)
	holders[assignment.MasterID] = struct{}{}

	for _, rid := range assignment.ReplicaIDs {
		holders[rid] = struct{}{}
		replicaNode, ok := f.state.GetNode(rid)
		if !ok || replicaNode.Status != NodeStatusHealthy {
			continue
		}
		f.notifyNodeRoleChange(replicaNode, partitionID, false)
	}

	// Anyone else must stop claiming this partition. Announcing only to the
	// holders leaves stale roles behind in two places that matter: a demoted
	// master that comes back still believes it is master, and after a reshard
	// the previous owner of a partition keeps mastering it alongside the new
	// one. Both are silent until something routes by node rather than by map.
	// Skip anything that shares a socket with a holder as well as the holders
	// themselves. Two map entries can point at one process if a node ever gets
	// registered twice, and a drop sent to that socket would undo the role the
	// holder was just given.
	holderAddrs := make(map[string]struct{}, len(holders))
	for id := range holders {
		if n, ok := clusterMap.Nodes[id]; ok {
			holderAddrs[fmt.Sprintf("%s:%d", n.Address, n.Port)] = struct{}{}
		}
	}

	for _, node := range clusterMap.Nodes {
		if node.Status != NodeStatusHealthy {
			continue
		}
		if _, holds := holders[node.ID]; holds {
			continue
		}
		if _, sameSocket := holderAddrs[fmt.Sprintf("%s:%d", node.Address, node.Port)]; sameSocket {
			continue
		}
		f.notifyNodeRoleDrop(node, partitionID)
	}

	return nil
}

// notifyNodeRoleDrop tells one node it holds no role in a partition. The node's
// data is untouched; only the role bookkeeping is removed.
func (f *FailoverManager) notifyNodeRoleDrop(node *NodeInfo, partitionID int) {
	url := fmt.Sprintf("http://%s:%d/internal/role", node.Address, node.Port)

	data, err := json.Marshal(map[string]any{
		"partition_id": partitionID,
		"drop":         true,
	})
	if err != nil {
		log.Printf("[failover] failed to marshal role drop: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		log.Printf("[failover] failed to create role-drop request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		log.Printf("[failover] failed to drop role on node %d: %v", node.ID, err)
		return
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		log.Printf("[failover] node %d returned status %d for role drop", node.ID, resp.StatusCode)
		return
	}

	log.Printf("[failover] node %d <- partition %d role=none", node.ID, partitionID)
}

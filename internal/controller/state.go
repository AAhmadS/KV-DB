package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusHealthy
	NodeStatusSuspect
	NodeStatusDead
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusHealthy:
		return "healthy"
	case NodeStatusSuspect:
		return "suspect"
	case NodeStatusDead:
		return "dead"
	default:
		return "unknown"
	}
}

type ReshardState int

const (
	ReshardStateStable ReshardState = iota
	ReshardStatePreparing
	ReshardStateMigrating
	ReshardStateCompleting
)

func (s ReshardState) String() string {
	switch s {
	case ReshardStateStable:
		return "stable"
	case ReshardStatePreparing:
		return "preparing"
	case ReshardStateMigrating:
		return "migrating"
	case ReshardStateCompleting:
		return "completing"
	default:
		return "unknown"
	}
}

type NodeInfo struct {
	ID              int        `json:"id"`
	Address         string     `json:"address"`
	Port            int        `json:"port"`
	ReplicationPort int        `json:"replication_port"`
	Status          NodeStatus `json:"status"`
	LastHeartbeat   time.Time  `json:"last_heartbeat"`
	JoinedAt        time.Time  `json:"joined_at"`
}

// MarshalJSON writes the status as its name. The zero value of an int enum is
// indistinguishable from "unknown" on the wire, and `"status":1` tells an
// operator reading /nodes nothing at all.
func (s NodeStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON accepts either form, so an older client that sends the number
// still parses.
func (s *NodeStatus) UnmarshalJSON(b []byte) error {
	var n int
	if err := json.Unmarshal(b, &n); err == nil {
		*s = NodeStatus(n)
		return nil
	}
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return err
	}
	switch name {
	case "healthy":
		*s = NodeStatusHealthy
	case "suspect":
		*s = NodeStatusSuspect
	case "dead":
		*s = NodeStatusDead
	default:
		*s = NodeStatusUnknown
	}
	return nil
}

func (n *NodeInfo) Addr() string {
	return n.Address
}

type PartitionAssignment struct {
	PartitionID int    `json:"partition_id"`
	MasterID    int    `json:"master_id"`
	ReplicaIDs  []int  `json:"replica_ids"`
	Version     uint64 `json:"version"`
}

type PartitionTable struct {
	NumPartitions int                    `json:"num_partitions"`
	Assignments   []*PartitionAssignment `json:"assignments"`
	Version       uint64                 `json:"version"`
}

type ClusterMap struct {
	Nodes             map[int]*NodeInfo `json:"nodes"`
	Current           *PartitionTable   `json:"current"`
	Next              *PartitionTable   `json:"next,omitempty"`
	Ranges            []*MigrationRange `json:"ranges,omitempty"`
	ReshardState      ReshardState      `json:"reshard_state"`
	ReplicationFactor int               `json:"replication_factor"`
	Version           uint64            `json:"version"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type ClusterState struct {
	mu                sync.RWMutex
	nodes             map[int]*NodeInfo
	partitions        *PartitionTable
	nextPartitions    *PartitionTable
	reshardState      ReshardState
	ranges            map[int]*MigrationRange
	replicationFactor int
	numPartitions     int
	version           uint64
	nodeIDCounter     int
}

func NewClusterState(numPartitions, replicationFactor int) *ClusterState {
	cs := &ClusterState{
		nodes:             make(map[int]*NodeInfo),
		ranges:            make(map[int]*MigrationRange),
		replicationFactor: replicationFactor,
		numPartitions:     numPartitions,
		reshardState:      ReshardStateStable,
	}

	cs.partitions = &PartitionTable{
		NumPartitions: numPartitions,
		Assignments:   make([]*PartitionAssignment, numPartitions),
		Version:       1,
	}

	for i := 0; i < numPartitions; i++ {
		cs.partitions.Assignments[i] = &PartitionAssignment{
			PartitionID: i,
			MasterID:    -1,
			ReplicaIDs:  []int{},
			Version:     1,
		}
	}

	return cs
}

// RegisterNode adds a node, or re-adopts one that is already known.
//
// Identity is (address, port), not a counter. A node that restarts and
// registers again is the same node: it listens on the same socket and, as far
// as the rest of the cluster is concerned, is the same participant. Minting a
// fresh id instead would leave two map entries pointing at one process, and
// then every per-node announcement would be sent twice with different content -
// the second one, arriving as "you hold no role here", silently unassigning a
// partition the map says that node masters.
// The second return value reports whether this was a rejoin rather than a first
// registration. A rejoined node has lost everything it held in memory, and the
// caller has to repair that; see recoverRejoinedNode.
func (cs *ClusterState) RegisterNode(address string, port, replicationPort int) (*NodeInfo, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, existing := range cs.sortedNodesLocked() {
		if existing.Address != address || existing.Port != port {
			continue
		}
		existing.ReplicationPort = replicationPort
		existing.Status = NodeStatusHealthy
		existing.LastHeartbeat = time.Now()
		cs.version++
		cs.rebalancePartitions()
		return existing, true
	}

	cs.nodeIDCounter++
	node := &NodeInfo{
		ID:              cs.nodeIDCounter,
		Address:         address,
		Port:            port,
		ReplicationPort: replicationPort,
		Status:          NodeStatusHealthy,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	cs.nodes[node.ID] = node
	cs.version++
	cs.rebalancePartitions()

	return node, false
}

func (cs *ClusterState) UpdateHeartbeat(nodeID int) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	node, ok := cs.nodes[nodeID]
	if !ok {
		return false
	}

	node.LastHeartbeat = time.Now()
	if node.Status == NodeStatusSuspect {
		node.Status = NodeStatusHealthy
	}

	return true
}

func (cs *ClusterState) SetNodeStatus(nodeID int, status NodeStatus) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if node, ok := cs.nodes[nodeID]; ok {
		node.Status = status
	}
}

func (cs *ClusterState) RemoveNode(nodeID int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	delete(cs.nodes, nodeID)
	cs.version++
	cs.rebalancePartitions()
}

func (cs *ClusterState) GetNode(nodeID int) (*NodeInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	node, ok := cs.nodes[nodeID]
	if !ok {
		return nil, false
	}

	copy := *node
	return &copy, true
}

func (cs *ClusterState) GetAllNodes() []*NodeInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(cs.nodes))
	for _, n := range cs.nodes {
		copy := *n
		nodes = append(nodes, &copy)
	}
	return nodes
}

func (cs *ClusterState) GetHealthyNodes() []*NodeInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.healthyNodesLocked()
}

// healthyNodesLocked is the caller-already-holds-cs.mu variant. Every internal
// caller must use this one: cs.mu is a sync.RWMutex and Go's RWMutex is not
// reentrant, so taking RLock while already holding Lock deadlocks the whole
// controller goroutine.
//
// Results are sorted by node ID. Ranging over cs.nodes (a map) produced a
// different partition-to-node assignment on every run, which made both
// rebalancing and reshard planning non-reproducible.
func (cs *ClusterState) healthyNodesLocked() []*NodeInfo {
	nodes := make([]*NodeInfo, 0, len(cs.nodes))
	for _, n := range cs.nodes {
		if n.Status == NodeStatusHealthy {
			c := *n
			nodes = append(nodes, &c)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

// sortedNodesLocked returns every node, healthy or not, ordered by ID.
func (cs *ClusterState) sortedNodesLocked() []*NodeInfo {
	nodes := make([]*NodeInfo, 0, len(cs.nodes))
	for _, n := range cs.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (cs *ClusterState) GetClusterMap() *ClusterMap {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	nodesCopy := make(map[int]*NodeInfo)
	for id, n := range cs.nodes {
		copy := *n
		nodesCopy[id] = &copy
	}

	cm := &ClusterMap{
		Nodes:             nodesCopy,
		Current:           cs.copyPartitionTable(cs.partitions),
		ReshardState:      cs.reshardState,
		ReplicationFactor: cs.replicationFactor,
		Version:           cs.version,
		UpdatedAt:         time.Now(),
	}

	if cs.nextPartitions != nil {
		cm.Next = cs.copyPartitionTable(cs.nextPartitions)
	}
	if len(cs.ranges) > 0 {
		cm.Ranges = cs.copyRangesLocked()
	}

	return cm
}

func (cs *ClusterState) copyPartitionTable(pt *PartitionTable) *PartitionTable {
	if pt == nil {
		return nil
	}

	copy := &PartitionTable{
		NumPartitions: pt.NumPartitions,
		Assignments:   make([]*PartitionAssignment, len(pt.Assignments)),
		Version:       pt.Version,
	}

	for i, a := range pt.Assignments {
		replicas := make([]int, len(a.ReplicaIDs))
		copy2 := *a
		copy2.ReplicaIDs = replicas
		for j, r := range a.ReplicaIDs {
			copy2.ReplicaIDs[j] = r
		}
		copy.Assignments[i] = &copy2
	}

	return copy
}

func (cs *ClusterState) GetPartitionMaster(partitionID int) (int, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(cs.partitions.Assignments) {
		return -1, false
	}

	masterID := cs.partitions.Assignments[partitionID].MasterID
	return masterID, masterID >= 0
}

func (cs *ClusterState) GetPartitionReplicas(partitionID int) []int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(cs.partitions.Assignments) {
		return nil
	}

	replicas := cs.partitions.Assignments[partitionID].ReplicaIDs
	result := make([]int, len(replicas))
	copy(result, replicas)
	return result
}

func (cs *ClusterState) PromoteReplica(partitionID int, newMasterID int) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if partitionID < 0 || partitionID >= len(cs.partitions.Assignments) {
		return false
	}

	assignment := cs.partitions.Assignments[partitionID]

	found := false
	newReplicas := make([]int, 0, len(assignment.ReplicaIDs))
	for _, rid := range assignment.ReplicaIDs {
		if rid == newMasterID {
			found = true
		} else {
			newReplicas = append(newReplicas, rid)
		}
	}

	if !found {
		return false
	}

	if assignment.MasterID >= 0 {
		newReplicas = append(newReplicas, assignment.MasterID)
	}

	assignment.MasterID = newMasterID
	assignment.ReplicaIDs = newReplicas
	assignment.Version++
	cs.partitions.Version++
	cs.version++

	return true
}

func (cs *ClusterState) MovePartition(partitionID int, newMasterID int) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if partitionID < 0 || partitionID >= len(cs.partitions.Assignments) {
		return ErrPartitionNotFound
	}

	newMaster, ok := cs.nodes[newMasterID]
	if !ok {
		return ErrNodeNotFound
	}
	if newMaster.Status == NodeStatusDead {
		return ErrNodeNotFound
	}

	assignment := cs.partitions.Assignments[partitionID]
	oldMasterID := assignment.MasterID

	if oldMasterID == newMasterID {
		return nil
	}

	// Build replica list: start from existing replicas + old master, then fill from healthy nodes.
	replicaSet := make(map[int]struct{})
	for _, rid := range assignment.ReplicaIDs {
		if rid == newMasterID {
			continue
		}
		if node, ok := cs.nodes[rid]; ok && node.Status != NodeStatusDead {
			replicaSet[rid] = struct{}{}
		}
	}
	if oldMasterID >= 0 && oldMasterID != newMasterID {
		if node, ok := cs.nodes[oldMasterID]; ok && node.Status != NodeStatusDead {
			replicaSet[oldMasterID] = struct{}{}
		}
	}

	needed := cs.replicationFactor - 1
	newReplicas := make([]int, 0, needed)
	for rid := range replicaSet {
		if len(newReplicas) >= needed {
			break
		}
		newReplicas = append(newReplicas, rid)
	}

	if len(newReplicas) < needed {
		for _, node := range cs.sortedNodesLocked() {
			if len(newReplicas) >= needed {
				break
			}
			if node.Status == NodeStatusDead {
				continue
			}
			if node.ID == newMasterID {
				continue
			}
			_, exists := replicaSet[node.ID]
			if exists {
				continue
			}
			replicaSet[node.ID] = struct{}{}
			newReplicas = append(newReplicas, node.ID)
		}
	}

	assignment.MasterID = newMasterID
	assignment.ReplicaIDs = newReplicas
	assignment.Version++
	cs.partitions.Version++
	cs.version++

	return nil
}

func (cs *ClusterState) rebalancePartitions() {
	healthyNodes := make([]*NodeInfo, 0)
	for _, n := range cs.sortedNodesLocked() {
		if n.Status == NodeStatusHealthy || n.Status == NodeStatusUnknown {
			healthyNodes = append(healthyNodes, n)
		}
	}

	if len(healthyNodes) == 0 {
		return
	}

	for i, assignment := range cs.partitions.Assignments {
		if assignment.MasterID < 0 || cs.nodes[assignment.MasterID] == nil ||
			cs.nodes[assignment.MasterID].Status == NodeStatusDead {
			nodeIdx := i % len(healthyNodes)
			assignment.MasterID = healthyNodes[nodeIdx].ID
			assignment.Version++
		}

		validReplicas := make([]int, 0)
		for _, rid := range assignment.ReplicaIDs {
			if node, ok := cs.nodes[rid]; ok && node.Status != NodeStatusDead {
				validReplicas = append(validReplicas, rid)
			}
		}
		// Enforce current replication factor by trimming dead/extra replicas.
		maxReplicas := cs.replicationFactor - 1
		if maxReplicas < 0 {
			maxReplicas = 0
		}
		if len(validReplicas) > maxReplicas {
			validReplicas = validReplicas[:maxReplicas]
		}
		assignment.ReplicaIDs = validReplicas

		neededReplicas := cs.replicationFactor - 1 - len(assignment.ReplicaIDs)
		if neededReplicas > 0 {
			// Offset the search by partition id so replicas spread across the
			// cluster instead of every partition choosing the same node.
			for k := 0; k < len(healthyNodes); k++ {
				node := healthyNodes[(i+1+k)%len(healthyNodes)]
				if neededReplicas <= 0 {
					break
				}
				if node.ID == assignment.MasterID {
					continue
				}
				isReplica := false
				for _, rid := range assignment.ReplicaIDs {
					if rid == node.ID {
						isReplica = true
						break
					}
				}
				if !isReplica {
					assignment.ReplicaIDs = append(assignment.ReplicaIDs, node.ID)
					neededReplicas--
				}
			}
		}
	}

	cs.partitions.Version++
}

func (cs *ClusterState) SetReplicationFactor(replicationFactor int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if replicationFactor < 1 {
		replicationFactor = 1
	}
	cs.replicationFactor = replicationFactor
	cs.rebalancePartitions()
	cs.version++
}

// PrepareReshard computes the new partition model and the per-range migration
// plan, then enters the intermediate dual-formula state (spec 13.1, 13.2).
// The current table stays authoritative for every key until that key's range
// reports done; nothing has moved at this point.
func (cs *ClusterState) PrepareReshard(newNumPartitions int) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.reshardState != ReshardStateStable {
		return ErrReshardInProgress
	}
	if newNumPartitions <= 0 {
		return fmt.Errorf("%w: new_num_partitions must be > 0", ErrInvalidReshardState)
	}

	oldNumPartitions := cs.partitions.NumPartitions
	if newNumPartitions == oldNumPartitions {
		return fmt.Errorf("%w: cluster is already at %d partitions", ErrInvalidReshardState, newNumPartitions)
	}

	healthyNodes := cs.healthyNodesLocked()
	if len(healthyNodes) == 0 {
		return ErrNoHealthyReplica
	}

	cs.nextPartitions = &PartitionTable{
		NumPartitions: newNumPartitions,
		Assignments:   make([]*PartitionAssignment, newNumPartitions),
		Version:       1,
	}

	cs.ranges = make(map[int]*MigrationRange, newNumPartitions)
	now := time.Now()

	for i := 0; i < newNumPartitions; i++ {
		nodeIdx := i % len(healthyNodes)
		cs.nextPartitions.Assignments[i] = &PartitionAssignment{
			PartitionID: i,
			MasterID:    healthyNodes[nodeIdx].ID,
			ReplicaIDs:  []int{},
			Version:     1,
		}

		cs.ranges[i] = &MigrationRange{
			NewPartitionID: i,
			SourceParts:    sourcePartitionsFor(oldNumPartitions, newNumPartitions, i),
			State:          RangePending,
			StateName:      RangePending.String(),
			UpdatedAt:      now,
		}
	}

	cs.reshardState = ReshardStatePreparing
	cs.version++

	return nil
}

func (cs *ClusterState) StartMigration() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.reshardState != ReshardStatePreparing {
		return ErrInvalidReshardState
	}

	cs.reshardState = ReshardStateMigrating
	cs.version++

	return nil
}

func (cs *ClusterState) CompleteMigration() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.reshardState != ReshardStateMigrating {
		return ErrInvalidReshardState
	}

	cs.reshardState = ReshardStateCompleting
	cs.version++

	return nil
}

func (cs *ClusterState) FinalizeReshard() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.reshardState != ReshardStateCompleting {
		return ErrInvalidReshardState
	}

	cs.partitions = cs.nextPartitions
	cs.nextPartitions = nil
	cs.numPartitions = cs.partitions.NumPartitions
	cs.reshardState = ReshardStateStable
	cs.ranges = make(map[int]*MigrationRange)
	// The next table was planned with masters only. Refill replicas so the
	// collapse does not silently drop the cluster to replication factor 1.
	cs.rebalancePartitions()
	cs.version++

	return nil
}

func (cs *ClusterState) CancelReshard() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.nextPartitions = nil
	cs.ranges = make(map[int]*MigrationRange)
	cs.reshardState = ReshardStateStable
	cs.version++
}

func (cs *ClusterState) ReshardState() ReshardState {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.reshardState
}

func (cs *ClusterState) Version() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.version
}

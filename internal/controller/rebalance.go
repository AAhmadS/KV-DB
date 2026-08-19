package controller

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Rebalancer redistributes partition masters across the healthy nodes.
//
// The spec requires this in two places: section 2.1 ("if a node is added or
// removed ... the controller is responsible for redistributing the partitions")
// and test scenario 5 ("start with a few nodes under load, add a node during
// the run, the controller rebalances the partitions").
//
// It matters more than it looks. Partition assignment happens when a node
// registers, and the first node to register is alone at that moment, so it
// becomes master of every partition. Nothing subsequently takes any away:
// rebalancePartitions only reassigns a partition whose master is missing or
// dead. Without this file, a three-node cluster serves every request from one
// node and adding nodes changes nothing at all - which would make the RPS
// scaling scenario measure nothing but the load generator.
//
// A move is done in the order that cannot lose a write:
//
//	freeze the partition on the current master   - no new writes for these keys
//	copy the partition's data to the target      - now complete, because frozen
//	switch the master in the map and announce it - target starts serving
//	unfreeze                                     - old master is now a replica
//
// A write that arrives at the frozen master is refused with 503, the load
// balancer retries, and by then the map names the new master. A write that
// arrives after the switch but at the old master is refused with 409
// NOT_MASTER, which the load balancer also retries. Neither is lost.
type Rebalancer struct {
	mu       sync.Mutex
	state    *ClusterState
	failover *FailoverManager
	client   *http.Client

	running   bool
	lastRunAt time.Time
	lastMoves int
	lastError string
	totalRuns int
}

// PartitionMove is one planned relocation.
type PartitionMove struct {
	PartitionID int `json:"partition_id"`
	FromNodeID  int `json:"from_node_id"`
	ToNodeID    int `json:"to_node_id"`
}

func NewRebalancer(state *ClusterState, failover *FailoverManager) *Rebalancer {
	return &Rebalancer{
		state:    state,
		failover: failover,
		client:   newControllerHTTPClient(30 * time.Second),
	}
}

// Plan computes the moves that would even out master ownership. It reads state
// and changes nothing, so it is also what the control panel shows as a preview.
func (rb *Rebalancer) Plan() []PartitionMove {
	cm := rb.state.GetClusterMap()
	if cm.Current == nil || len(cm.Current.Assignments) == 0 {
		return nil
	}

	healthy := make([]*NodeInfo, 0, len(cm.Nodes))
	for _, n := range cm.Nodes {
		if n.Status == NodeStatusHealthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) < 2 {
		return nil
	}
	sort.Slice(healthy, func(i, j int) bool { return healthy[i].ID < healthy[j].ID })

	numPartitions := len(cm.Current.Assignments)
	numNodes := len(healthy)

	// Target ownership: the first (numPartitions % numNodes) nodes carry one
	// extra partition, everyone else carries the floor.
	base := numPartitions / numNodes
	extra := numPartitions % numNodes

	target := make(map[int]int, numNodes)
	for i, n := range healthy {
		target[n.ID] = base
		if i < extra {
			target[n.ID]++
		}
	}

	owned := make(map[int][]int, numNodes)
	for _, a := range cm.Current.Assignments {
		if a.MasterID < 0 {
			continue
		}
		if _, ok := target[a.MasterID]; !ok {
			// Master is not healthy; failover owns that case, not rebalancing.
			continue
		}
		owned[a.MasterID] = append(owned[a.MasterID], a.PartitionID)
	}
	for id := range owned {
		sort.Ints(owned[id])
	}

	// Nodes that need partitions, in id order so the plan is deterministic.
	needy := make([]int, 0, numNodes)
	for _, n := range healthy {
		for i := len(owned[n.ID]); i < target[n.ID]; i++ {
			needy = append(needy, n.ID)
		}
	}
	if len(needy) == 0 {
		return nil
	}

	moves := make([]PartitionMove, 0, len(needy))
	next := 0

	for _, n := range healthy {
		surplus := len(owned[n.ID]) - target[n.ID]
		for i := 0; i < surplus && next < len(needy); i++ {
			// Give away the highest-numbered partitions first; the low ones
			// tend to already be where they belong.
			partitionID := owned[n.ID][len(owned[n.ID])-1-i]
			moves = append(moves, PartitionMove{
				PartitionID: partitionID,
				FromNodeID:  n.ID,
				ToNodeID:    needy[next],
			})
			next++
		}
	}

	return moves
}

// Run executes the plan. It returns how many partitions actually moved, which
// is not necessarily len(Plan()): a move whose source or target stops being
// healthy mid-run is skipped rather than forced.
func (rb *Rebalancer) Run() (int, error) {
	rb.mu.Lock()
	if rb.running {
		rb.mu.Unlock()
		return 0, fmt.Errorf("rebalance already in progress")
	}
	if rb.state.ReshardState() != ReshardStateStable {
		rb.mu.Unlock()
		return 0, fmt.Errorf("%w: cannot rebalance while a reshard is in flight", ErrReshardInProgress)
	}
	rb.running = true
	rb.mu.Unlock()

	defer func() {
		rb.mu.Lock()
		rb.running = false
		rb.lastRunAt = time.Now()
		rb.totalRuns++
		rb.mu.Unlock()
	}()

	moves := rb.Plan()
	if len(moves) == 0 {
		rb.setResult(0, "")
		return 0, nil
	}

	log.Printf("[rebalance] plan: %d partitions to move across %d nodes",
		len(moves), len(rb.state.GetHealthyNodes()))

	done := 0
	var firstErr error

	for _, mv := range moves {
		if err := rb.movePartition(mv); err != nil {
			log.Printf("[rebalance] partition %d: %v", mv.PartitionID, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		done++
	}

	msg := ""
	if firstErr != nil {
		msg = firstErr.Error()
	}
	rb.setResult(done, msg)

	log.Printf("[rebalance] moved %d of %d planned partitions", done, len(moves))
	return done, firstErr
}

// movePartition relocates one partition's master without losing writes.
func (rb *Rebalancer) movePartition(mv PartitionMove) (err error) {
	// The plan was computed from a snapshot. Ownership can change between then
	// and now - a manual move, a failover promotion, an earlier move in this
	// same plan - and acting on a stale plan is worse than skipping: the source
	// refuses the snapshot with 421 and the partition is left half-moved.
	currentMaster, ok := rb.state.GetPartitionMaster(mv.PartitionID)
	if !ok || currentMaster != mv.FromNodeID {
		return fmt.Errorf("plan is stale: partition %d is mastered by node %d, not %d",
			mv.PartitionID, currentMaster, mv.FromNodeID)
	}

	source, ok := rb.state.GetNode(mv.FromNodeID)
	if !ok || source.Status != NodeStatusHealthy {
		return fmt.Errorf("source node %d is not healthy", mv.FromNodeID)
	}
	target, ok := rb.state.GetNode(mv.ToNodeID)
	if !ok || target.Status != NodeStatusHealthy {
		return fmt.Errorf("target node %d is not healthy", mv.ToNodeID)
	}

	// The freeze is expressed in the same terms the reshard uses. With the old
	// and new partition counts equal, "the keys of new partition p" is exactly
	// "the keys of partition p", so the same primitive works unchanged.
	// OldNumPartitions is deliberately not used here: it reports 0 outside a
	// reshard, and a zero partition count makes FreezeRange a silent no-op.
	numPartitions := rb.state.GetClusterMap().Current.NumPartitions
	if numPartitions <= 0 {
		return fmt.Errorf("cluster reports %d partitions", numPartitions)
	}

	spec := rangeSpec{
		OldNumPartitions: numPartitions,
		NewNumPartitions: numPartitions,
		NewPartitionID:   mv.PartitionID,
	}

	if err := postJSONToNode(rb.client, source, "/internal/range/freeze", spec, nil); err != nil {
		return fmt.Errorf("freeze on node %d: %w", source.ID, err)
	}
	defer func() {
		if err := postJSONToNode(rb.client, source, "/internal/range/unfreeze", spec, nil); err != nil {
			log.Printf("[rebalance] unfreeze partition %d on node %d: %v", mv.PartitionID, source.ID, err)
		}
	}()

	// From here on a node has been told something about its role. If the move
	// then fails, that node keeps the role it was given while the map still
	// says something else, and every write to the partition is refused with
	// 409 NOT_MASTER until the next map change - indefinitely, in practice.
	// Re-announcing the partition from the map is the undo.
	defer func() {
		if err == nil {
			return
		}
		if nerr := rb.failover.NotifyPartitionRoles(mv.PartitionID); nerr != nil {
			log.Printf("[rebalance] partition %d: could not restore roles after a failed move: %v",
				mv.PartitionID, nerr)
		}
	}()

	// Give the target a replica role first. A node refuses a snapshot for a
	// partition it holds no role in (421 PARTITION_NOT_OWNED), and it is the
	// honest order anyway: the target joins as a replica, catches up, and is
	// only then promoted.
	if err := postJSONToNode(rb.client, target, "/internal/role", map[string]any{
		"partition_id": mv.PartitionID,
		"is_master":    false,
		"replicas":     []string{},
	}, nil); err != nil {
		return fmt.Errorf("pre-assign replica role on node %d: %w", target.ID, err)
	}

	if err := copyPartitionSnapshot(rb.client, mv.PartitionID, source, target); err != nil {
		return fmt.Errorf("copy partition %d to node %d: %w", mv.PartitionID, target.ID, err)
	}

	if err := rb.state.MovePartition(mv.PartitionID, mv.ToNodeID); err != nil {
		return fmt.Errorf("reassign partition %d: %w", mv.PartitionID, err)
	}

	if err := rb.failover.NotifyPartitionRoles(mv.PartitionID); err != nil {
		return fmt.Errorf("announce partition %d: %w", mv.PartitionID, err)
	}

	log.Printf("[rebalance] partition %d moved from node %d to node %d",
		mv.PartitionID, mv.FromNodeID, mv.ToNodeID)
	return nil
}

// TriggerAsync runs a rebalance in the background. Registration uses it, so a
// node that joins during a load run does not block its own /register response
// on a data copy.
func (rb *Rebalancer) TriggerAsync(reason string) {
	go func() {
		moved, err := rb.Run()
		switch {
		case err != nil:
			log.Printf("[rebalance] triggered by %s: %v", reason, err)
		case moved > 0:
			log.Printf("[rebalance] triggered by %s: %d partitions moved", reason, moved)
		}
	}()
}

func (rb *Rebalancer) setResult(moves int, errMsg string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.lastMoves = moves
	rb.lastError = errMsg
}

// Status is what /rebalance/status returns.
func (rb *Rebalancer) Status() map[string]any {
	rb.mu.Lock()
	running := rb.running
	lastRunAt := rb.lastRunAt
	lastMoves := rb.lastMoves
	lastError := rb.lastError
	totalRuns := rb.totalRuns
	rb.mu.Unlock()

	// Ownership as it stands, which is the thing an examiner actually wants to
	// see when asking whether the cluster is balanced.
	cm := rb.state.GetClusterMap()
	ownership := make(map[string]int)
	if cm.Current != nil {
		for _, a := range cm.Current.Assignments {
			if a.MasterID < 0 {
				continue
			}
			ownership[fmt.Sprintf("node-%d", a.MasterID)]++
		}
	}

	out := map[string]any{
		"running":          running,
		"total_runs":       totalRuns,
		"last_moves":       lastMoves,
		"pending_plan":     rb.Plan(),
		"master_ownership": ownership,
		"version":          rb.state.Version(),
	}
	if !lastRunAt.IsZero() {
		out["last_run_at"] = lastRunAt
	}
	if lastError != "" {
		out["last_error"] = lastError
	}
	return out
}

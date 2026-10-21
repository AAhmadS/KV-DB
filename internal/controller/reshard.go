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

// ReshardOrchestrator drives spec section 13 end to end.
//
// The shape of it, and the reason each step exists:
//
//	13.1  PrepareReshard        compute the new model; two formulas now exist
//	13.2  (intermediate state)  every range starts pending, so the OLD formula
//	                            answers for every key; the load balancer learns
//	                            the range states from /map
//	13.3  per range:
//	          transferring      bulk copy while the old master still takes writes
//	          handoff           old master frozen; nobody may write this range
//	          (final copy)      drain what the bulk copy could not have seen
//	          done              the new master announces readiness itself
//	13.4  CompleteMigration     every range done -> collapse to one formula
//	13.5  notify + finalize     nodes and load balancer drop the old formula
//
// The ordering constraint that makes this correct: a range only becomes done
// after the old master has stopped writing to it AND the new master has said it
// applied everything. Any other order can lose a write that landed between the
// last copy and the switch.
type ReshardOrchestrator struct {
	mu             sync.Mutex
	state          *ClusterState
	failover       *FailoverManager
	client         *http.Client
	migrationState map[int]MigrationStatus
	controllerURL  string
	running        bool
	lastError      string
	startedAt      time.Time
	finishedAt     time.Time
	stopCh         chan struct{}
}

type MigrationStatus struct {
	NodeID    int       `json:"node_id"`
	Status    string    `json:"status"`
	Progress  int       `json:"progress"`
	Error     string    `json:"error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// rangeSpec is the wire form shared with the database nodes.
type rangeSpec struct {
	OldNumPartitions int `json:"old_num_partitions"`
	NewNumPartitions int `json:"new_num_partitions"`
	NewPartitionID   int `json:"new_partition_id"`
}

type rangeMigrateRequest struct {
	rangeSpec
	TargetAddr string `json:"target_addr"`
	Phase      string `json:"phase"`
}

type rangeActivateRequest struct {
	NewPartitionID int      `json:"new_partition_id"`
	Replicas       []string `json:"replicas"`
	ControllerURL  string   `json:"controller_url"`
	NodeID         int      `json:"node_id"`
}

type MigrationResponse struct {
	Status       string `json:"status"`
	KeysMigrated int    `json:"keys_migrated"`
	Error        string `json:"error,omitempty"`
}

func NewReshardOrchestrator(state *ClusterState, failover *FailoverManager) *ReshardOrchestrator {
	return &ReshardOrchestrator{
		state:          state,
		failover:       failover,
		migrationState: make(map[int]MigrationStatus),
		stopCh:         make(chan struct{}),
		client:         newControllerHTTPClient(60 * time.Second),
	}
}

// SetControllerURL records the address nodes should call back on when they
// announce range readiness. Without it the announcement of spec 13.3 has
// nowhere to go, so StartReshard refuses to run.
func (ro *ReshardOrchestrator) SetControllerURL(url string) {
	ro.mu.Lock()
	defer ro.mu.Unlock()
	ro.controllerURL = url
}

func (ro *ReshardOrchestrator) ControllerURL() string {
	ro.mu.Lock()
	defer ro.mu.Unlock()
	return ro.controllerURL
}

// StartReshard enters the intermediate state and kicks off migration.
func (ro *ReshardOrchestrator) StartReshard(newNumPartitions int) error {
	ro.mu.Lock()
	if ro.running {
		ro.mu.Unlock()
		return ErrReshardInProgress
	}
	if ro.controllerURL == "" {
		ro.mu.Unlock()
		return fmt.Errorf("controller callback URL is not configured; cannot run a reshard")
	}
	ro.mu.Unlock()

	oldNumPartitions := ro.state.GetClusterMap().Current.NumPartitions

	if err := ro.state.PrepareReshard(newNumPartitions); err != nil {
		return err
	}

	ro.mu.Lock()
	ro.running = true
	ro.lastError = ""
	ro.startedAt = time.Now()
	ro.finishedAt = time.Time{}
	ro.migrationState = make(map[int]MigrationStatus)
	ro.mu.Unlock()

	log.Printf("[reshard] prepared: %d -> %d partitions, %d ranges to move",
		oldNumPartitions, newNumPartitions, newNumPartitions)

	go ro.orchestrateMigration(oldNumPartitions, newNumPartitions)

	return nil
}

func (ro *ReshardOrchestrator) orchestrateMigration(oldNumPartitions, newNumPartitions int) {
	defer func() {
		ro.mu.Lock()
		ro.running = false
		ro.finishedAt = time.Now()
		ro.mu.Unlock()
	}()

	if err := ro.state.StartMigration(); err != nil {
		log.Printf("[reshard] cannot start migration: %v", err)
		ro.abort(fmt.Sprintf("start migration: %v", err), oldNumPartitions, newNumPartitions)
		return
	}

	// Clear range state left by any previous reshard before traffic starts
	// relying on this one; markers are expressed in the old reshard's partition
	// count and would bounce live requests.
	ro.resetRangeState()

	log.Println("[reshard] intermediate state active: both formulas are live")

	for _, rng := range ro.state.GetRanges() {
		select {
		case <-ro.stopCh:
			ro.abort("cancelled", oldNumPartitions, newNumPartitions)
			return
		default:
		}

		if err := ro.migrateRange(rng, oldNumPartitions, newNumPartitions); err != nil {
			log.Printf("[reshard] range %d failed: %v", rng.NewPartitionID, err)
			ro.state.SetRangeError(rng.NewPartitionID, err.Error())
			ro.abort(fmt.Sprintf("range %d: %v", rng.NewPartitionID, err), oldNumPartitions, newNumPartitions)
			return
		}
	}

	if !ro.state.AllRangesDone() {
		ro.abort("not every range reported done", oldNumPartitions, newNumPartitions)
		return
	}

	// 13.4: every hash range has transferred, so the new formula may be used
	// for every key.
	if err := ro.state.CompleteMigration(); err != nil {
		log.Printf("[reshard] complete migration: %v", err)
		ro.abort(fmt.Sprintf("complete migration: %v", err), oldNumPartitions, newNumPartitions)
		return
	}

	log.Println("[reshard] all ranges transferred; collapsing to the new formula")
	ro.notifyNodesNewTopology(newNumPartitions)

	if err := ro.state.FinalizeReshard(); err != nil {
		log.Printf("[reshard] finalize: %v", err)
		ro.setError(fmt.Sprintf("finalize: %v", err))
		return
	}

	// 13.5: the map is now single-formula. Re-announce roles so replicas of the
	// new table learn about themselves, then let the old owners free the data.
	ro.notifyAllRoles()
	ro.dropStaleCopies(oldNumPartitions, newNumPartitions)
	ro.unfreezeAll()

	log.Printf("[reshard] complete: cluster is now using %d partitions", newNumPartitions)
}

// migrateRange walks one hash range through transferring -> handoff -> done.
func (ro *ReshardOrchestrator) migrateRange(rng *MigrationRange, oldNumPartitions, newNumPartitions int) error {
	cm := ro.state.GetClusterMap()

	if cm.Next == nil || rng.NewPartitionID >= len(cm.Next.Assignments) {
		return fmt.Errorf("no target assignment")
	}
	targetID := cm.Next.Assignments[rng.NewPartitionID].MasterID
	targetNode, ok := cm.Nodes[targetID]
	if !ok || targetNode.Status != NodeStatusHealthy {
		return fmt.Errorf("target node %d is not healthy", targetID)
	}
	targetAddr := fmt.Sprintf("%s:%d", targetNode.Address, targetNode.Port)

	// The distinct old masters holding any part of this range.
	sources := make([]*NodeInfo, 0, len(rng.SourceParts))
	seen := make(map[int]struct{})
	for _, oldPart := range rng.SourceParts {
		if oldPart < 0 || oldPart >= len(cm.Current.Assignments) {
			continue
		}
		masterID := cm.Current.Assignments[oldPart].MasterID
		if masterID < 0 || masterID == targetID {
			continue // already on the target; nothing to ship
		}
		if _, dup := seen[masterID]; dup {
			continue
		}
		src, ok := cm.Nodes[masterID]
		if !ok || src.Status != NodeStatusHealthy {
			return fmt.Errorf("source node %d for old partition %d is not healthy", masterID, oldPart)
		}
		seen[masterID] = struct{}{}
		sources = append(sources, src)
	}

	spec := rangeSpec{
		OldNumPartitions: oldNumPartitions,
		NewNumPartitions: newNumPartitions,
		NewPartitionID:   rng.NewPartitionID,
	}

	// --- bulk copy: old masters still accept writes -------------------------
	if err := ro.state.SetRangeState(rng.NewPartitionID, RangeTransferring); err != nil {
		return err
	}
	for _, src := range sources {
		moved, err := ro.callRangeMigrate(src, rangeMigrateRequest{
			rangeSpec:  spec,
			TargetAddr: targetAddr,
			Phase:      "bulk",
		})
		if err != nil {
			return fmt.Errorf("bulk copy from node %d: %w", src.ID, err)
		}
		ro.state.AddRangeKeys(rng.NewPartitionID, moved)
		ro.recordNodeStatus(src.ID, "bulk", moved, "")
	}

	// --- handoff instant (13.3) ---------------------------------------------
	// Freeze first, then drain. Freezing before the final copy is what makes
	// the final copy complete: no write can land after it starts.
	if err := ro.state.SetRangeState(rng.NewPartitionID, RangeHandoff); err != nil {
		return err
	}
	for _, src := range sources {
		if err := ro.callRangeFreeze(src, spec); err != nil {
			return fmt.Errorf("freeze on node %d: %w", src.ID, err)
		}
	}
	for _, src := range sources {
		moved, err := ro.callRangeMigrate(src, rangeMigrateRequest{
			rangeSpec:  spec,
			TargetAddr: targetAddr,
			Phase:      "final",
		})
		if err != nil {
			return fmt.Errorf("final drain from node %d: %w", src.ID, err)
		}
		ro.state.AddRangeKeys(rng.NewPartitionID, moved)
		ro.recordNodeStatus(src.ID, "final", moved, "")
	}

	// --- the new master announces itself ------------------------------------
	// The node calls /reshard/range-ready, which is what actually flips the
	// range to done. This call returning 200 means that callback has happened.
	if err := ro.callRangeActivate(targetNode, rng.NewPartitionID); err != nil {
		return fmt.Errorf("activate on node %d: %w", targetNode.ID, err)
	}

	if st, ok := ro.state.GetRangeState(rng.NewPartitionID); !ok || st != RangeDone {
		return fmt.Errorf("node %d did not announce readiness (range is %s)", targetNode.ID, st)
	}

	// The old masters must now bounce requests for this range instead of serving
	// their frozen copy. A load balancer whose map is a few seconds stale will
	// still route some keys here by the old formula; answering 409 NOT_MASTER is
	// what makes it refresh and re-route, and it is the difference between a
	// stale read and a slightly slower correct one.
	for _, src := range sources {
		if err := ro.callRangeMoved(src, spec); err != nil {
			log.Printf("[reshard] mark range %d moved on node %d: %v", rng.NewPartitionID, src.ID, err)
		}
	}

	log.Printf("[reshard] range %d -> node %d: done", rng.NewPartitionID, targetNode.ID)
	log.Printf("[RESHARD-MOVE] range=%d targetNode=%d oldPartitions=%v",
		rng.NewPartitionID, targetNode.ID, rng.SourceParts)
	return nil
}

// MarkRangeReady is the controller side of the new master's announcement.
func (ro *ReshardOrchestrator) MarkRangeReady(partitionID, nodeID int) error {
	if err := ro.state.SetRangeState(partitionID, RangeDone); err != nil {
		return err
	}
	ro.recordNodeStatus(nodeID, "ready", 0, "")
	log.Printf("[reshard] node %d announced range %d ready", nodeID, partitionID)
	return nil
}

// abort unwinds a failed reshard: unfreeze everything so the old masters serve
// writes again, then drop back to the old formula. Nothing has been deleted at
// this point, so the old formula is still complete and correct.
func (ro *ReshardOrchestrator) abort(reason string, oldNumPartitions, newNumPartitions int) {
	log.Printf("[reshard] aborting: %s", reason)
	ro.setError(reason)
	// Nothing has been handed over, so both the freezes and any moved markers
	// are wrong now; the old formula is complete and correct on its own.
	ro.resetRangeState()
	ro.state.CancelReshard()
	log.Printf("[reshard] aborted; cluster remains on %d partitions", oldNumPartitions)
}

func (ro *ReshardOrchestrator) setError(msg string) {
	ro.mu.Lock()
	defer ro.mu.Unlock()
	ro.lastError = msg
}

func (ro *ReshardOrchestrator) recordNodeStatus(nodeID int, status string, progress int, errMsg string) {
	ro.mu.Lock()
	defer ro.mu.Unlock()
	prev := ro.migrationState[nodeID]
	ro.migrationState[nodeID] = MigrationStatus{
		NodeID:    nodeID,
		Status:    status,
		Progress:  prev.Progress + progress,
		Error:     errMsg,
		UpdatedAt: time.Now(),
	}
}

// --- node calls -------------------------------------------------------------

func (ro *ReshardOrchestrator) callRangeMigrate(node *NodeInfo, req rangeMigrateRequest) (int, error) {
	var out MigrationResponse
	if err := ro.postJSON(node, "/internal/range/migrate", req, &out); err != nil {
		return 0, err
	}
	if out.Status != "ok" {
		return out.KeysMigrated, fmt.Errorf("node reported: %s", out.Error)
	}
	return out.KeysMigrated, nil
}

func (ro *ReshardOrchestrator) callRangeFreeze(node *NodeInfo, spec rangeSpec) error {
	return ro.postJSON(node, "/internal/range/freeze", spec, nil)
}

func (ro *ReshardOrchestrator) callRangeUnfreezeAll(node *NodeInfo) error {
	return ro.postJSON(node, "/internal/range/unfreeze?all=true", map[string]any{}, nil)
}

func (ro *ReshardOrchestrator) callRangeMoved(node *NodeInfo, spec rangeSpec) error {
	return ro.postJSON(node, "/internal/range/moved", spec, nil)
}

func (ro *ReshardOrchestrator) callRangeDrop(node *NodeInfo, spec rangeSpec) error {
	return ro.postJSON(node, "/internal/range/drop", spec, nil)
}

func (ro *ReshardOrchestrator) callRangeActivate(node *NodeInfo, partitionID int) error {
	return ro.postJSON(node, "/internal/range/activate", rangeActivateRequest{
		NewPartitionID: partitionID,
		Replicas:       nil,
		ControllerURL:  ro.ControllerURL(),
		NodeID:         node.ID,
	}, nil)
}

func (ro *ReshardOrchestrator) postJSON(node *NodeInfo, path string, body any, out any) error {
	return postJSONToNode(ro.client, node, path, body, out)
}

// postJSONToNode POSTs a JSON body to one node's HTTP API. Shared by the
// reshard orchestrator and the rebalancer, which drive the same node endpoints.
func postJSONToNode(client *http.Client, node *NodeInfo, path string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	url := fmt.Sprintf("http://%s:%d%s", node.Address, node.Port, path)

	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", path, err)
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned status %d", path, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode %s response: %w", path, err)
		}
	}
	return nil
}

// notifyNodesNewTopology tells every node the new partition count, which is the
// moment they stop using the old formula for their own hashing (spec 13.4).
func (ro *ReshardOrchestrator) notifyNodesNewTopology(newNumPartitions int) {
	body, err := json.Marshal(map[string]any{
		"action":         "update_topology",
		"num_partitions": newNumPartitions,
		"version":        ro.state.Version(),
	})
	if err != nil {
		log.Printf("[reshard] marshal topology update: %v", err)
		return
	}

	var wg sync.WaitGroup
	for _, node := range ro.state.GetAllNodes() {
		if node.Status != NodeStatusHealthy {
			continue
		}
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()

			url := fmt.Sprintf("http://%s:%d/internal/topology", n.Address, n.Port)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				log.Printf("[reshard] topology request for node %d: %v", n.ID, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := ro.client.Do(req)
			if err != nil {
				log.Printf("[reshard] notify node %d: %v", n.ID, err)
				return
			}
			netutil.DrainAndClose(resp)
			if resp.StatusCode != http.StatusOK {
				log.Printf("[reshard] node %d returned %d for topology update", n.ID, resp.StatusCode)
			}
		}(node)
	}
	wg.Wait()
}

// notifyAllRoles re-announces master and replica roles for the collapsed map,
// and reseeds any replica the collapse introduced.
func (ro *ReshardOrchestrator) notifyAllRoles() {
	if ro.failover == nil {
		return
	}
	cm := ro.state.GetClusterMap()
	for _, a := range cm.Current.Assignments {
		if err := ro.failover.NotifyPartitionRoles(a.PartitionID); err != nil {
			log.Printf("[reshard] notify roles for partition %d: %v", a.PartitionID, err)
			continue
		}
		master, ok := cm.Nodes[a.MasterID]
		if !ok || master.Status != NodeStatusHealthy {
			continue
		}
		for _, rid := range a.ReplicaIDs {
			replica, ok := cm.Nodes[rid]
			if !ok || replica.Status != NodeStatusHealthy {
				continue
			}
			if err := copyPartitionSnapshot(ro.client, a.PartitionID, master, replica); err != nil {
				log.Printf("[reshard] reseed partition %d onto node %d: %v", a.PartitionID, rid, err)
			}
		}
	}
}

// dropStaleCopies lets the previous owners free the data they no longer serve
// (spec 13.4). A node is only asked to drop a range it now holds no role in.
func (ro *ReshardOrchestrator) dropStaleCopies(oldNumPartitions, newNumPartitions int) {
	cm := ro.state.GetClusterMap()

	for _, a := range cm.Current.Assignments {
		holders := make(map[int]struct{}, len(a.ReplicaIDs)+1)
		holders[a.MasterID] = struct{}{}
		for _, rid := range a.ReplicaIDs {
			holders[rid] = struct{}{}
		}

		spec := rangeSpec{
			OldNumPartitions: oldNumPartitions,
			NewNumPartitions: newNumPartitions,
			NewPartitionID:   a.PartitionID,
		}

		for _, node := range cm.Nodes {
			if node.Status != NodeStatusHealthy {
				continue
			}
			if _, holds := holders[node.ID]; holds {
				continue
			}
			if err := ro.callRangeDrop(node, spec); err != nil {
				log.Printf("[reshard] drop range %d on node %d: %v", a.PartitionID, node.ID, err)
			}
		}
	}
}

// resetRangeState clears freezes AND moved markers on every healthy node.
func (ro *ReshardOrchestrator) resetRangeState() {
	for _, node := range ro.state.GetAllNodes() {
		if node.Status != NodeStatusHealthy {
			continue
		}
		if err := ro.postJSON(node, "/internal/range/unfreeze?reset=true", map[string]any{}, nil); err != nil {
			log.Printf("[reshard] reset range state on node %d: %v", node.ID, err)
		}
	}
}

func (ro *ReshardOrchestrator) unfreezeAll() {
	for _, node := range ro.state.GetAllNodes() {
		if node.Status != NodeStatusHealthy {
			continue
		}
		if err := ro.callRangeUnfreezeAll(node); err != nil {
			log.Printf("[reshard] unfreeze node %d: %v", node.ID, err)
		}
	}
}

// GetStatus is what /reshard/status returns and what the admin panel polls.
func (ro *ReshardOrchestrator) GetStatus() map[string]any {
	ro.mu.Lock()
	running := ro.running
	lastErr := ro.lastError
	startedAt := ro.startedAt
	finishedAt := ro.finishedAt
	nodeStatus := make(map[int]MigrationStatus, len(ro.migrationState))
	for k, v := range ro.migrationState {
		nodeStatus[k] = v
	}
	ro.mu.Unlock()

	ranges := ro.state.GetRanges()
	done := 0
	for _, r := range ranges {
		if r.State == RangeDone {
			done++
		}
	}

	out := map[string]any{
		"reshard_state":       ro.state.ReshardState().String(),
		"running":             running,
		"old_num_partitions":  ro.state.OldNumPartitions(),
		"next_num_partitions": ro.state.NextNumPartitions(),
		"ranges_total":        len(ranges),
		"ranges_done":         done,
		"ranges":              ranges,
		"migration_status":    nodeStatus,
		"version":             ro.state.Version(),
	}
	if lastErr != "" {
		out["last_error"] = lastErr
	}
	if !startedAt.IsZero() {
		out["started_at"] = startedAt
	}
	if !finishedAt.IsZero() {
		out["finished_at"] = finishedAt
		if !startedAt.IsZero() {
			out["duration_ms"] = finishedAt.Sub(startedAt).Milliseconds()
		}
	}
	return out
}

// CancelReshard abandons an in-flight reshard and unwinds the freezes.
func (ro *ReshardOrchestrator) CancelReshard() {
	ro.resetRangeState()
	ro.state.CancelReshard()

	ro.mu.Lock()
	ro.migrationState = make(map[int]MigrationStatus)
	ro.lastError = "cancelled by operator"
	ro.mu.Unlock()

	log.Println("[reshard] cancelled by operator")
}

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

type SetReplicationFactorRequest struct {
	ReplicationFactor int `json:"replication_factor"`
}

type Handler struct {
	state      *ClusterState
	failover   *FailoverManager
	reshard    *ReshardOrchestrator
	rebalancer *Rebalancer
	startTime  time.Time
	client     *http.Client
	// rejoinClient is separate from client: a rejoin repair runs in the
	// background and copies whole partitions, so it must not share a timeout
	// with the request-path client.
	rejoinClient *http.Client
}

func NewHandler(state *ClusterState, failover *FailoverManager, reshard *ReshardOrchestrator, rebalancer *Rebalancer) *Handler {
	return &Handler{
		state:        state,
		failover:     failover,
		reshard:      reshard,
		rebalancer:   rebalancer,
		startTime:    time.Now(),
		client:       newControllerHTTPClient(60 * time.Second),
		rejoinClient: newRejoinHTTPClient(),
	}
}

func (h *Handler) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	if req.Address == "" || req.Port == 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "address and port required")
		return
	}

	node, rejoined := h.state.RegisterNode(req.Address, req.Port, req.ReplicationPort)
	// Best-effort: ensure nodes learn their roles after registration.
	go func() {
		// A node that has registered before has restarted, which means it came
		// back with nothing: move its partitions to the replicas that still
		// hold the data, then refill it. Without this the cluster stays green
		// while its keys quietly disappear (spec test scenario 4).
		if rejoined {
			recoverRejoinedNode(h.rejoinClient, h.state, h.failover, node.ID)
		}

		clusterMap := h.state.GetClusterMap()
		for i := range clusterMap.Current.Assignments {
			_ = h.failover.NotifyPartitionRoles(i)
		}
		// A node that just joined owns nothing, because assignment only happens
		// when a partition has no live master. Spec 2.1 makes redistributing on
		// join the controller's job, so do it here rather than leaving the
		// first node to register owning the whole cluster forever.
		if h.rebalancer != nil {
			h.rebalancer.TriggerAsync(fmt.Sprintf("node %d joining", node.ID))
		}
	}()

	h.writeJSON(w, http.StatusOK, RegisterResponse{
		NodeID:        node.ID,
		NumPartitions: h.state.partitions.NumPartitions,
		Message:       "registered successfully",
	})
}

func (h *Handler) HandleNotifyRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	clusterMap := h.state.GetClusterMap()
	for i := range clusterMap.Current.Assignments {
		_ = h.failover.NotifyPartitionRoles(i)
	}

	h.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": h.state.Version()})
}

func (h *Handler) HandleSetReplicationFactor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req SetReplicationFactorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}
	if req.ReplicationFactor < 1 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "replication_factor must be >= 1")
		return
	}

	before := h.state.GetClusterMap()
	prevReplicas := make(map[int]map[int]struct{}, len(before.Current.Assignments))
	for _, a := range before.Current.Assignments {
		set := make(map[int]struct{}, len(a.ReplicaIDs))
		for _, rid := range a.ReplicaIDs {
			set[rid] = struct{}{}
		}
		prevReplicas[a.PartitionID] = set
	}

	h.state.SetReplicationFactor(req.ReplicationFactor)
	after := h.state.GetClusterMap()

	// Announce the new roles BEFORE copying anything.
	//
	// A node refuses a snapshot for a partition it holds no role in
	// (Node.ApplySnapshotWithSequence returns ErrPartitionNotOwned). Pushing
	// first and announcing second - which is what this did - meant every newly
	// added replica rejected its seed, and was then told it was a replica of a
	// partition it had no data for. Raising the factor looked like it worked:
	// the map showed the replicas, and the write stream kept them current from
	// that moment on. Only the keys written BEFORE the change were missing, and
	// nothing reads a replica until its master dies, so the loss did not surface
	// until a failover promoted an empty replica. Measured in scenario 6:
	// 5 of 40 keys gone after stopping one node at replication factor 2.
	for _, a := range after.Current.Assignments {
		if err := h.failover.NotifyPartitionRoles(a.PartitionID); err != nil {
			log.Printf("[replication-factor] partition %d: announcing roles failed: %v", a.PartitionID, err)
		}
	}

	// Now seed the replicas that are new to their partition. Replicas that were
	// already holding it keep the copy they have.
	seeded, failed := 0, 0
	for _, a := range after.Current.Assignments {
		oldSet := prevReplicas[a.PartitionID]
		masterNode, ok := after.Nodes[a.MasterID]
		if !ok || masterNode.Status != NodeStatusHealthy {
			continue
		}
		for _, rid := range a.ReplicaIDs {
			if _, existed := oldSet[rid]; existed {
				continue
			}
			replicaNode, ok := after.Nodes[rid]
			if !ok || replicaNode.Status != NodeStatusHealthy {
				continue
			}
			if err := copyPartitionSnapshot(h.client, a.PartitionID, masterNode, replicaNode); err != nil {
				// Logged, not swallowed. A replica that did not take its seed is
				// an empty replica, and its only symptom is data loss at the next
				// failover - by which time nothing points back here.
				log.Printf("[replication-factor] partition %d: seeding node %d from node %d failed: %v",
					a.PartitionID, rid, a.MasterID, err)
				failed++
				continue
			}
			seeded++
		}
	}
	log.Printf("[replication-factor] factor=%d: %d replicas seeded, %d failed", req.ReplicationFactor, seeded, failed)

	h.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "replication_factor": req.ReplicationFactor, "version": h.state.Version()})
}

func (h *Handler) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	nodeIDStr := r.URL.Query().Get("node_id")
	if nodeIDStr == "" {
		var req HeartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "node_id required")
			return
		}
		nodeIDStr = strconv.Itoa(req.NodeID)
	}

	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid node_id")
		return
	}

	if !h.state.UpdateHeartbeat(nodeID) {
		h.writeError(w, http.StatusNotFound, "NODE_NOT_FOUND", "node not found")
		return
	}

	h.writeJSON(w, http.StatusOK, HeartbeatResponse{
		Acknowledged: true,
		Version:      h.state.Version(),
	})
}

func (h *Handler) HandleMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	clusterMap := h.state.GetClusterMap()

	partitions := make([]PartitionInfo, len(clusterMap.Current.Assignments))
	for i, a := range clusterMap.Current.Assignments {
		masterAddr := ""
		if node, ok := clusterMap.Nodes[a.MasterID]; ok {
			masterAddr = fmt.Sprintf("%s:%d", node.Address, node.Port)
		}

		// Dead replicas are omitted. The load balancer routes off this list and
		// has no health information of its own, so naming a node that has
		// already failed over would send it reads that cannot be served. The
		// assignment itself keeps the replica ID: the node may come back.
		replicas := make([]string, 0, len(a.ReplicaIDs))
		for _, rid := range a.ReplicaIDs {
			node, ok := clusterMap.Nodes[rid]
			if !ok || node.Status == NodeStatusDead {
				continue
			}
			replicas = append(replicas, fmt.Sprintf("%s:%d", node.Address, node.Port))
		}

		partitions[i] = PartitionInfo{
			ID:         a.PartitionID,
			MasterAddr: masterAddr,
			MasterID:   a.MasterID,
			Replicas:   replicas,
		}
	}

	resp := MapResponse{
		Partitions:    partitions,
		NumPartitions: clusterMap.Current.NumPartitions,
		ReshardState:  clusterMap.ReshardState.String(),
		Version:       clusterMap.Version,
	}

	if len(clusterMap.Ranges) > 0 {
		ranges := make([]RangeInfo, 0, len(clusterMap.Ranges))
		for _, r := range clusterMap.Ranges {
			ranges = append(ranges, RangeInfo{
				NewPartitionID: r.NewPartitionID,
				State:          r.State.String(),
				KeysMoved:      r.KeysMoved,
			})
		}
		resp.Ranges = ranges
	}

	if clusterMap.Next != nil {
		nextPartitions := make([]PartitionInfo, len(clusterMap.Next.Assignments))
		for i, a := range clusterMap.Next.Assignments {
			masterAddr := ""
			if node, ok := clusterMap.Nodes[a.MasterID]; ok {
				masterAddr = fmt.Sprintf("%s:%d", node.Address, node.Port)
			}

			replicas := make([]string, 0, len(a.ReplicaIDs))
			for _, rid := range a.ReplicaIDs {
				node, ok := clusterMap.Nodes[rid]
				if !ok || node.Status == NodeStatusDead {
					continue
				}
				replicas = append(replicas, fmt.Sprintf("%s:%d", node.Address, node.Port))
			}

			nextPartitions[i] = PartitionInfo{
				ID:         a.PartitionID,
				MasterAddr: masterAddr,
				MasterID:   a.MasterID,
				Replicas:   replicas,
			}
		}
		resp.NextPartitions = nextPartitions
		resp.NextNumPartitions = clusterMap.Next.NumPartitions
	}

	h.writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	h.writeJSON(w, http.StatusOK, h.state.GetClusterMap())
}

func (h *Handler) HandleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"nodes": h.state.GetAllNodes(),
	})
}

func (h *Handler) HandleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	nodeIDStr := r.URL.Query().Get("node_id")
	if nodeIDStr == "" {
		var req RemoveNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "node_id required")
			return
		}
		nodeIDStr = strconv.Itoa(req.NodeID)
	}

	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil || nodeID <= 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid node_id")
		return
	}

	if _, ok := h.state.GetNode(nodeID); !ok {
		h.writeError(w, http.StatusNotFound, "NODE_NOT_FOUND", "node not found")
		return
	}

	// Treat removal as a failure to promote replicas first, then remove.
	h.state.SetNodeStatus(nodeID, NodeStatusDead)
	h.failover.HandleNodeFailure(nodeID)
	h.state.RemoveNode(nodeID)

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"node_id": nodeID,
		"version": h.state.Version(),
	})
}

func (h *Handler) HandleReshard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req ReshardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	if req.NewNumPartitions <= 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "new_num_partitions must be positive")
		return
	}

	if err := h.reshard.StartReshard(req.NewNumPartitions); err != nil {
		status := http.StatusConflict
		if errors.Is(err, ErrInvalidReshardState) || errors.Is(err, ErrNoHealthyReplica) {
			status = http.StatusBadRequest
		}
		h.writeError(w, status, "RESHARD_ERROR", err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":             "started",
		"new_num_partitions": req.NewNumPartitions,
		"old_num_partitions": h.state.OldNumPartitions(),
		"version":            h.state.Version(),
	})
}

// HandleReshardRangeReady is the endpoint a new master calls to announce that
// it has applied everything the old master sent and is ready to serve the range
// (spec 13.3). Until this arrives the range stays in handoff and the load
// balancer must not use the new formula for it.
func (h *Handler) HandleReshardRangeReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	partitionID, err := strconv.Atoi(r.URL.Query().Get("partition_id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid partition_id")
		return
	}
	nodeID, err := strconv.Atoi(r.URL.Query().Get("node_id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid node_id")
		return
	}

	if err := h.reshard.MarkRangeReady(partitionID, nodeID); err != nil {
		h.writeError(w, http.StatusConflict, "RESHARD_ERROR", err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"partition_id": partitionID,
		"node_id":      nodeID,
		"version":      h.state.Version(),
	})
}

func (h *Handler) HandleReshardStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	h.writeJSON(w, http.StatusOK, h.reshard.GetStatus())
}

func (h *Handler) HandleReshardCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	h.reshard.CancelReshard()

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "cancelled",
		"version": h.state.Version(),
	})
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	nodes := h.state.GetAllNodes()
	healthyCount := 0
	for _, n := range nodes {
		if n.Status == NodeStatusHealthy {
			healthyCount++
		}
	}

	status := "healthy"
	if healthyCount == 0 && len(nodes) > 0 {
		status = "degraded"
	}

	h.writeJSON(w, http.StatusOK, HealthResponse{
		Status:    status,
		Component: "controller",
		Details: map[string]any{
			"uptime_seconds": int(time.Since(h.startTime).Seconds()),
			"total_nodes":    len(nodes),
			"healthy_nodes":  healthyCount,
			"reshard_state":  h.state.ReshardState().String(),
			"version":        h.state.Version(),
		},
	})
}

func (h *Handler) HandleFailover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	partitionIDStr := r.URL.Query().Get("partition_id")
	newMasterIDStr := r.URL.Query().Get("new_master_id")

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid partition_id")
		return
	}

	newMasterID, err := strconv.Atoi(newMasterIDStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid new_master_id")
		return
	}

	if err := h.failover.TriggerManualFailover(partitionID, newMasterID); err != nil {
		h.writeError(w, http.StatusInternalServerError, "FAILOVER_ERROR", err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"partition_id":  partitionID,
		"new_master_id": newMasterID,
	})
}

// HandleRebalance redistributes partition masters across the healthy nodes.
// POST with ?async=true to return immediately; the default runs it inline so a
// caller can wait for the cluster to be balanced before measuring anything.
func (h *Handler) HandleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if h.rebalancer == nil {
		h.writeError(w, http.StatusNotImplemented, "NOT_AVAILABLE", "rebalancer is not configured")
		return
	}

	if r.URL.Query().Get("async") == "true" {
		h.rebalancer.TriggerAsync("manual request")
		h.writeJSON(w, http.StatusAccepted, map[string]any{"status": "started"})
		return
	}

	planned := len(h.rebalancer.Plan())
	moved, err := h.rebalancer.Run()
	if err != nil {
		h.writeJSON(w, http.StatusConflict, map[string]any{
			"status":  "error",
			"planned": planned,
			"moved":   moved,
			"error":   err.Error(),
			"version": h.state.Version(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"planned": planned,
		"moved":   moved,
		"version": h.state.Version(),
	})
}

func (h *Handler) HandleRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if h.rebalancer == nil {
		h.writeError(w, http.StatusNotImplemented, "NOT_AVAILABLE", "rebalancer is not configured")
		return
	}

	h.writeJSON(w, http.StatusOK, h.rebalancer.Status())
}

func (h *Handler) HandleMovePartition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	var req MovePartitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	if req.PartitionID < 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "partition_id must be >= 0")
		return
	}
	if req.TargetNodeID <= 0 {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "target_node_id must be > 0")
		return
	}

	clusterMap := h.state.GetClusterMap()
	if req.PartitionID >= len(clusterMap.Current.Assignments) {
		h.writeError(w, http.StatusNotFound, "PARTITION_NOT_FOUND", "partition not found")
		return
	}

	assignment := clusterMap.Current.Assignments[req.PartitionID]
	currentMasterID := assignment.MasterID
	if currentMasterID < 0 {
		h.writeError(w, http.StatusConflict, "NO_MASTER", "partition has no master")
		return
	}

	// If target is already a replica, this is just a leader move.
	for _, rid := range assignment.ReplicaIDs {
		if rid == req.TargetNodeID {
			if err := h.failover.TriggerManualFailover(req.PartitionID, req.TargetNodeID); err != nil {
				h.writeError(w, http.StatusInternalServerError, "MOVE_ERROR", err.Error())
				return
			}
			h.writeJSON(w, http.StatusOK, map[string]any{
				"status":         "ok",
				"partition_id":   req.PartitionID,
				"target_node_id": req.TargetNodeID,
				"mode":           "promote_replica",
				"version":        h.state.Version(),
			})
			return
		}
	}

	sourceNode, ok := h.state.GetNode(currentMasterID)
	if !ok || sourceNode.Status != NodeStatusHealthy {
		h.writeError(w, http.StatusConflict, "MOVE_ERROR", "current master is not healthy")
		return
	}

	targetNode, ok := h.state.GetNode(req.TargetNodeID)
	if !ok || targetNode.Status != NodeStatusHealthy {
		h.writeError(w, http.StatusBadRequest, "MOVE_ERROR", "target node is not healthy")
		return
	}

	// Copy data for this partition from source master -> target using existing migration API.
	// We set old/new num partitions equal so the partition mapping is identity.
	targetAddr := fmt.Sprintf("%s:%d", targetNode.Address, targetNode.Port)

	migrationTask := map[string]any{
		"source_node_id":     currentMasterID,
		"target_node_addr":   targetAddr,
		"old_partition_id":   req.PartitionID,
		"new_partition_id":   req.PartitionID,
		"old_num_partitions": clusterMap.Current.NumPartitions,
		"new_num_partitions": clusterMap.Current.NumPartitions,
	}

	if err := h.executeNodeMigration(sourceNode, migrationTask); err != nil {
		h.writeError(w, http.StatusInternalServerError, "MOVE_ERROR", err.Error())
		return
	}

	if err := h.state.MovePartition(req.PartitionID, req.TargetNodeID); err != nil {
		h.writeError(w, http.StatusInternalServerError, "MOVE_ERROR", err.Error())
		return
	}

	_ = h.failover.NotifyPartitionRoles(req.PartitionID)

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"partition_id":   req.PartitionID,
		"target_node_id": req.TargetNodeID,
		"mode":           "migrate_then_switch",
		"version":        h.state.Version(),
	})
}

func (h *Handler) executeNodeMigration(sourceNode *NodeInfo, migrationTask map[string]any) error {
	body, err := json.Marshal(migrationTask)
	if err != nil {
		return fmt.Errorf("marshal migration task: %w", err)
	}

	url := fmt.Sprintf("http://%s:%d/internal/migrate", sourceNode.Address, sourceNode.Port)

	ctx, cancel := context.WithTimeout(context.Background(), h.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create migration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("send migration request: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("migration failed with status %d: %s", resp.StatusCode, string(data))
	}

	var parsed struct {
		Status       string `json:"status"`
		KeysMigrated int    `json:"keys_migrated"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return fmt.Errorf("decode migration response: %w", err)
	}
	if parsed.Status != "ok" {
		if parsed.Error != "" {
			return fmt.Errorf("migration error: %s", parsed.Error)
		}
		return fmt.Errorf("migration error")
	}

	return nil
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.writeJSON(w, status, ErrorResponse{
		Error: message,
		Code:  code,
	})
}

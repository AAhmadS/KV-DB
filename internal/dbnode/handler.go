package dbnode

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

type Handler struct {
	node *Node
}

func NewHandler(node *Node) *Handler {
	return &Handler{node: node}
}

func (h *Handler) HandleInternalWAL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	partitionIDStr := r.URL.Query().Get("partition_id")
	if partitionIDStr == "" {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "missing partition_id")
		return
	}
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid partition_id")
		return
	}

	afterSeq := uint64(0)
	if s := r.URL.Query().Get("after_seq"); s != "" {
		v, convErr := strconv.ParseUint(s, 10, 64)
		if convErr != nil {
			h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid after_seq")
			return
		}
		afterSeq = v
	}

	limit := 1000
	if s := r.URL.Query().Get("limit"); s != "" {
		v, convErr := strconv.Atoi(s)
		if convErr != nil {
			h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid limit")
			return
		}
		limit = v
	}

	entries, latest, err := h.node.GetWALEntries(partitionID, afterSeq, limit)
	if err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"partition_id":    partitionID,
		"latest_sequence": latest,
		"entries":         entries,
	})
}

func (h *Handler) HandleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "missing key parameter")
		return
	}

	value, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "failed to read body")
		return
	}

	partitionID, ok := h.partitionParam(w, r, key)
	if !ok {
		return
	}

	if err := h.node.SetInPartition(partitionID, key, value); err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"key":    key,
	})
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "missing key parameter")
		return
	}

	partitionID, ok := h.partitionParam(w, r, key)
	if !ok {
		return
	}

	value, version, err := h.node.GetInPartition(partitionID, key)
	if err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, GetResponse{
		Key:     key,
		Value:   value,
		Version: version,
	})
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "missing key parameter")
		return
	}

	partitionID, ok := h.partitionParam(w, r, key)
	if !ok {
		return
	}

	if err := h.node.DeleteInPartition(partitionID, key); err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"key":    key,
	})
}

// partitionParam resolves which partition a data request belongs to.
//
// The load balancer sends ?partition=N because during a reshard it, not the
// node, is the component that knows which of the two live formulas applies to
// this key (spec 13.2). When the parameter is absent the node falls back to
// its own formula, which keeps direct curl-against-a-node usable.
func (h *Handler) partitionParam(w http.ResponseWriter, r *http.Request, key string) (int, bool) {
	// A range that has already been handed to a new master is not ours, whatever
	// the caller believes. Bouncing it with NOT_MASTER is what makes a stale
	// load balancer refresh its map (spec 13.3).
	if h.node.IsRangeMovedForKey(key) {
		h.writeError(w, http.StatusConflict, CodeNotMaster, "range has moved to a new master")
		return 0, false
	}

	raw := r.URL.Query().Get("partition")
	if raw == "" {
		return h.node.partitionForKey(key), true
	}

	partitionID, err := strconv.Atoi(raw)
	if err != nil || partitionID < 0 {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid partition")
		return 0, false
	}
	return partitionID, true
}

func (h *Handler) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var req ReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	if err := h.node.ApplyReplication(&req); err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"sequence": req.Sequence,
	})
}

func (h *Handler) HandleSnapshot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleSnapshotGet(w, r)
	case http.MethodPost:
		h.handleSnapshotPost(w, r)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
	}
}

func (h *Handler) handleSnapshotGet(w http.ResponseWriter, r *http.Request) {
	partitionIDStr := r.URL.Query().Get("partition_id")
	var (
		data []byte
		err  error
	)
	if partitionIDStr != "" {
		partitionID, convErr := strconv.Atoi(partitionIDStr)
		if convErr != nil {
			h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid partition_id")
			return
		}
		data, err = h.node.CreatePartitionSnapshot(partitionID)
		w.Header().Set("X-Partition-WAL-Seq", strconv.FormatUint(h.node.partitionWALSequence(partitionID), 10))
	} else {
		data, err = h.node.CreateSnapshot()
	}
	if err != nil {
		h.handleNodeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handler) handleSnapshotPost(w http.ResponseWriter, r *http.Request) {
	partitionIDStr := r.URL.Query().Get("partition_id")
	if partitionIDStr == "" {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "missing partition_id")
		return
	}

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid partition_id")
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "failed to read body")
		return
	}

	walSeq := uint64(0)
	if s := r.URL.Query().Get("wal_seq"); s != "" {
		v, convErr := strconv.ParseUint(s, 10, 64)
		if convErr != nil {
			h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid wal_seq")
			return
		}
		walSeq = v
	}

	if err := h.node.ApplySnapshotWithSequence(partitionID, data, walSeq); err != nil {
		h.handleNodeError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"partition_id": partitionID,
	})
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	status := h.node.HealthStatus()
	httpStatus := http.StatusOK
	if status != "healthy" {
		httpStatus = http.StatusServiceUnavailable
	}

	h.writeJSON(w, httpStatus, HealthResponse{
		Status:    status,
		Component: "dbnode",
		NodeID:    h.node.ID(),
		Role:      "node",
		Details:   h.node.Stats(),
	})
}

func (h *Handler) HandleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var task MigrationTask
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	result, err := h.node.Migrator().ExecuteMigration(task)
	if err != nil {
		h.writeJSON(w, http.StatusInternalServerError, MigrationResult{
			Status: "error",
			Error:  err.Error(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, result)
}

func (h *Handler) HandleReceiveMigration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var batch KeyBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	if err := h.node.Migrator().ReceiveMigration(batch); err != nil {
		h.writeError(w, http.StatusInternalServerError, CodeInternalError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"keys_received": len(batch.Keys),
	})
}

func (h *Handler) HandleTopologyUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var update struct {
		Action        string `json:"action"`
		NumPartitions int    `json:"num_partitions"`
		Version       uint64 `json:"version"`
	}

	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	if update.Action == "update_topology" && update.NumPartitions > 0 {
		h.node.UpdateNumPartitions(update.NumPartitions)
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": update.Version,
	})
}

func (h *Handler) HandleRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var roleUpdate struct {
		PartitionID int      `json:"partition_id"`
		IsMaster    bool     `json:"is_master"`
		Replicas    []string `json:"replicas"`
		// Drop means this node holds no role in the partition any more. The
		// controller sends it to every node that is neither master nor replica,
		// so a demoted master or a previous owner after a reshard cannot keep
		// claiming a partition it no longer serves.
		Drop bool `json:"drop"`
	}

	if err := json.NewDecoder(r.Body).Decode(&roleUpdate); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	if roleUpdate.Drop {
		h.node.RemovePartition(roleUpdate.PartitionID)
		h.writeJSON(w, http.StatusOK, map[string]any{
			"status":       "ok",
			"partition_id": roleUpdate.PartitionID,
			"role":         "none",
		})
		return
	}

	role := RoleReplica
	if roleUpdate.IsMaster {
		role = RoleMaster
	}

	h.node.SetPartitionRole(roleUpdate.PartitionID, role, roleUpdate.Replicas)

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"partition_id": roleUpdate.PartitionID,
		"role":         role.String(),
	})
}

func (h *Handler) handleNodeError(w http.ResponseWriter, err error) {
	switch err {
	case ErrNotMaster:
		h.writeError(w, http.StatusConflict, CodeNotMaster, err.Error())
	case ErrPartitionNotOwned:
		h.writeError(w, http.StatusMisdirectedRequest, CodePartitionNotOwned, err.Error())
	case ErrKeyNotFound:
		h.writeError(w, http.StatusNotFound, CodeKeyNotFound, err.Error())
	case ErrRangeFrozen:
		// 503: the correct answer is "ask again shortly", and both the load
		// balancer and the client already treat 503 as retryable.
		h.writeError(w, http.StatusServiceUnavailable, CodeRangeFrozen, err.Error())
	default:
		h.writeError(w, http.StatusInternalServerError, CodeInternalError, err.Error())
	}
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code string, message string) {
	h.writeJSON(w, status, ErrorResponse{
		Error: message,
		Code:  code,
	})
}

// HandleDie is a testing endpoint that causes the node to exit.
// This is used for integration testing of failover scenarios.
func (h *Handler) HandleDie(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	log.Printf("[node %d] received /die request, shutting down...", h.node.ID())

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "dying",
		"node_id": h.node.ID(),
	})

	// Exit the process after sending response
	go func() {
		os.Exit(0)
	}()
}

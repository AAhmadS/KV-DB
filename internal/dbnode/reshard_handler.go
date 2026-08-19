package dbnode

import (
	"encoding/json"
	"net/http"
)

// HTTP surface for the node's half of spec section 13. Every one of these is
// driven by the controller's reshard orchestrator; none is reachable from the
// client data path.

// HandleRangeMigrate ships one hash range to the target node.
func (h *Handler) HandleRangeMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var req RangeMigrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	moved, err := h.node.MigrateRange(r.Context(), req)
	if err != nil {
		h.writeJSON(w, http.StatusInternalServerError, MigrationResult{
			Status:       "error",
			KeysMigrated: moved,
			Error:        err.Error(),
		})
		return
	}

	h.writeJSON(w, http.StatusOK, MigrationResult{
		Status:       "ok",
		KeysMigrated: moved,
	})
}

// HandleRangeFreeze opens the handoff window (spec 13.3): this node stops
// answering writes for the named hash range.
func (h *Handler) HandleRangeFreeze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var spec RangeSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	h.node.FreezeRange(spec)
	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":           "frozen",
		"new_partition_id": spec.NewPartitionID,
		"frozen_ranges":    h.node.FrozenRangeCount(),
	})
}

// HandleRangeUnfreeze lifts a freeze. With no body it lifts every freeze,
// which is how an abandoned reshard is unwound.
func (h *Handler) HandleRangeUnfreeze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	if r.URL.Query().Get("reset") == "true" {
		h.node.ResetRangeState()
		h.writeJSON(w, http.StatusOK, map[string]any{
			"status":        "reset",
			"frozen_ranges": h.node.FrozenRangeCount(),
		})
		return
	}

	if r.URL.Query().Get("all") == "true" {
		h.node.UnfreezeAll()
		h.writeJSON(w, http.StatusOK, map[string]any{
			"status":        "unfrozen",
			"frozen_ranges": h.node.FrozenRangeCount(),
		})
		return
	}

	var spec RangeSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	h.node.UnfreezeRange(spec)
	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":           "unfrozen",
		"new_partition_id": spec.NewPartitionID,
		"frozen_ranges":    h.node.FrozenRangeCount(),
	})
}

// HandleRangeActivate makes this node the master of a new partition and has it
// announce readiness to the controller. The announcement is synchronous, so a
// 200 here means the controller has recorded the range as done.
func (h *Handler) HandleRangeActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var req RangeActivateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	if err := h.node.ActivateRange(r.Context(), req); err != nil {
		h.writeError(w, http.StatusInternalServerError, CodeInternalError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":           "active",
		"new_partition_id": req.NewPartitionID,
		"node_id":          h.node.ID(),
	})
}

// HandleRangeDrop frees this node's now-unowned copy of a range (spec 13.4).
func (h *Handler) HandleRangeDrop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var spec RangeSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	dropped, err := h.node.DropRange(spec)
	if err != nil {
		h.writeError(w, http.StatusConflict, CodeInternalError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"keys_dropped": dropped,
	})
}

// HandleRangeMoved tells this node that a range it used to master now lives
// elsewhere, so it must stop answering for those keys entirely (spec 13.3).
func (h *Handler) HandleRangeMoved(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}

	var spec RangeSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		h.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "invalid JSON body")
		return
	}

	h.node.MarkRangeMoved(spec)
	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":           "moved",
		"new_partition_id": spec.NewPartitionID,
	})
}

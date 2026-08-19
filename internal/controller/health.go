package controller

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"
)

type HealthChecker struct {
	state          *ClusterState
	failover       *FailoverManager
	client         *http.Client
	interval       time.Duration
	suspectTimeout time.Duration
	deadTimeout    time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

type HealthCheckerConfig struct {
	Interval       time.Duration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	RequestTimeout time.Duration
}

func DefaultHealthCheckerConfig() HealthCheckerConfig {
	return HealthCheckerConfig{
		Interval:       5 * time.Second,
		SuspectTimeout: 10 * time.Second,
		DeadTimeout:    30 * time.Second,
		RequestTimeout: 3 * time.Second,
	}
}

func NewHealthChecker(state *ClusterState, failover *FailoverManager, cfg HealthCheckerConfig) *HealthChecker {
	return &HealthChecker{
		state:          state,
		failover:       failover,
		interval:       cfg.Interval,
		suspectTimeout: cfg.SuspectTimeout,
		deadTimeout:    cfg.DeadTimeout,
		stopCh:         make(chan struct{}),
		client:         newControllerHTTPClient(cfg.RequestTimeout),
	}
}

func (h *HealthChecker) Start() {
	h.wg.Add(1)
	go h.run()
}

func (h *HealthChecker) Stop() {
	close(h.stopCh)
	h.wg.Wait()
}

func (h *HealthChecker) run() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.checkAllNodes()
		}
	}
}

func (h *HealthChecker) checkAllNodes() {
	nodes := h.state.GetAllNodes()

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()
			h.checkNode(n)
		}(node)
	}
	wg.Wait()
}

func (h *HealthChecker) checkNode(node *NodeInfo) {
	url := fmt.Sprintf("http://%s:%d/health", node.Address, node.Port)

	ctx, cancel := context.WithTimeout(context.Background(), h.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		h.handleUnhealthy(node)
		return
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.handleUnhealthy(node)
		return
	}
	defer netutil.DrainAndClose(resp)

	if resp.StatusCode == http.StatusOK {
		recovered := node.Status != NodeStatusHealthy
		h.state.UpdateHeartbeat(node.ID)
		if recovered {
			log.Printf("[health] node %d recovered", node.ID)
			h.state.SetNodeStatus(node.ID, NodeStatusHealthy)
			go h.reseedRecoveredNode(node.ID)
		}
		return
	}

	h.handleUnhealthy(node)
}

func (h *HealthChecker) reseedRecoveredNode(nodeID int) {
	clusterMap := h.state.GetClusterMap()
	recoveredNode, ok := clusterMap.Nodes[nodeID]
	if !ok {
		return
	}

	for _, assignment := range clusterMap.Current.Assignments {
		if assignment.MasterID < 0 {
			continue
		}
		if assignment.MasterID == nodeID {
			// Spec intent: recovered nodes should come back as replicas, not masters.
			continue
		}

		isReplica := false
		for _, rid := range assignment.ReplicaIDs {
			if rid == nodeID {
				isReplica = true
				break
			}
		}
		if !isReplica {
			continue
		}

		masterNode, ok := clusterMap.Nodes[assignment.MasterID]
		if !ok || masterNode.Status != NodeStatusHealthy {
			continue
		}

		// Ensure the recovered node re-learns its replica role and replication targets.
		_ = h.failover.NotifyPartitionRoles(assignment.PartitionID)

		if err := copyPartitionSnapshot(h.client, assignment.PartitionID, masterNode, recoveredNode); err != nil {
			log.Printf("[health] node %d reseed failed for partition %d: %v", nodeID, assignment.PartitionID, err)
			continue
		}
		log.Printf("[health] node %d reseeded for partition %d", nodeID, assignment.PartitionID)
	}
}

func (h *HealthChecker) handleUnhealthy(node *NodeInfo) {
	timeSinceHeartbeat := time.Since(node.LastHeartbeat)

	switch {
	case timeSinceHeartbeat >= h.deadTimeout:
		if node.Status != NodeStatusDead {
			log.Printf("[health] node %d marked dead (no heartbeat for %v)", node.ID, timeSinceHeartbeat)
			h.state.SetNodeStatus(node.ID, NodeStatusDead)
			h.failover.HandleNodeFailure(node.ID)
		}

	case timeSinceHeartbeat >= h.suspectTimeout:
		if node.Status == NodeStatusHealthy {
			log.Printf("[health] node %d marked suspect (no heartbeat for %v)", node.ID, timeSinceHeartbeat)
			h.state.SetNodeStatus(node.ID, NodeStatusSuspect)
		}

	default:
		log.Printf("[health] node %d health check failed, but within timeout", node.ID)
	}
}

func (h *HealthChecker) CheckNodeNow(nodeID int) error {
	node, ok := h.state.GetNode(nodeID)
	if !ok {
		return ErrNodeNotFound
	}

	h.checkNode(node)
	return nil
}

//go:build reshard_probe
// +build reshard_probe

package test

import (
	"testing"
	"time"

	"github.com/kvdb/kvdb/internal/controller"
)

// Probe: does ClusterState.PrepareReshard return at all?
func TestProbe_PrepareReshard(t *testing.T) {
	cs := controller.NewClusterState(4, 1)
	cs.RegisterNode("127.0.0.1", 9101, 9101)
	cs.RegisterNode("127.0.0.1", 9102, 9102)

	done := make(chan error, 1)
	go func() {
		done <- cs.PrepareReshard(8)
	}()

	select {
	case err := <-done:
		t.Logf("PrepareReshard returned: err=%v state=%s", err, cs.ReshardState())
	case <-time.After(3 * time.Second):
		t.Fatalf("PrepareReshard DID NOT RETURN within 3s (deadlock)")
	}
}

//go:build integration
// +build integration

package test

import (
	context "context"
	nethttp "net/http"
	time "time"
)

func waitForClusterHealth(ctx context.Context, lbURL string) error {
	healthURL := lbURL + "/health"

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := nethttp.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == nethttp.StatusOK {
				return nil
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

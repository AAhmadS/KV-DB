package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kvdb/kvdb/internal/dbnode"
)

func main() {
	cfg := loadConfig()

	node, err := dbnode.NewNode(cfg)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	serverCfg := dbnode.DefaultServerConfig(cfg.Port)
	serverCfg.ReplicationPort = cfg.ReplicationPort
	server := dbnode.NewServer(node, serverCfg)

	if err := server.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	log.Printf("dbnode %d started on port %d", cfg.ID, cfg.Port)

	// Join the cluster. Until this happens the controller does not know the
	// node exists, so it assigns it no partitions and the node answers 421 to
	// everything the load balancer sends it. Registration is what turns a
	// running process into a cluster member.
	regCtx, regCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := registerWithController(regCtx, node, cfg, advertiseAddress()); err != nil {
		regCancel()
		log.Printf("failed to register with controller: %v", err)
		_ = server.Stop()
		os.Exit(1)
	}
	regCancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("received shutdown signal")
	if err := server.Stop(); err != nil {
		log.Printf("error during shutdown: %v", err)
	}
}

// advertiseAddress is the host other cluster members should use to reach this
// node. Under Docker Compose that is the service name, which is also the
// container hostname, so the hostname is the right default.
func advertiseAddress() string {
	if v := os.Getenv("NODE_ADVERTISE_ADDR"); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "127.0.0.1"
}

// registerWithController POSTs /register until the controller accepts, because
// the controller may still be starting. It gives up when ctx expires rather
// than retrying forever, so a misconfigured URL fails visibly instead of
// leaving a process that looks healthy and is in no cluster.
func registerWithController(ctx context.Context, node *dbnode.Node, cfg dbnode.NodeConfig, address string) error {
	body, err := json.Marshal(map[string]any{
		"address":          address,
		"port":             cfg.Port,
		"replication_port": cfg.ReplicationPort,
	})
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	url := cfg.ControllerURL + "/register"

	backoff := 500 * time.Millisecond
	var lastErr error

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("gave up registering at %s after %d attempts: %w (last error: %v)",
				url, attempt-1, err, lastErr)
		}

		lastErr = tryRegister(ctx, client, url, body, address, node, cfg)
		if lastErr == nil {
			return nil
		}

		log.Printf("[register] attempt %d failed: %v (retrying in %s)", attempt, lastErr, backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up registering at %s: %w (last error: %v)", url, ctx.Err(), lastErr)
		case <-time.After(backoff):
		}

		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func tryRegister(ctx context.Context, client *http.Client, url string, body []byte, address string, node *dbnode.Node, cfg dbnode.NodeConfig) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("controller returned %d: %s", resp.StatusCode, string(payload))
	}

	var out struct {
		NodeID        int    `json:"node_id"`
		NumPartitions int    `json:"num_partitions"`
		Message       string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode register response: %w", err)
	}

	// Take the partition count from the controller rather than from this
	// process's own environment. They are two copies of one number, and a
	// disagreement between them puts every key in the wrong partition.
	if out.NumPartitions > 0 && out.NumPartitions != node.NumPartitions() {
		log.Printf("[register] adopting controller's partition count: %d (local config said %d)",
			out.NumPartitions, node.NumPartitions())
		node.UpdateNumPartitions(out.NumPartitions)
	}

	log.Printf("[register] joined as node %d at %s:%d (replication %d), cluster has %d partitions",
		out.NodeID, address, cfg.Port, cfg.ReplicationPort, out.NumPartitions)
	return nil
}

func loadConfig() dbnode.NodeConfig {
	cfg := dbnode.DefaultNodeConfig()

	if v := os.Getenv("NODE_ID"); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			cfg.ID = id
		}
	}

	if v := os.Getenv("NODE_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Port = port
		}
	}

	if v := os.Getenv("NODE_REPLICATION_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.ReplicationPort = port
		}
	}

	if v := os.Getenv("NODE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}

	if v := os.Getenv("NODE_CONTROLLER_URL"); v != "" {
		cfg.ControllerURL = v
	}

	if v := os.Getenv("NODE_WAL_SYNC_MODE"); v != "" {
		cfg.WALSyncMode = v
	}

	if v := os.Getenv("NODE_SNAPSHOT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SnapshotInterval = d
		}
	}

	if v := os.Getenv("NODE_NUM_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NumPartitions = n
		}
	}

	return cfg
}

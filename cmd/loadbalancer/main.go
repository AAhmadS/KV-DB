package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kvdb/kvdb/internal/loadbalancer"
)

func main() {
	cfg := loadConfig()

	server := loadbalancer.NewServer(cfg)

	if err := server.Start(); err != nil {
		log.Fatalf("failed to start load balancer: %v", err)
	}

	log.Printf("loadbalancer started on port %d", cfg.Port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("received shutdown signal")
	if err := server.Stop(); err != nil {
		log.Printf("error during shutdown: %v", err)
	}
}

func loadConfig() loadbalancer.ServerConfig {
	cfg := loadbalancer.DefaultServerConfig()

	if v := os.Getenv("LB_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Port = port
		}
	}

	if v := os.Getenv("LB_CONTROLLER_URL"); v != "" {
		cfg.ControllerURL = v
	}

	if v := os.Getenv("LB_TOPOLOGY_REFRESH"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.TopologyRefresh = d
		}
	}

	if v := os.Getenv("LB_REQUEST_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.RequestTimeout = d
		}
	}

	if v := os.Getenv("LB_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxRetries = n
		}
	}

	// Spec 2.3 allows reads from either the master or a replica. Off by default
	// so a Get always sees the latest acknowledged Set; on, it spreads reads.
	if v := os.Getenv("LB_READ_FROM_REPLICAS"); v == "true" || v == "1" {
		cfg.ReadFromReplicas = true
	}

	return cfg
}

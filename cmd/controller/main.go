package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kvdb/kvdb/internal/controller"
)

func main() {
	cfg := loadConfig()

	server := controller.NewServer(cfg)

	if err := server.Start(); err != nil {
		log.Fatalf("failed to start controller: %v", err)
	}

	log.Printf("controller started on port %d", cfg.Port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("received shutdown signal")
	if err := server.Stop(); err != nil {
		log.Printf("error during shutdown: %v", err)
	}
}

func loadConfig() controller.ServerConfig {
	cfg := controller.DefaultServerConfig()

	if v := os.Getenv("CONTROLLER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Port = port
		}
	}

	// Database nodes call this URL back to announce range readiness during a
	// reshard. The default (127.0.0.1) is right when everything shares a host
	// and wrong inside Docker, where it must be the service name.
	if v := os.Getenv("CONTROLLER_ADVERTISE_URL"); v != "" {
		cfg.AdvertiseURL = v
	}

	if v := os.Getenv("CONTROLLER_NUM_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NumPartitions = n
		}
	}

	if v := os.Getenv("CONTROLLER_REPLICATION_FACTOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ReplicationFactor = n
		}
	}

	if v := os.Getenv("CONTROLLER_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HealthInterval = d
		}
	}

	if v := os.Getenv("CONTROLLER_HEARTBEAT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.DeadTimeout = d
			cfg.SuspectTimeout = d / 2
		}
	}

	return cfg
}

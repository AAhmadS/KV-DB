package controller

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type Server struct {
	config        ServerConfig
	state         *ClusterState
	healthChecker *HealthChecker
	failover      *FailoverManager
	reshard       *ReshardOrchestrator
	rebalancer    *Rebalancer
	handler       *Handler
	httpServer    *http.Server
}

type ServerConfig struct {
	Port              int
	NumPartitions     int
	ReplicationFactor int
	// AdvertiseURL is the address database nodes should call back on when they
	// announce range readiness during a reshard (spec 13.3). Empty means
	// "http://127.0.0.1:<Port>", which is right for the in-process test
	// harness and wrong under Docker, where it must be the service name.
	AdvertiseURL    string
	HealthInterval  time.Duration
	SuspectTimeout  time.Duration
	DeadTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Port:              8000,
		NumPartitions:     16,
		ReplicationFactor: 2,
		HealthInterval:    5 * time.Second,
		SuspectTimeout:    10 * time.Second,
		DeadTimeout:       30 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ShutdownTimeout:   30 * time.Second,
	}
}

func NewServer(cfg ServerConfig) *Server {
	state := NewClusterState(cfg.NumPartitions, cfg.ReplicationFactor)
	failover := NewFailoverManager(state)
	reshard := NewReshardOrchestrator(state, failover)
	rebalancer := NewRebalancer(state, failover)

	healthCfg := HealthCheckerConfig{
		Interval:       cfg.HealthInterval,
		SuspectTimeout: cfg.SuspectTimeout,
		DeadTimeout:    cfg.DeadTimeout,
		RequestTimeout: 3 * time.Second,
	}
	healthChecker := NewHealthChecker(state, failover, healthCfg)

	handler := NewHandler(state, failover, reshard, rebalancer)

	adminHandler := NewAdminHandler(state)

	mux := http.NewServeMux()

	// Admin panel routes
	mux.Handle("/admin", adminHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/admin", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	// API routes
	mux.HandleFunc("/register", handler.HandleRegister)
	mux.HandleFunc("/heartbeat", handler.HandleHeartbeat)
	mux.HandleFunc("/map", handler.HandleMap)
	mux.HandleFunc("/topology", handler.HandleTopology)
	mux.HandleFunc("/nodes", handler.HandleNodes)
	mux.HandleFunc("/nodes/remove", handler.HandleRemoveNode)
	mux.HandleFunc("/internal/notify-roles", handler.HandleNotifyRoles)
	mux.HandleFunc("/replication/factor", handler.HandleSetReplicationFactor)
	mux.HandleFunc("/reshard", handler.HandleReshard)
	mux.HandleFunc("/reshard/status", handler.HandleReshardStatus)
	mux.HandleFunc("/reshard/range-ready", handler.HandleReshardRangeReady)
	mux.HandleFunc("/reshard/cancel", handler.HandleReshardCancel)
	mux.HandleFunc("/rebalance", handler.HandleRebalance)
	mux.HandleFunc("/rebalance/status", handler.HandleRebalanceStatus)
	mux.HandleFunc("/failover", handler.HandleFailover)
	mux.HandleFunc("/partition/move", handler.HandleMovePartition)
	mux.HandleFunc("/health", handler.HandleHealth)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      logMiddleware(mux),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &Server{
		config:        cfg,
		state:         state,
		healthChecker: healthChecker,
		failover:      failover,
		reshard:       reshard,
		rebalancer:    rebalancer,
		handler:       handler,
		httpServer:    httpServer,
	}
}

func (s *Server) Start() error {
	s.reshard.SetControllerURL(s.advertiseURL())
	s.healthChecker.Start()

	log.Printf("[controller] starting on port %d", s.config.Port)
	log.Printf("[controller] partitions=%d replication_factor=%d",
		s.config.NumPartitions, s.config.ReplicationFactor)

	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		s.healthChecker.Stop()
		return fmt.Errorf("listen %s: %w", s.httpServer.Addr, err)
	}

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[controller] listen error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	log.Println("[controller] shutting down...")

	s.healthChecker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}

	log.Println("[controller] stopped")
	return nil
}

func (s *Server) ListenAndServe() error {
	s.reshard.SetControllerURL(s.advertiseURL())
	s.healthChecker.Start()

	log.Printf("[controller] listening on port %d", s.config.Port)
	return s.httpServer.ListenAndServe()
}

func (s *Server) State() *ClusterState {
	return s.state
}

func (s *Server) advertiseURL() string {
	if s.config.AdvertiseURL != "" {
		return s.config.AdvertiseURL
	}
	return fmt.Sprintf("http://127.0.0.1:%d", s.config.Port)
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		log.Printf("[http] %s %s %d %v",
			r.Method,
			r.URL.Path,
			wrapped.status,
			time.Since(start),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

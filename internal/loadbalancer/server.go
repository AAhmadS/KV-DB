package loadbalancer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type Server struct {
	config    ServerConfig
	topology  *TopologyCache
	router    *Router
	http      *http.Server
	startTime time.Time
}

type ServerConfig struct {
	Port            int
	ControllerURL   string
	TopologyRefresh time.Duration
	RequestTimeout  time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	MaxRetries      int
	// ReadFromReplicas lets Get be served by a replica as well as the master
	// (spec 2.3). Off by default, because it trades read-your-writes for read
	// throughput.
	ReadFromReplicas bool
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Port:             8080,
		ControllerURL:    "http://controller:8000",
		TopologyRefresh:  5 * time.Second,
		RequestTimeout:   10 * time.Second,
		ReadTimeout:      15 * time.Second,
		WriteTimeout:     15 * time.Second,
		ShutdownTimeout:  30 * time.Second,
		MaxRetries:       2,
		ReadFromReplicas: false,
	}
}

func NewServer(cfg ServerConfig) *Server {
	topologyCfg := TopologyCacheConfig{
		ControllerURL:   cfg.ControllerURL,
		RefreshInterval: cfg.TopologyRefresh,
		RequestTimeout:  3 * time.Second,
	}
	topology := NewTopologyCache(topologyCfg)

	routerCfg := RouterConfig{
		RequestTimeout:   cfg.RequestTimeout,
		MaxRetries:       cfg.MaxRetries,
		ReadFromReplicas: cfg.ReadFromReplicas,
	}
	router := NewRouter(topology, routerCfg)

	s := &Server{
		config:    cfg,
		topology:  topology,
		router:    router,
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/topology", s.handleTopology)

	s.http = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      logMiddleware(mux),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s
}

// Router exposes the routing layer so a caller can install a re-shard routing
// decider (see internal/loadbalancer/reshard_route.go).
func (s *Server) Router() *Router {
	return s.router
}

func (s *Server) Start() error {
	if err := s.topology.Start(); err != nil {
		return fmt.Errorf("start topology cache: %w", err)
	}

	log.Printf("[loadbalancer] starting on port %d", s.config.Port)
	log.Printf("[loadbalancer] controller=%s refresh=%v",
		s.config.ControllerURL, s.config.TopologyRefresh)

	// Logged because it changes the consistency the caller gets, and because a
	// measurement that compares the two strategies needs the log to show which
	// one was actually running.
	if s.config.ReadFromReplicas {
		log.Printf("[loadbalancer] read strategy: master and replicas (higher read throughput, no read-your-writes)")
	} else {
		log.Printf("[loadbalancer] read strategy: master only (a Get always sees the latest acknowledged Set)")
	}

	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		s.topology.Stop()
		return fmt.Errorf("listen %s: %w", s.http.Addr, err)
	}

	go func() {
		if err := s.http.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[loadbalancer] listen error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	log.Println("[loadbalancer] shutting down...")

	s.topology.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.http.Shutdown(ctx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}

	log.Println("[loadbalancer] stopped")
	return nil
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "KEY_REQUIRED", "key is required in path")
		return
	}

	ctx := r.Context()

	switch r.Method {
	case http.MethodGet:
		s.handleGet(ctx, w, key)
	case http.MethodPut, http.MethodPost:
		s.handleSet(ctx, w, r, key)
	case http.MethodDelete:
		s.handleDelete(ctx, w, key)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (s *Server) handleGet(ctx context.Context, w http.ResponseWriter, key string) {
	resp, err := s.router.RouteGet(ctx, key)
	if err != nil {
		log.Printf("[loadbalancer] GET %s failed: %v", key, err)
		s.writeRoutingError(w, err)
		return
	}

	s.router.ProxyRequest(w, resp)
}

func (s *Server) handleSet(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "INVALID_BODY", "failed to read body")
		return
	}

	resp, err := s.router.RouteSet(ctx, key, value)
	if err != nil {
		log.Printf("[loadbalancer] SET %s failed: %v", key, err)
		s.writeRoutingError(w, err)
		return
	}

	s.router.ProxyRequest(w, resp)
}

func (s *Server) handleDelete(ctx context.Context, w http.ResponseWriter, key string) {
	resp, err := s.router.RouteDelete(ctx, key)
	if err != nil {
		log.Printf("[loadbalancer] DELETE %s failed: %v", key, err)
		s.writeRoutingError(w, err)
		return
	}

	s.router.ProxyRequest(w, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	clusterMap := s.topology.GetClusterMap()
	status := "healthy"
	if clusterMap == nil {
		status = "degraded"
	}

	s.writeJSON(w, http.StatusOK, HealthResponse{
		Status:    status,
		Component: "loadbalancer",
		Details: map[string]any{
			"uptime_seconds":   int(time.Since(s.startTime).Seconds()),
			"topology_version": s.topology.Version(),
			"reshard_state":    s.topology.ReshardState(),
			"num_partitions":   s.topology.NumPartitions(),
		},
	})
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	clusterMap := s.topology.GetClusterMap()
	if clusterMap == nil {
		s.writeError(w, http.StatusServiceUnavailable, "NO_TOPOLOGY", "topology not available")
		return
	}

	s.writeJSON(w, http.StatusOK, clusterMap)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeRoutingError turns a routing failure into the status the client SDK
// should act on. A range mid-handoff is a "try again in a moment", not a
// gateway failure: the request was refused deliberately and will succeed once
// the new master announces readiness (spec 13.3).
func (s *Server) writeRoutingError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRangeInHandoff):
		s.writeError(w, http.StatusServiceUnavailable, "RANGE_IN_HANDOFF", err.Error())
	case errors.Is(err, ErrNoHealthyNode), errors.Is(err, ErrClusterMapEmpty):
		s.writeError(w, http.StatusServiceUnavailable, "NO_HEALTHY_NODE", err.Error())
	default:
		s.writeError(w, http.StatusBadGateway, "ROUTING_ERROR", err.Error())
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, message string) {
	s.writeJSON(w, status, ErrorResponse{
		Error: message,
		Code:  code,
	})
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

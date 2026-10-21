package dbnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type Server struct {
	node       *Node
	handler    *Handler
	httpServer *http.Server

	// replServer serves the same routes on the node's replication port. The
	// controller hands replicas out to masters as address:replication_port, so
	// a node whose replication port differs from its client port has to be
	// reachable on both or every /replicate call is refused at the socket. It
	// is nil when the two ports are the same, which is how the in-process tests
	// are configured.
	replServer *http.Server

	config ServerConfig
}

type ServerConfig struct {
	Port            int
	ReplicationPort int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

func DefaultServerConfig(port int) ServerConfig {
	return ServerConfig{
		Port:            port,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     60 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}
}

func NewServer(node *Node, cfg ServerConfig) *Server {
	handler := NewHandler(node)

	mux := http.NewServeMux()
	mux.HandleFunc("/set", handler.HandleSet)
	mux.HandleFunc("/get", handler.HandleGet)
	mux.HandleFunc("/delete", handler.HandleDelete)
	mux.HandleFunc("/replicate", handler.HandleReplicate)
	mux.HandleFunc("/internal/replicate", handler.HandleReplicate)
	mux.HandleFunc("/snapshot", handler.HandleSnapshot)
	mux.HandleFunc("/internal/snapshot", handler.HandleSnapshot)
	mux.HandleFunc("/health", handler.HandleHealth)
	mux.HandleFunc("/internal/migrate", handler.HandleMigrate)
	mux.HandleFunc("/internal/receive-migration", handler.HandleReceiveMigration)
	mux.HandleFunc("/internal/topology", handler.HandleTopologyUpdate)
	mux.HandleFunc("/internal/role", handler.HandleRole)
	mux.HandleFunc("/internal/assign-role", handler.HandleRole)
	mux.HandleFunc("/internal/wal", handler.HandleInternalWAL)
	mux.HandleFunc("/internal/range/migrate", handler.HandleRangeMigrate)
	mux.HandleFunc("/internal/range/freeze", handler.HandleRangeFreeze)
	mux.HandleFunc("/internal/range/unfreeze", handler.HandleRangeUnfreeze)
	mux.HandleFunc("/internal/range/activate", handler.HandleRangeActivate)
	mux.HandleFunc("/internal/range/drop", handler.HandleRangeDrop)
	mux.HandleFunc("/internal/range/moved", handler.HandleRangeMoved)
	mux.HandleFunc("/die", handler.HandleDie)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      logMiddleware(mux),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	srv := &Server{
		node:       node,
		handler:    handler,
		httpServer: httpServer,
		config:     cfg,
	}

	if cfg.ReplicationPort > 0 && cfg.ReplicationPort != cfg.Port {
		srv.replServer = &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.ReplicationPort),
			Handler:      logMiddleware(mux),
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		}
	}

	return srv
}

func (s *Server) Start() error {
	if err := s.node.Start(); err != nil {
		return fmt.Errorf("start node: %w", err)
	}

	log.Printf("[server] starting on port %d", s.config.Port)

	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		_ = s.node.Stop()
		return fmt.Errorf("listen %s: %w", s.httpServer.Addr, err)
	}

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[server] listen error: %v", err)
		}
	}()

	if s.replServer != nil {
		replLn, err := net.Listen("tcp", s.replServer.Addr)
		if err != nil {
			_ = s.httpServer.Close()
			_ = s.node.Stop()
			return fmt.Errorf("listen %s (replication): %w", s.replServer.Addr, err)
		}
		log.Printf("[server] also serving replication on port %d", s.config.ReplicationPort)
		go func() {
			if err := s.replServer.Serve(replLn); err != nil && err != http.ErrServerClosed {
				log.Printf("[server] replication listen error: %v", err)
			}
		}()
	}

	return nil
}

func (s *Server) Stop() error {
	log.Println("[server] shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("[server] http shutdown error: %v", err)
	}

	if s.replServer != nil {
		if err := s.replServer.Shutdown(ctx); err != nil {
			log.Printf("[server] replication http shutdown error: %v", err)
		}
	}

	if err := s.node.Stop(); err != nil {
		return fmt.Errorf("stop node: %w", err)
	}

	log.Println("[server] stopped")
	return nil
}

func (s *Server) ListenAndServe() error {
	if err := s.node.Start(); err != nil {
		return fmt.Errorf("start node: %w", err)
	}

	if s.replServer != nil {
		log.Printf("[server] also serving replication on port %d", s.config.ReplicationPort)
		go func() {
			if err := s.replServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[server] replication listen error: %v", err)
			}
		}()
	}

	log.Printf("[server] listening on port %d", s.config.Port)
	return s.httpServer.ListenAndServe()
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

package health

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Server wraps an http.Server that serves /health.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// NewServer constructs the HTTP server on listenAddr with /health registered.
func NewServer(listenAddr string, agg *Aggregator, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("/health", agg.Handler())
	return &Server{
		srv: &http.Server{
			Addr:              listenAddr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log,
	}
}

// Start launches the server in a goroutine and returns immediately.
func (s *Server) Start() {
	go func() {
		s.log.Info("health http listening", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("health http server failed", "err", err)
		}
	}()
}

// Shutdown stops the server with a graceful timeout.
func (s *Server) Shutdown(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

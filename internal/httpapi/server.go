package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// ServerConfig controls the external HTTP server.
type ServerConfig struct {
	ListenAddress     string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

// Server runs the external HTTP API with graceful shutdown.
type Server struct {
	server          *http.Server
	shutdownTimeout time.Duration
	listen          func(string, string) (net.Listener, error)
	logger          *slog.Logger
}

// NewServer validates configuration and constructs the HTTP server.
func NewServer(config ServerConfig, handler http.Handler, logger *slog.Logger) (*Server, error) {
	if config.ListenAddress == "" {
		return nil, errors.New("HTTP listen address must not be empty")
	}
	if config.ReadTimeout <= 0 || config.ReadHeaderTimeout <= 0 || config.WriteTimeout <= 0 || config.IdleTimeout <= 0 || config.ShutdownTimeout <= 0 {
		return nil, errors.New("HTTP server timeouts must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		server: &http.Server{
			Addr: config.ListenAddress, Handler: handler,
			ReadTimeout: config.ReadTimeout, ReadHeaderTimeout: config.ReadHeaderTimeout,
			WriteTimeout: config.WriteTimeout, IdleTimeout: config.IdleTimeout,
		},
		shutdownTimeout: config.ShutdownTimeout,
		listen:          net.Listen,
		logger:          logger,
	}, nil
}

// Run serves requests until cancellation and then drains active requests.
func (server *Server) Run(ctx context.Context) error {
	listener, err := server.listen("tcp", server.server.Addr)
	if err != nil {
		return fmt.Errorf("listen for HTTP requests: %w", err)
	}
	server.logger.InfoContext(ctx, "Mercury HTTP API started", "address", server.server.Addr)
	served := make(chan error, 1)
	go func() { served <- server.server.Serve(listener) }()

	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP requests: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), server.shutdownTimeout)
		defer cancel()
		if err := server.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP requests during shutdown: %w", err)
		}
		server.logger.InfoContext(context.Background(), "Mercury HTTP API stopped")
		return nil
	}
}

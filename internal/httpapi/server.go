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

// ServerConfig defines the listener and resource bounds for the external HTTP
// server. Positive timeouts prevent slow or abandoned connections from holding
// server resources indefinitely and bound graceful shutdown.
type ServerConfig struct {
	ListenAddress     string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

// Server owns the net/http listener and graceful-shutdown lifecycle for the
// external API. Request behavior remains entirely in the injected Handler.
type Server struct {
	server          *http.Server
	shutdownTimeout time.Duration
	listen          func(string, string) (net.Listener, error)
	logger          *slog.Logger
}

// NewServer validates all listener timeouts and constructs a stopped Server.
// A nil logger falls back to slog.Default; no listener is opened until Run.
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

// Run opens the configured listener and serves until the server fails or ctx is
// cancelled. Cancellation starts a separately timed shutdown context so the
// cancelled application context does not prevent in-flight requests from
// draining. After a successful Shutdown, Run joins the serving goroutine before
// returning and verifies that it stopped for the expected reason.
func (server *Server) Run(ctx context.Context) error {
	listener, err := server.listen("tcp", server.server.Addr)
	if err != nil {
		return fmt.Errorf("listen for HTTP requests: %w", err)
	}
	server.logger.InfoContext(ctx, "Mercury HTTP API started", "address", server.server.Addr)
	// The buffered result lets Serve report its terminal error even if listener
	// shutdown completes while Run is moving between select branches.
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

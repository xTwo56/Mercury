package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestServerGracefulShutdown(t *testing.T) {
	listener := &blockingListener{closed: make(chan struct{})}
	server, err := NewServer(validServerConfig(), http.NotFoundHandler(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	server.listen = func(string, string) (net.Listener, error) { return listener, nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := listener.Close(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("listener Close() error = %v, want already closed", err)
	}
}

type blockingListener struct {
	closed chan struct{}
	once   sync.Once
}

func (listener *blockingListener) Accept() (net.Conn, error) {
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *blockingListener) Close() error {
	closedNow := false
	listener.once.Do(func() {
		closedNow = true
		close(listener.closed)
	})
	if !closedNow {
		return net.ErrClosed
	}
	return nil
}

func (*blockingListener) Addr() net.Addr { return testAddress("test") }

type testAddress string

func (address testAddress) Network() string { return string(address) }
func (address testAddress) String() string  { return string(address) }

func TestNewServerValidation(t *testing.T) {
	tests := []struct {
		name   string
		config ServerConfig
	}{
		{name: "missing address", config: ServerConfig{ReadTimeout: time.Second, ReadHeaderTimeout: time.Second, WriteTimeout: time.Second, IdleTimeout: time.Second, ShutdownTimeout: time.Second}},
		{name: "zero read timeout", config: func() ServerConfig { value := validServerConfig(); value.ReadTimeout = 0; return value }()},
		{name: "zero header timeout", config: func() ServerConfig { value := validServerConfig(); value.ReadHeaderTimeout = 0; return value }()},
		{name: "zero write timeout", config: func() ServerConfig { value := validServerConfig(); value.WriteTimeout = 0; return value }()},
		{name: "zero idle timeout", config: func() ServerConfig { value := validServerConfig(); value.IdleTimeout = 0; return value }()},
		{name: "zero shutdown timeout", config: func() ServerConfig { value := validServerConfig(); value.ShutdownTimeout = 0; return value }()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewServer(tt.config, http.NotFoundHandler(), nil); err == nil {
				t.Fatal("NewServer() error = nil, want validation error")
			}
		})
	}
}

func validServerConfig() ServerConfig {
	return ServerConfig{
		ListenAddress: "127.0.0.1:0", ReadTimeout: time.Second,
		ReadHeaderTimeout: time.Second, WriteTimeout: time.Second,
		IdleTimeout: time.Second, ShutdownTimeout: time.Second,
	}
}

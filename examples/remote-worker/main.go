// Command remote-worker demonstrates consuming Mercury from another process.
// It imports only public SDK packages and communicates exclusively over HTTP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xtwo56/mercury/remoteworker"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

func main() {
	client, err := workerclient.New(workerclient.Config{
		ServerURL:   os.Getenv("MERCURY_URL"),
		BearerToken: os.Getenv("MERCURY_WORKER_BEARER_TOKEN"),
		HTTPClient:  &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		log.Fatal(err)
	}
	registry := remoteworker.NewRegistry()
	if err := registry.Register("uppercase", remoteworker.HandlerFunc(uppercase)); err != nil {
		log.Fatal(err)
	}
	runtime, err := remoteworker.New(client, registry, remoteworker.Config{
		WorkerID:           workerclient.WorkerID("example-worker"),
		Concurrency:        4,
		PollInterval:       time.Second,
		HeartbeatInterval:  20 * time.Second,
		UncertainClaimHold: time.Minute,
		ShutdownTimeout:    15 * time.Second,
	}, slog.Default())
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runtime.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func uppercase(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var request struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, remoteworker.Permanent(errors.New("invalid uppercase payload"))
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return json.Marshal(struct {
		Text string `json:"text"`
	}{Text: strings.ToUpper(request.Text)})
}

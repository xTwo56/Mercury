package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClientClaimUsesContractAndClosesResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/v1/worker/jobs/claim" || request.Method != http.MethodPost {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("missing bearer credential")
		}
		body, _ := io.ReadAll(request.Body)
		if !strings.Contains(string(body), `"supported_types":["image","sleep"]`) {
			t.Fatalf("claim body = %s", body)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"id":"job-1","task_type":"sleep","payload":{},"state":"leased","lease":{"worker_id":"worker-1","token":"lease-token","expires_at":"2026-09-20T00:01:00Z"}}`)
	}))
	defer server.Close()
	client := newTestClient(t, Config{ServerURL: server.URL, BearerToken: "secret"})
	job, err := client.Claim(context.Background(), "worker-1", []TaskType{"image", "sleep"})
	if err != nil || job.ID != "job-1" || job.Lease == nil || job.Lease.Token != "lease-token" {
		t.Fatalf("Claim() = %#v, %v", job, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("requests = %d, want 1", calls.Load())
	}
}

func TestClientErrorClassificationsAndUncertainty(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		code      string
		want      error
		uncertain bool
	}{
		{name: "authentication", status: 401, code: "unauthorized", want: ErrAuthentication},
		{name: "ownership", status: 409, code: "stale_execution", want: ErrOwnershipLost},
		{name: "missing", status: 404, code: "job_not_found", want: ErrJobNotFound},
		{name: "server after possible commit", status: 500, code: "internal_error", want: ErrProtocol, uncertain: true},
		{name: "invalid request", status: 400, code: "invalid_worker_request", want: ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(test.status)
				_, _ = io.WriteString(response, `{"error":{"code":"`+test.code+`","message":"safe"}}`)
			}))
			defer server.Close()
			client := newTestClient(t, Config{ServerURL: server.URL, BearerToken: "secret"})
			_, err := client.Start(context.Background(), "job", "worker", "token")
			if !errors.Is(err, test.want) || OutcomeUncertain(err) != test.uncertain {
				t.Fatalf("error = %v; want %v uncertain=%v", err, test.want, test.uncertain)
			}
			if calls.Load() != 1 {
				t.Fatalf("requests = %d, client retried", calls.Load())
			}
		})
	}
}

func TestClientTransportErrorsAreUncertainOnlyForMutations(t *testing.T) {
	transportErr := errors.New("network unavailable")
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})}
	client := newTestClient(t, Config{ServerURL: "https://mercury.example", BearerToken: "secret", HTTPClient: httpClient})
	_, startErr := client.Start(context.Background(), "job", "worker", "token")
	if !errors.Is(startErr, ErrTransport) || !errors.Is(startErr, transportErr) || !OutcomeUncertain(startErr) {
		t.Fatalf("start error = %v", startErr)
	}
	_, inspectErr := client.Inspect(context.Background(), "job")
	if !errors.Is(inspectErr, ErrTransport) || OutcomeUncertain(inspectErr) {
		t.Fatalf("inspect error = %v", inspectErr)
	}
}

func TestClientBoundsResponsesDisablesRedirectsAndClosesBodies(t *testing.T) {
	t.Run("bounded and closed", func(t *testing.T) {
		body := &trackingBody{Reader: strings.NewReader(strings.Repeat("x", 9))}
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
		})}
		client := newTestClient(t, Config{ServerURL: "https://mercury.example", BearerToken: "secret", HTTPClient: httpClient, MaxResponseBytes: 8})
		_, err := client.Inspect(context.Background(), "job")
		if !errors.Is(err, ErrTransport) || !body.closed.Load() {
			t.Fatalf("error/closed = %v/%v", err, body.closed.Load())
		}
	})

	t.Run("redirect not followed", func(t *testing.T) {
		var destinationCalls atomic.Int32
		destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationCalls.Add(1) }))
		defer destination.Close()
		source := httptest.NewServer(http.RedirectHandler(destination.URL, http.StatusTemporaryRedirect))
		defer source.Close()
		client := newTestClient(t, Config{ServerURL: source.URL, BearerToken: "secret"})
		_, err := client.Start(context.Background(), "job", "worker", "token")
		if !errors.Is(err, ErrProtocol) || destinationCalls.Load() != 0 {
			t.Fatalf("error/destination calls = %v/%d", err, destinationCalls.Load())
		}
	})
}

func TestClientContextCancellationIsPreserved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	})}
	client := newTestClient(t, Config{ServerURL: "https://mercury.example", BearerToken: "secret", HTTPClient: httpClient})
	_, err := client.Inspect(ctx, "job")
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrTransport) {
		t.Fatalf("error = %v", err)
	}
}

func newTestClient(t *testing.T, config Config) *Client {
	t.Helper()
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type trackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (body *trackingBody) Close() error {
	body.closed.Store(true)
	return nil
}

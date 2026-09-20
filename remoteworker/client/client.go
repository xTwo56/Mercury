// Package client implements Mercury's authenticated remote-worker HTTP protocol.
//
// In simple terms, Client asks Mercury for work and reports lifecycle changes.
// It performs exactly one HTTP request per method call: callers can inspect an
// uncertain error and reconcile state, but the client never blindly retries a
// request that may already have changed a job.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultMaxRequestBytes  int64 = 1 << 20
	defaultMaxResponseBytes int64 = 1 << 20
)

var (
	// ErrNoJobAvailable means Mercury currently has no eligible supported work.
	ErrNoJobAvailable = errors.New("no remote job available")
	// ErrAuthentication means Mercury rejected the configured bearer credential.
	ErrAuthentication = errors.New("remote worker authentication failed")
	// ErrOwnershipLost means the execution state, worker, token, or lease is stale.
	ErrOwnershipLost = errors.New("remote job ownership lost")
	// ErrJobNotFound means Mercury has no job with the requested ID.
	ErrJobNotFound = errors.New("remote job not found")
	// ErrProtocol means a request or response violated the worker HTTP contract.
	ErrProtocol = errors.New("remote worker protocol error")
	// ErrTransport means Mercury did not provide a usable HTTP response.
	ErrTransport = errors.New("remote worker transport error")
)

// TaskType identifies a task contract understood by an application handler.
type TaskType string

// JobID identifies a Mercury job.
type JobID string

// WorkerID identifies one remote worker process.
type WorkerID string

// LeaseToken is Mercury's opaque fencing credential for one execution attempt.
type LeaseToken string

// State is the durable lifecycle state returned by Mercury.
type State string

const (
	// StateQueued is available for a future claim once its availability time arrives.
	StateQueued State = "queued"
	// StateLeased is owned temporarily but execution has not been confirmed.
	StateLeased State = "leased"
	// StateRunning is executing under a live lease.
	StateRunning State = "running"
	// StateRetryScheduled will become claimable at its availability time.
	StateRetryScheduled State = "retry_scheduled"
	// StateSucceeded is a terminal successful outcome.
	StateSucceeded State = "succeeded"
	// StateFailed is a terminal failed outcome.
	StateFailed State = "failed"
)

// FailureClassification tells Mercury whether a handler error may be retried.
// Mercury, not the remote runtime, decides when a retry becomes available.
type FailureClassification string

const (
	// FailureRetryable lets Mercury schedule another attempt when budget remains.
	FailureRetryable FailureClassification = "retryable"
	// FailurePermanent asks Mercury to fail the job immediately.
	FailurePermanent FailureClassification = "permanent"
)

// Lease describes the worker and fencing token currently owning a job.
// ExpiresAt is the last server-confirmed boundary; ownership is not valid at or
// after that instant.
type Lease struct {
	WorkerID  WorkerID   `json:"worker_id"`
	Token     LeaseToken `json:"token"`
	ExpiresAt time.Time  `json:"expires_at"`
}

// Job is the public worker projection returned by Mercury. Payload and Result
// remain raw JSON so applications can define task-specific contracts without
// importing Mercury's internal domain package.
type Job struct {
	ID                JobID           `json:"id"`
	TaskType          TaskType        `json:"task_type"`
	Payload           json.RawMessage `json:"payload"`
	State             State           `json:"state"`
	MaxAttempts       int             `json:"max_attempts"`
	AttemptsStarted   int             `json:"attempts_started"`
	RemainingAttempts int             `json:"remaining_attempts"`
	CreatedAt         time.Time       `json:"created_at"`
	AvailableAt       time.Time       `json:"available_at"`
	StartedAt         *time.Time      `json:"started_at"`
	CompletedAt       *time.Time      `json:"completed_at"`
	Result            json.RawMessage `json:"result"`
	LastError         string          `json:"last_error,omitempty"`
	FailedAt          *time.Time      `json:"failed_at"`
	Lease             *Lease          `json:"lease"`
	ExecutionDeadline *time.Time      `json:"execution_deadline"`
}

// Config controls HTTP connectivity and local message-size bounds. HTTPClient
// is copied before its redirect policy is disabled, so its transport and
// connection pool are reused without mutating the caller's client.
type Config struct {
	ServerURL        string
	BearerToken      string
	HTTPClient       *http.Client
	MaxRequestBytes  int64
	MaxResponseBytes int64
}

// OperationError describes a failed lifecycle request without retaining or
// printing bearer credentials, lease tokens, request bodies, or response bodies.
// Uncertain is true only when Mercury may have committed a state-changing
// operation before the response became unusable.
type OperationError struct {
	Operation  string
	StatusCode int
	Code       string
	Message    string
	Uncertain  bool
	Kind       error
	Cause      error
}

func (err *OperationError) Error() string {
	if err.StatusCode != 0 {
		return fmt.Sprintf("remote worker %s: HTTP %d (%s)", err.Operation, err.StatusCode, err.Code)
	}
	return fmt.Sprintf("remote worker %s: %v", err.Operation, err.Kind)
}

// Unwrap preserves both the stable SDK classification and low-level causes
// such as context cancellation for errors.Is checks.
func (err *OperationError) Unwrap() []error {
	if err.Cause == nil {
		return []error{err.Kind}
	}
	return []error{err.Kind, err.Cause}
}

// OutcomeUncertain reports whether a state-changing request may have committed.
// Callers should inspect known jobs before deciding what happened; an uncertain
// claim has no job ID and must be allowed to expire rather than retried blindly.
func OutcomeUncertain(err error) bool {
	var operationError *OperationError
	return errors.As(err, &operationError) && operationError.Uncertain
}

// Client consumes Mercury's remote-worker lifecycle endpoints. It is safe for
// concurrent use and contains no database dependency.
type Client struct {
	base             *url.URL
	bearerToken      string
	httpClient       *http.Client
	maxRequestBytes  int64
	maxResponseBytes int64
}

// New validates connectivity configuration and creates a reusable Client.
// Redirects are never followed because forwarding authorization to another
// origin could disclose the bearer credential.
func New(config Config) (*Client, error) {
	base, err := url.Parse(config.ServerURL)
	if err != nil || base.Scheme == "" || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, errors.New("remote worker server URL must be an absolute HTTP or HTTPS URL")
	}
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("remote worker server URL must not contain user information, query, or fragment")
	}
	if strings.TrimSpace(config.BearerToken) == "" || strings.ContainsAny(config.BearerToken, " \t\r\n") {
		return nil, errors.New("remote worker bearer token must be nonblank and contain no whitespace")
	}
	maxRequestBytes := config.MaxRequestBytes
	if maxRequestBytes == 0 {
		maxRequestBytes = defaultMaxRequestBytes
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	if maxRequestBytes <= 0 || maxResponseBytes <= 0 {
		return nil, errors.New("remote worker HTTP size limits must be positive")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	clientCopy := *httpClient
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base.Path = strings.TrimRight(base.Path, "/")
	return &Client{
		base: base, bearerToken: config.BearerToken, httpClient: &clientCopy,
		maxRequestBytes: maxRequestBytes, maxResponseBytes: maxResponseBytes,
	}, nil
}

// Claim requests one available job whose task type appears in supportedTypes.
// A 204 response becomes ErrNoJobAvailable. If the response is uncertain, the
// caller must not immediately claim again with the same capacity slot because
// Mercury may already have created an unobserved lease.
func (client *Client) Claim(ctx context.Context, workerID WorkerID, supportedTypes []TaskType) (Job, error) {
	request := struct {
		WorkerID       WorkerID   `json:"worker_id"`
		SupportedTypes []TaskType `json:"supported_types"`
	}{workerID, supportedTypes}
	var result Job
	err := client.doJSON(ctx, "claim", http.MethodPost, "/v1/worker/jobs/claim", request, &result, true, true)
	return result, err
}

// Start confirms that Mercury persisted the leased-to-running transition before
// a handler is allowed to execute. An uncertain error should be reconciled with
// Inspect using the same job ID and ownership credentials.
func (client *Client) Start(ctx context.Context, id JobID, workerID WorkerID, token LeaseToken) (Job, error) {
	return client.ownershipOperation(ctx, "start", id, workerID, token)
}

// Heartbeat asks Mercury to extend a running execution's lease. The server
// chooses the new expiration and enforces its maximum execution duration.
func (client *Client) Heartbeat(ctx context.Context, id JobID, workerID WorkerID, token LeaseToken) (Job, error) {
	return client.ownershipOperation(ctx, "heartbeat", id, workerID, token)
}

// Complete reports one successful outcome. Client sends the request once; an
// uncertain response must be reconciled with Inspect instead of blindly replayed.
func (client *Client) Complete(ctx context.Context, id JobID, workerID WorkerID, token LeaseToken, result json.RawMessage) (Job, error) {
	request := struct {
		ownershipRequest
		Result json.RawMessage `json:"result"`
	}{ownershipRequest: ownershipRequest{workerID, token}, Result: result}
	var completed Job
	err := client.doJSON(ctx, "complete", http.MethodPost, client.jobPath(id, "complete"), request, &completed, true, false)
	return completed, err
}

// Fail reports one retryable or permanent handler failure. Mercury applies the
// attempt budget and schedules any retry; the client never computes retry time.
func (client *Client) Fail(ctx context.Context, id JobID, workerID WorkerID, token LeaseToken, classification FailureClassification, message string) (Job, error) {
	request := struct {
		ownershipRequest
		Classification FailureClassification `json:"classification"`
		Message        string                `json:"message"`
	}{ownershipRequest: ownershipRequest{workerID, token}, Classification: classification, Message: message}
	var failed Job
	err := client.doJSON(ctx, "fail", http.MethodPost, client.jobPath(id, "fail"), request, &failed, true, false)
	return failed, err
}

// Inspect reads current durable state without reserving or mutating work. It is
// the reconciliation operation for lifecycle requests with uncertain responses.
func (client *Client) Inspect(ctx context.Context, id JobID) (Job, error) {
	var inspected Job
	err := client.doJSON(ctx, "inspect", http.MethodGet, client.jobPath(id, ""), nil, &inspected, false, false)
	return inspected, err
}

type ownershipRequest struct {
	WorkerID   WorkerID   `json:"worker_id"`
	LeaseToken LeaseToken `json:"lease_token"`
}

func (client *Client) ownershipOperation(ctx context.Context, operation string, id JobID, workerID WorkerID, token LeaseToken) (Job, error) {
	var result Job
	err := client.doJSON(ctx, operation, http.MethodPost, client.jobPath(id, operation), ownershipRequest{workerID, token}, &result, true, false)
	return result, err
}

func (client *Client) jobPath(id JobID, operation string) string {
	path := "/v1/worker/jobs/" + url.PathEscape(string(id))
	if operation != "" {
		path += "/" + operation
	}
	return path
}

func (client *Client) doJSON(ctx context.Context, operation, method, path string, input, output any, mutation, noContent bool) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return &OperationError{Operation: operation, Kind: ErrProtocol, Cause: err}
		}
		if int64(len(body)) > client.maxRequestBytes {
			return &OperationError{Operation: operation, Kind: ErrProtocol, Cause: errors.New("request body exceeds configured limit")}
		}
	}
	endpoint := *client.base
	endpoint.Path = client.base.Path + path
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return &OperationError{Operation: operation, Kind: ErrProtocol, Cause: err}
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return &OperationError{Operation: operation, Kind: ErrTransport, Cause: err, Uncertain: mutation}
	}
	defer response.Body.Close()
	responseBody, readErr := readBounded(response.Body, client.maxResponseBytes)
	if readErr != nil {
		return &OperationError{Operation: operation, Kind: ErrTransport, Cause: readErr, Uncertain: mutation}
	}
	if noContent && response.StatusCode == http.StatusNoContent {
		return ErrNoJobAvailable
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError(operation, response.StatusCode, responseBody, mutation)
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return &OperationError{Operation: operation, Kind: ErrProtocol, Cause: errors.New("successful response is not application/json"), Uncertain: mutation}
	}
	if err := decodeOne(responseBody, output); err != nil {
		return &OperationError{Operation: operation, Kind: ErrProtocol, Cause: err, Uncertain: mutation}
	}
	return nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("response body exceeds configured limit")
	}
	return body, nil
}

func decodeOne(body []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode response JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("response contains trailing JSON")
	}
	return nil
}

func responseError(operation string, status int, body []byte, mutation bool) error {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	kind := ErrProtocol
	switch status {
	case http.StatusUnauthorized:
		kind = ErrAuthentication
	case http.StatusNotFound:
		kind = ErrJobNotFound
	case http.StatusConflict:
		kind = ErrOwnershipLost
	}
	uncertain := mutation && status >= 500
	return &OperationError{
		Operation: operation, StatusCode: status, Code: envelope.Error.Code,
		Message: envelope.Error.Message, Kind: kind, Uncertain: uncertain,
	}
}

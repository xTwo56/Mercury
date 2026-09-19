package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/xtwo56/mercury/internal/app"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/storage/postgres"
)

// RemoteWorkers is the generic lifecycle boundary exposed to authenticated workers.
type RemoteWorkers interface {
	Claim(context.Context, job.WorkerID, []job.TaskType) (job.Job, error)
	Start(context.Context, job.JobID, job.WorkerID, job.LeaseToken) (job.Job, error)
	Heartbeat(context.Context, job.JobID, job.WorkerID, job.LeaseToken) (job.Job, error)
	Complete(context.Context, job.JobID, job.WorkerID, job.LeaseToken, json.RawMessage) (job.Job, error)
	Fail(context.Context, job.JobID, job.WorkerID, job.LeaseToken, string, string) (job.Job, error)
	Inspect(context.Context, job.JobID) (job.Job, error)
}

type workerHandler struct {
	workers    RemoteWorkers
	credential [sha256.Size]byte
	fallback   http.Handler
}

// NewWorkerHandler protects the entire worker namespace before decoding requests.
// Bearer authentication grants API access; worker IDs and fencing tokens only
// prove ownership of a particular execution. Existing producer routes are delegated.
func NewWorkerHandler(workers RemoteWorkers, credential string, fallback http.Handler) (http.Handler, error) {
	if strings.TrimSpace(credential) == "" || strings.ContainsAny(credential, " \t\r\n") {
		return nil, errors.New("worker bearer credential must be nonblank and contain no whitespace")
	}
	if workers == nil || fallback == nil {
		return nil, errors.New("worker HTTP dependencies must not be nil")
	}
	return &workerHandler{workers: workers, credential: sha256.Sum256([]byte(credential)), fallback: fallback}, nil
}

func (h *workerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/worker" && !strings.HasPrefix(r.URL.Path, "/v1/worker/") {
		h.fallback.ServeHTTP(w, r)
		return
	}
	// Never cache lease credentials, and never include them in errors or logs.
	w.Header().Set("Cache-Control", "no-store")
	values := r.Header.Values("Authorization")
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(values) != 1 || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		h.unauthorized(w)
		return
	}
	supplied := sha256.Sum256([]byte(parts[1]))
	if subtle.ConstantTimeCompare(supplied[:], h.credential[:]) != 1 {
		h.unauthorized(w)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/worker/jobs/")
	if path == r.URL.Path {
		writeError(w, 404, "not_found", "resource not found")
		return
	}
	if path == "claim" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		var body struct {
			WorkerID       job.WorkerID   `json:"worker_id"`
			SupportedTypes []job.TaskType `json:"supported_types"`
		}
		if !decodeWorker(w, r, &body) {
			return
		}
		value, err := h.workers.Claim(r.Context(), body.WorkerID, body.SupportedTypes)
		h.respond(w, value, err)
		return
	}
	parts = strings.Split(path, "/")
	if parts[0] == "" || len(parts) > 2 {
		writeError(w, 404, "not_found", "resource not found")
		return
	}
	id := job.JobID(parts[0])
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		value, err := h.workers.Inspect(r.Context(), id)
		h.respond(w, value, err)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var value job.Job
	var err error
	switch parts[1] {
	case "start", "heartbeat":
		var body ownershipRequest
		if !decodeWorker(w, r, &body) {
			return
		}
		if parts[1] == "start" {
			value, err = h.workers.Start(r.Context(), id, body.WorkerID, body.LeaseToken)
		} else {
			value, err = h.workers.Heartbeat(r.Context(), id, body.WorkerID, body.LeaseToken)
		}
	case "complete":
		var body struct {
			ownershipRequest
			Result json.RawMessage `json:"result"`
		}
		if !decodeWorker(w, r, &body) {
			return
		}
		value, err = h.workers.Complete(r.Context(), id, body.WorkerID, body.LeaseToken, body.Result)
	case "fail":
		var body struct {
			ownershipRequest
			Classification string `json:"classification"`
			Message        string `json:"message"`
		}
		if !decodeWorker(w, r, &body) {
			return
		}
		value, err = h.workers.Fail(r.Context(), id, body.WorkerID, body.LeaseToken, body.Classification, body.Message)
	default:
		writeError(w, 404, "not_found", "resource not found")
		return
	}
	h.respond(w, value, err)
}

type ownershipRequest struct {
	WorkerID   job.WorkerID   `json:"worker_id"`
	LeaseToken job.LeaseToken `json:"lease_token"`
}

func (h *workerHandler) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="mercury-workers"`)
	writeError(w, 401, "unauthorized", "worker authentication required")
}

// decodeWorker bounds untrusted input and rejects misspelled or trailing fields.
// Semantic validation remains in the service so non-HTTP callers get the same rules.
func decodeWorker(w http.ResponseWriter, r *http.Request, target any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, 415, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(target)
	if err == nil {
		var extra any
		err = decoder.Decode(&extra)
		if errors.Is(err, io.EOF) {
			err = nil
		} else if err == nil {
			err = errors.New("unexpected trailing JSON")
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, 413, "request_too_large", "request body is too large")
		} else {
			writeError(w, 400, "invalid_json", "request body must contain one valid JSON object")
		}
		return false
	}
	return true
}

type workerLeaseResponse struct {
	WorkerID  job.WorkerID   `json:"worker_id"`
	Token     job.LeaseToken `json:"token"`
	ExpiresAt time.Time      `json:"expires_at"`
}
type workerJobResponse struct {
	jobResponse
	Lease             *workerLeaseResponse `json:"lease"`
	ExecutionDeadline *time.Time           `json:"execution_deadline"`
}

// respond translates errors without exposing database details. Inspection includes
// the current lease so a worker can distinguish its execution from a newer owner.
func (h *workerHandler) respond(w http.ResponseWriter, value job.Job, err error) {
	switch {
	case errors.Is(err, postgres.ErrNoJobAvailable):
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, app.ErrInvalidWorkerRequest):
		writeError(w, 400, "invalid_worker_request", "invalid worker request")
	case errors.Is(err, postgres.ErrJobNotFound):
		writeError(w, 404, "job_not_found", "job not found")
	case errors.Is(err, postgres.ErrLifecycleConflict):
		writeError(w, 409, "stale_execution", "execution state or ownership changed; inspect job state")
	case err != nil:
		writeError(w, 500, "internal_error", "internal server error; inspect job state before repeating a report")
	default:
		body := workerJobResponse{jobResponse: publicJob(value)}
		if value.Lease != nil {
			body.Lease = &workerLeaseResponse{value.Lease.WorkerID, value.Lease.Token, value.Lease.ExpiresAt}
		}
		if value.StartedAt != nil {
			deadline := value.StartedAt.Add(app.RemoteMaxExecution)
			body.ExecutionDeadline = &deadline
		}
		writeJSON(w, 200, body)
	}
}

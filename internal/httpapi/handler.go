// Package httpapi adapts Mercury's application-level job use cases to HTTP.
// Handler owns routing, transport validation, error/status mapping, and the
// public representation of a job; it delegates task validation, job creation,
// idempotent persistence, and retrieval to JobService. Server owns the network
// listener and coordinated shutdown without adding business logic.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xtwo56/mercury/internal/app"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

const (
	maxRequestBodyBytes    int64 = 1 << 20
	maxIdempotencyKeyBytes       = 255
)

// JobService is the narrow application boundary required by Handler. Submit
// returns whether an idempotent request created or replayed a job, while Get
// returns the current aggregate for inspection. Implementations remain
// responsible for domain validation and persistence transactions.
type JobService interface {
	Submit(context.Context, app.Submission) (app.SubmissionResult, error)
	Get(context.Context, job.JobID) (job.Job, error)
}

// Handler exposes job submission and inspection through the versioned HTTP
// routes. It contains no repository or task-registry dependency directly,
// keeping HTTP concerns separate from application and storage behavior.
type Handler struct {
	jobs JobService
	auth bearerAuthenticator
}

// NewHandler constructs the external HTTP adapter around jobs. The producer
// credential protects submission only; job inspection retains its existing
// public behavior and worker lifecycle routes use a separate authenticator.
func NewHandler(jobs JobService, producerCredential string) (*Handler, error) {
	if jobs == nil {
		return nil, errors.New("job HTTP service must not be nil")
	}
	auth, err := newBearerAuthenticator("producer", producerCredential)
	if err != nil {
		return nil, err
	}
	return &Handler{jobs: jobs, auth: auth}, nil
}

// ServeHTTP dispatches POST /v1/jobs and GET /v1/jobs/{jobID}. It rejects
// unsupported methods before invoking application logic and deliberately
// treats malformed or nested job paths as unknown resources.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.URL.Path == "/v1/jobs":
		if request.Method != http.MethodPost {
			methodNotAllowed(response, http.MethodPost)
			return
		}
		// Simply: authenticate the producer before reading or validating its job.
		// Technically: exactly one timing-safely matched bearer value is required
		// before task validation, ID generation, or idempotent persistence can run.
		if !handler.auth.authorized(request) {
			handler.unauthorizedProducer(response)
			return
		}
		handler.submit(response, request)
	case strings.HasPrefix(request.URL.Path, "/v1/jobs/"):
		if request.Method != http.MethodGet {
			methodNotAllowed(response, http.MethodGet)
			return
		}
		id := strings.TrimPrefix(request.URL.Path, "/v1/jobs/")
		if id == "" || strings.Contains(id, "/") {
			writeError(response, http.StatusNotFound, "not_found", "resource not found")
			return
		}
		handler.get(response, request, job.JobID(id))
	default:
		writeError(response, http.StatusNotFound, "not_found", "resource not found")
	}
}

func (handler *Handler) unauthorizedProducer(response http.ResponseWriter) {
	response.Header().Set("WWW-Authenticate", `Bearer realm="mercury-producers"`)
	writeError(response, http.StatusUnauthorized, "unauthorized", "producer authentication required")
}

type submissionRequest struct {
	TaskType    string          `json:"task_type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	AvailableAt *string         `json:"available_at,omitempty"`
}

// submit validates the HTTP envelope, converts transport fields into an
// application Submission, and maps the resulting application errors to stable
// public responses. Defaults and lifecycle fields are intentionally left to
// app.JobService so non-HTTP callers receive identical submission semantics.
func (handler *Handler) submit(response http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}

	// Limit the stream before decoding so a syntactically valid oversized body
	// cannot force unbounded buffering or JSON parsing work.
	request.Body = http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var submitted submissionRequest
	if err := decoder.Decode(&submitted); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(response, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid_json", "request body must contain valid JSON")
		return
	}
	if err := requireJSONEnd(decoder); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return
	}
	// Preserve absence separately from a supplied key: absence creates on every
	// request, while a supplied key participates in atomic storage deduplication.
	idempotencyKey, err := requestIdempotencyKey(request)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is invalid")
		return
	}

	var availableAt *time.Time
	if submitted.AvailableAt != nil {
		parsed, err := time.Parse(time.RFC3339, *submitted.AvailableAt)
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_available_at", "available_at must be an RFC 3339 timestamp")
			return
		}
		availableAt = &parsed
	}
	created, err := handler.jobs.Submit(request.Context(), app.Submission{
		TaskType:       job.TaskType(submitted.TaskType),
		Payload:        submitted.Payload,
		MaxAttempts:    submitted.MaxAttempts,
		AvailableAt:    availableAt,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		switch {
		case errors.Is(err, task.ErrUnsupportedType):
			writeError(response, http.StatusBadRequest, "unsupported_task_type", "task_type is not supported")
		case errors.Is(err, task.ErrInvalidPayload):
			writeError(response, http.StatusBadRequest, "invalid_payload", "payload does not match the task definition")
		case errors.Is(err, app.ErrInvalidSubmission):
			writeError(response, http.StatusBadRequest, "invalid_job", "job submission is invalid")
		case errors.Is(err, app.ErrIdempotencyConflict):
			writeError(response, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used for another submission")
		default:
			writeError(response, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}

	response.Header().Set("Location", "/v1/jobs/"+url.PathEscape(string(created.Job.ID)))
	// A replay returns the existing job's current representation and is not a
	// second creation, so it receives 200 rather than 201.
	status := http.StatusCreated
	if created.Replayed {
		status = http.StatusOK
	}
	writeJSON(response, status, publicJob(created.Job))
}

// requestIdempotencyKey distinguishes an absent key from a present opaque key.
// Requiring one printable-ASCII header value avoids ambiguous normalization by
// HTTP intermediaries; case and byte content are otherwise preserved exactly.
func requestIdempotencyKey(request *http.Request) (*string, error) {
	values := request.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > maxIdempotencyKeyBytes {
		return nil, errors.New("invalid Idempotency-Key")
	}
	for _, character := range []byte(values[0]) {
		if character < 0x21 || character > 0x7e {
			return nil, errors.New("invalid Idempotency-Key")
		}
	}
	key := values[0]
	return &key, nil
}

// get retrieves the job's latest durable state and prevents repository error
// details from crossing the HTTP boundary. Only the application's distinguishable
// not-found error becomes a 404.
func (handler *Handler) get(response http.ResponseWriter, request *http.Request, id job.JobID) {
	loaded, err := handler.jobs.Get(request.Context(), id)
	if errors.Is(err, app.ErrJobNotFound) {
		writeError(response, http.StatusNotFound, "job_not_found", "job not found")
		return
	}
	if err != nil {
		writeError(response, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(response, http.StatusOK, publicJob(loaded))
}

// jobResponse is the public job projection. It includes task and lifecycle
// information useful to submitters but deliberately omits worker ownership,
// lease expiration, and lease tokens.
type jobResponse struct {
	ID                job.JobID       `json:"id"`
	TaskType          job.TaskType    `json:"task_type"`
	Payload           json.RawMessage `json:"payload"`
	State             job.State       `json:"state"`
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
}

// publicJob projects the domain aggregate into its credential-free HTTP form.
// Remaining attempts are calculated by the domain rather than duplicated here.
func publicJob(value job.Job) jobResponse {
	return jobResponse{
		ID: value.ID, TaskType: value.TaskType, Payload: value.Payload, State: value.State,
		MaxAttempts: value.MaxAttempts, AttemptsStarted: value.AttemptsStarted,
		RemainingAttempts: value.RemainingAttempts(), CreatedAt: value.CreatedAt,
		AvailableAt: value.AvailableAt, StartedAt: value.StartedAt, CompletedAt: value.CompletedAt,
		Result: value.Result, LastError: value.LastError, FailedAt: value.FailedAt,
	}
}

// errorResponse provides one consistent, non-sensitive JSON error envelope.
type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// methodNotAllowed writes both the 405 body and the required Allow header so
// clients can discover the method supported by the matched route.
func methodNotAllowed(response http.ResponseWriter, allowed string) {
	response.Header().Set("Allow", allowed)
	writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

// writeError converts an internal error classification into the API's stable
// error envelope. Callers choose safe messages rather than exposing causes.
func writeError(response http.ResponseWriter, status int, code, message string) {
	body := errorResponse{}
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(response, status, body)
}

// writeJSON commits status and serializes body with the API content type.
func writeJSON(response http.ResponseWriter, status int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(body)
}

// requireJSONEnd requires EOF after the first decoded object, rejecting a
// second value or trailing non-whitespace data that Decoder.Decode alone would
// otherwise leave unread.
func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

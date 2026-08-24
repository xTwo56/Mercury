// Package httpapi exposes Mercury job use cases over HTTP.
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

const maxRequestBodyBytes int64 = 1 << 20

// JobService is the application boundary used by HTTP handlers.
type JobService interface {
	Submit(context.Context, app.Submission) (job.Job, error)
	Get(context.Context, job.JobID) (job.Job, error)
}

// Handler serves job submission and inspection endpoints.
type Handler struct {
	jobs JobService
}

// NewHandler constructs the external HTTP handler.
func NewHandler(jobs JobService) *Handler {
	return &Handler{jobs: jobs}
}

// ServeHTTP routes the minimal versioned jobs API.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.URL.Path == "/v1/jobs":
		if request.Method != http.MethodPost {
			methodNotAllowed(response, http.MethodPost)
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

type submissionRequest struct {
	TaskType    string          `json:"task_type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	AvailableAt *string         `json:"available_at,omitempty"`
}

func (handler *Handler) submit(response http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}

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
		TaskType:    job.TaskType(submitted.TaskType),
		Payload:     submitted.Payload,
		MaxAttempts: submitted.MaxAttempts,
		AvailableAt: availableAt,
	})
	if err != nil {
		switch {
		case errors.Is(err, task.ErrUnsupportedType):
			writeError(response, http.StatusBadRequest, "unsupported_task_type", "task_type is not supported")
		case errors.Is(err, task.ErrInvalidPayload):
			writeError(response, http.StatusBadRequest, "invalid_payload", "payload does not match the task definition")
		case errors.Is(err, app.ErrInvalidSubmission):
			writeError(response, http.StatusBadRequest, "invalid_job", "job submission is invalid")
		default:
			writeError(response, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}

	response.Header().Set("Location", "/v1/jobs/"+url.PathEscape(string(created.ID)))
	writeJSON(response, http.StatusCreated, publicJob(created))
}

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

func publicJob(value job.Job) jobResponse {
	return jobResponse{
		ID: value.ID, TaskType: value.TaskType, Payload: value.Payload, State: value.State,
		MaxAttempts: value.MaxAttempts, AttemptsStarted: value.AttemptsStarted,
		RemainingAttempts: value.RemainingAttempts(), CreatedAt: value.CreatedAt,
		AvailableAt: value.AvailableAt, StartedAt: value.StartedAt, CompletedAt: value.CompletedAt,
		Result: value.Result, LastError: value.LastError, FailedAt: value.FailedAt,
	}
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func methodNotAllowed(response http.ResponseWriter, allowed string) {
	response.Header().Set("Allow", allowed)
	writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	body := errorResponse{}
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(response, status, body)
}

func writeJSON(response http.ResponseWriter, status int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(body)
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

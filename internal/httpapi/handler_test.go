package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/app"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

func TestHandlerSubmitJob(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.FixedZone("test", 5*60*60+30*60))
	delayed := now.Add(time.Hour).Format(time.RFC3339)
	five := 5
	tests := []struct {
		name            string
		body            string
		wantAttempts    int
		wantAvailableAt time.Time
	}{
		{name: "immediate defaults", body: `{"task_type":"sleep","payload":{"duration_ms":250}}`, wantAttempts: 3, wantAvailableAt: now.UTC()},
		{name: "delayed explicit attempts", body: `{"task_type":"sleep","payload":{"duration_ms":500},"max_attempts":5,"available_at":"` + delayed + `"}`, wantAttempts: five, wantAvailableAt: now.Add(time.Hour).UTC()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeJobStore{}
			handler := testHandler(t, store, now)
			response := performRequest(handler, http.MethodPost, "/v1/jobs", "application/json", tt.body)
			if response.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201; body=%s", response.Code, response.Body.String())
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", response.Header().Get("Content-Type"))
			}
			if response.Header().Get("Location") != "/v1/jobs/job-generated" {
				t.Errorf("Location = %q, want generated job URL", response.Header().Get("Location"))
			}
			created := store.createdJob(t)
			if created.ID != job.JobID("job-generated") || created.State != job.StateQueued || created.AttemptsStarted != 0 {
				t.Errorf("created lifecycle fields = %#v, want generated queued job", created)
			}
			if created.MaxAttempts != tt.wantAttempts || !created.CreatedAt.Equal(now.UTC()) || !created.AvailableAt.Equal(tt.wantAvailableAt) {
				t.Errorf("created defaults = %#v, want attempts %d and availability %v", created, tt.wantAttempts, tt.wantAvailableAt)
			}
			if created.CreatedAt.Location() != time.UTC || created.AvailableAt.Location() != time.UTC {
				t.Errorf("created timestamps not normalized to UTC: %#v", created)
			}
		})
	}
}

func TestHandlerIdempotentSubmission(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	body := `{"task_type":"sleep","payload":{"duration_ms":250}}`
	store := &fakeJobStore{}
	handler := testHandler(t, store, now)

	first := performRequestWithIdempotencyKey(handler, body, "Case-Sensitive-Key")
	replay := performRequestWithIdempotencyKey(handler, body, "Case-Sensitive-Key")
	if first.Code != http.StatusCreated || replay.Code != http.StatusOK {
		t.Fatalf("first/replay status = %d/%d, want 201/200", first.Code, replay.Code)
	}
	if first.Header().Get("Location") != replay.Header().Get("Location") {
		t.Errorf("first/replay locations differ: %q/%q", first.Header().Get("Location"), replay.Header().Get("Location"))
	}
	if len(store.created) != 1 {
		t.Errorf("created jobs = %d, want 1", len(store.created))
	}

	store.mu.Lock()
	existing := store.idempotent["Case-Sensitive-Key"]
	completedAt := now.Add(time.Minute)
	existing.job.State = job.StateSucceeded
	existing.job.AttemptsStarted = 1
	existing.job.CompletedAt = &completedAt
	existing.job.Result = json.RawMessage(`{"slept":true}`)
	store.idempotent["Case-Sensitive-Key"] = existing
	store.mu.Unlock()
	completedReplay := performRequestWithIdempotencyKey(handler, body, "Case-Sensitive-Key")
	if completedReplay.Code != http.StatusOK || !strings.Contains(completedReplay.Body.String(), `"state":"succeeded"`) || !strings.Contains(completedReplay.Body.String(), `"slept":true`) {
		t.Errorf("completed replay = %d %s", completedReplay.Code, completedReplay.Body.String())
	}
}

func TestHandlerSubmissionWithoutIdempotencyAlwaysCreates(t *testing.T) {
	store := &fakeJobStore{}
	handler := testHandler(t, store, time.Now().UTC())
	body := `{"task_type":"sleep","payload":{"duration_ms":1}}`
	for range 2 {
		if response := performRequest(handler, http.MethodPost, "/v1/jobs", "application/json", body); response.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201", response.Code)
		}
	}
	if len(store.created) != 2 {
		t.Errorf("created jobs = %d, want 2", len(store.created))
	}
}

func TestHandlerIdempotencyConflictAndKeyValidation(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeJobStore{}
	handler := testHandler(t, store, now)
	if response := performRequestWithIdempotencyKey(handler, `{"task_type":"sleep","payload":{"duration_ms":1}}`, "same-key"); response.Code != http.StatusCreated {
		t.Fatalf("first status = %d", response.Code)
	}
	conflict := performRequestWithIdempotencyKey(handler, `{"task_type":"sleep","payload":{"duration_ms":2}}`, "same-key")
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "idempotency_conflict") {
		t.Errorf("conflict response = %d %s", conflict.Code, conflict.Body.String())
	}

	validBody := `{"task_type":"sleep","payload":{"duration_ms":1}}`
	tests := []struct {
		name   string
		values []string
	}{
		{name: "empty", values: []string{""}},
		{name: "whitespace", values: []string{"contains space"}},
		{name: "control", values: []string{"bad\x01key"}},
		{name: "oversized", values: []string{strings.Repeat("x", maxIdempotencyKeyBytes+1)}},
		{name: "multiple", values: []string{"one", "two"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(validBody))
			request.Header.Set("Content-Type", "application/json")
			request.Header["Idempotency-Key"] = tt.values
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "invalid_idempotency_key") {
				t.Errorf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHandlerIdempotencyFingerprintSemantics(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	base := `{"task_type":"sleep","payload":{"duration_ms":1}}`
	tests := []struct {
		name       string
		firstBody  string
		secondBody string
		wantStatus int
	}{
		{name: "canonical payload", firstBody: base, secondBody: `{ "payload" : { "duration_ms" : 1 }, "task_type" : "sleep" }`, wantStatus: http.StatusOK},
		{name: "different payload", firstBody: base, secondBody: `{"task_type":"sleep","payload":{"duration_ms":2}}`, wantStatus: http.StatusConflict},
		{name: "omitted versus explicit attempts", firstBody: base, secondBody: `{"task_type":"sleep","payload":{"duration_ms":1},"max_attempts":3}`, wantStatus: http.StatusConflict},
		{name: "omitted versus explicit availability", firstBody: base, secondBody: `{"task_type":"sleep","payload":{"duration_ms":1},"available_at":"2026-08-26T12:00:00Z"}`, wantStatus: http.StatusConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeJobStore{}
			handler := testHandler(t, store, now)
			if first := performRequestWithIdempotencyKey(handler, tt.firstBody, "fingerprint-key"); first.Code != http.StatusCreated {
				t.Fatalf("first status = %d", first.Code)
			}
			if second := performRequestWithIdempotencyKey(handler, tt.secondBody, "fingerprint-key"); second.Code != tt.wantStatus {
				t.Errorf("second status = %d, want %d: %s", second.Code, tt.wantStatus, second.Body.String())
			}
		})
	}

	store := &fakeJobStore{}
	handler := testHandler(t, store, now)
	if performRequestWithIdempotencyKey(handler, base, "Case").Code != http.StatusCreated || performRequestWithIdempotencyKey(handler, base, "case").Code != http.StatusCreated {
		t.Error("case-sensitive keys were treated as equal")
	}
}

func TestHandlerConcurrentIdempotentSubmissions(t *testing.T) {
	tests := []struct {
		name                 string
		bodies               []string
		wantOK, wantConflict int
	}{
		{name: "identical", bodies: []string{
			`{"task_type":"sleep","payload":{"duration_ms":1}}`,
			`{"task_type":"sleep","payload":{"duration_ms":1}}`,
		}, wantOK: 2},
		{name: "conflicting", bodies: []string{
			`{"task_type":"sleep","payload":{"duration_ms":1}}`,
			`{"task_type":"sleep","payload":{"duration_ms":2}}`,
		}, wantOK: 1, wantConflict: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeJobStore{}
			handler := testHandler(t, store, time.Now().UTC())
			start := make(chan struct{})
			statuses := make(chan int, len(tt.bodies))
			var wait sync.WaitGroup
			for _, body := range tt.bodies {
				wait.Add(1)
				go func(body string) {
					defer wait.Done()
					<-start
					statuses <- performRequestWithIdempotencyKey(handler, body, "concurrent-key").Code
				}(body)
			}
			close(start)
			wait.Wait()
			close(statuses)
			okCount, conflictCount, createdCount := 0, 0, 0
			for status := range statuses {
				if status == http.StatusCreated {
					createdCount++
					okCount++
				}
				if status == http.StatusOK {
					okCount++
				}
				if status == http.StatusConflict {
					conflictCount++
				}
			}
			if createdCount != 1 || okCount != tt.wantOK || conflictCount != tt.wantConflict || len(store.created) != 1 {
				t.Errorf("created/ok/conflict/rows = %d/%d/%d/%d", createdCount, okCount, conflictCount, len(store.created))
			}
		})
	}
}

func TestHandlerRejectsInvalidSubmissions(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "incorrect content type", contentType: "text/plain", body: `{}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "missing content type", body: `{}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "malformed JSON", contentType: "application/json", body: `{"task_type":`, wantStatus: http.StatusBadRequest},
		{name: "unknown field", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1},"extra":true}`, wantStatus: http.StatusBadRequest},
		{name: "unsupported task", contentType: "application/json", body: `{"task_type":"unknown","payload":{}}`, wantStatus: http.StatusBadRequest},
		{name: "missing duration", contentType: "application/json", body: `{"task_type":"sleep","payload":{}}`, wantStatus: http.StatusBadRequest},
		{name: "zero duration", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":0}}`, wantStatus: http.StatusBadRequest},
		{name: "negative duration", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":-1}}`, wantStatus: http.StatusBadRequest},
		{name: "fractional duration", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1.5}}`, wantStatus: http.StatusBadRequest},
		{name: "unknown sleep field", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1,"other":true}}`, wantStatus: http.StatusBadRequest},
		{name: "zero attempts", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1},"max_attempts":0}`, wantStatus: http.StatusBadRequest},
		{name: "invalid timestamp", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1},"available_at":"tomorrow"}`, wantStatus: http.StatusBadRequest},
		{name: "past availability", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1},"available_at":"2026-08-24T11:59:59Z"}`, wantStatus: http.StatusBadRequest},
		{name: "trailing JSON", contentType: "application/json", body: `{"task_type":"sleep","payload":{"duration_ms":1}} {}`, wantStatus: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeJobStore{}
			response := performRequest(testHandler(t, store, now), http.MethodPost, "/v1/jobs", tt.contentType, tt.body)
			if response.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body=%s", response.Code, tt.wantStatus, response.Body.String())
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", response.Header().Get("Content-Type"))
			}
			if len(store.created) != 0 {
				t.Errorf("invalid submission persisted %d jobs", len(store.created))
			}
		})
	}
}

func TestHandlerRejectsOversizedSubmission(t *testing.T) {
	body := `{"task_type":"sleep","payload":{"duration_ms":1},"padding":"` + strings.Repeat("x", int(maxRequestBodyBytes)) + `"}`
	response := performRequest(testHandler(t, &fakeJobStore{}, time.Now()), http.MethodPost, "/v1/jobs", "application/json", body)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413; body=%s", response.Code, response.Body.String())
	}
}

func TestHandlerRepositoryFailures(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	store := &fakeJobStore{createError: errors.New("database failure"), getError: errors.New("database failure")}
	handler := testHandler(t, store, now)
	created := performRequest(handler, http.MethodPost, "/v1/jobs", "application/json", `{"task_type":"sleep","payload":{"duration_ms":1}}`)
	if created.Code != http.StatusInternalServerError {
		t.Errorf("POST status = %d, want 500", created.Code)
	}
	loaded := performRequest(handler, http.MethodGet, "/v1/jobs/job-1", "", "")
	if loaded.Code != http.StatusInternalServerError {
		t.Errorf("GET status = %d, want 500", loaded.Code)
	}
}

func TestHandlerGetJobSafeRepresentation(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(time.Minute)
	store := &fakeJobStore{loaded: job.Job{
		ID: job.JobID("job-1"), TaskType: job.TaskType("sleep"), Payload: json.RawMessage(`{"duration_ms":10}`),
		State: job.StateRunning, MaxAttempts: 3, AttemptsStarted: 1, CreatedAt: now, AvailableAt: now,
		StartedAt: &startedAt, Lease: &job.Lease{WorkerID: job.WorkerID("secret-worker"), Token: job.LeaseToken("secret-token"), ExpiresAt: now.Add(time.Hour)},
	}}
	response := performRequest(testHandler(t, store, now), http.MethodGet, "/v1/jobs/job-1", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{"secret-token", "secret-worker", "lease_token", "lease_worker", `"lease"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("response exposed internal lease data %q: %s", forbidden, body)
		}
	}
	for _, required := range []string{`"state":"running"`, `"attempts_started":1`, `"remaining_attempts":2`, `"started_at"`, `"result"`, `"failed_at"`} {
		if !strings.Contains(body, required) {
			t.Errorf("response missing %s: %s", required, body)
		}
	}
}

func TestHandlerMissingJobAndMethods(t *testing.T) {
	notFound := errors.New("missing")
	store := &fakeJobStore{getError: notFound, notFound: notFound}
	handler := testHandler(t, store, time.Now())
	if response := performRequest(handler, http.MethodGet, "/v1/jobs/missing", "", ""); response.Code != http.StatusNotFound {
		t.Errorf("missing GET status = %d, want 404", response.Code)
	}
	if response := performRequest(handler, http.MethodGet, "/v1/jobs", "", ""); response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Errorf("collection method response = %d Allow %q", response.Code, response.Header().Get("Allow"))
	}
	if response := performRequest(handler, http.MethodPost, "/v1/jobs/job-1", "application/json", `{}`); response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Errorf("item method response = %d Allow %q", response.Code, response.Header().Get("Allow"))
	}
}

func testHandler(t *testing.T, store *fakeJobStore, now time.Time) http.Handler {
	t.Helper()
	registry := task.NewRegistry(map[job.TaskType]task.Validator{task.SleepTaskType: task.SleepValidator{}})
	service, err := app.NewJobService(store, registry, fixedClock{now: now}, fixedIDGenerator{}, func(err error) bool {
		return store.notFound != nil && errors.Is(err, store.notFound)
	}, func(err error) bool { return errors.Is(err, errFakeIdempotencyConflict) })
	if err != nil {
		t.Fatalf("NewJobService() error = %v", err)
	}
	return NewHandler(service)
}

func performRequest(handler http.Handler, method, path, contentType, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func performRequestWithIdempotencyKey(handler http.Handler, body, key string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fixedIDGenerator struct{}

func (fixedIDGenerator) NewJobID() (job.JobID, error) { return job.JobID("job-generated"), nil }

type fakeJobStore struct {
	mu          sync.Mutex
	created     []job.Job
	createError error
	loaded      job.Job
	getError    error
	notFound    error
	idempotent  map[string]fakeIdempotentJob
}

var errFakeIdempotencyConflict = errors.New("idempotency conflict")

type fakeIdempotentJob struct {
	job         job.Job
	fingerprint []byte
}

func (store *fakeJobStore) Create(_ context.Context, value job.Job) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.createError != nil {
		return store.createError
	}
	store.created = append(store.created, value)
	return nil
}
func (store *fakeJobStore) CreateIdempotent(_ context.Context, value job.Job, key string, fingerprint []byte) (job.Job, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.createError != nil {
		return job.Job{}, false, store.createError
	}
	if store.idempotent == nil {
		store.idempotent = make(map[string]fakeIdempotentJob)
	}
	if existing, ok := store.idempotent[key]; ok {
		if !bytes.Equal(existing.fingerprint, fingerprint) {
			return job.Job{}, false, errFakeIdempotencyConflict
		}
		return existing.job, false, nil
	}
	store.created = append(store.created, value)
	store.idempotent[key] = fakeIdempotentJob{job: value, fingerprint: append([]byte(nil), fingerprint...)}
	return value, true, nil
}
func (store *fakeJobStore) GetByID(context.Context, job.JobID) (job.Job, error) {
	return store.loaded, store.getError
}
func (store *fakeJobStore) createdJob(t *testing.T) job.Job {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.created) != 1 {
		t.Fatalf("created jobs = %d, want 1", len(store.created))
	}
	return store.created[0]
}

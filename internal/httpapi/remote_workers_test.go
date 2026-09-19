package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/app"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/storage/postgres"
)

type remoteClock struct{ now time.Time }

func (c *remoteClock) Now() time.Time { return c.now }

type remoteTokens struct{}

func (remoteTokens) NewLeaseToken() (job.LeaseToken, error) { return "fence-1", nil }

// This fake delegates transitions to the real domain. Persistence races and
// fencing after recovery are covered separately against isolated PostgreSQL.
type remoteRepository struct {
	app.WorkerRepository
	value        job.Job
	calls        int
	err          error
	contextValue any
}

func (r *remoteRepository) ClaimNext(ctx context.Context, w job.WorkerID, token job.LeaseToken, now, expires time.Time, types ...job.TaskType) (job.Job, error) {
	r.calls++
	if r.err != nil {
		return job.Job{}, r.err
	}
	for _, typ := range types {
		if typ == r.value.TaskType {
			err := r.value.Claim(w, token, now, expires)
			return r.value, err
		}
	}
	return job.Job{}, postgres.ErrNoJobAvailable
}
func (r *remoteRepository) GetByID(ctx context.Context, id job.JobID) (job.Job, error) {
	r.calls++
	r.contextValue = ctx.Value(remoteContextKey{})
	if r.err != nil {
		return job.Job{}, r.err
	}
	if id != r.value.ID {
		return job.Job{}, postgres.ErrJobNotFound
	}
	return r.value, nil
}
func (r *remoteRepository) transition(id job.JobID, fn func() error) (job.Job, error) {
	r.calls++
	if id != r.value.ID {
		return job.Job{}, postgres.ErrJobNotFound
	}
	if err := fn(); err != nil {
		return job.Job{}, postgres.ErrLifecycleConflict
	}
	return r.value, nil
}
func (r *remoteRepository) StartExecution(_ context.Context, id job.JobID, w job.WorkerID, t job.LeaseToken, now time.Time) (job.Job, error) {
	return r.transition(id, func() error { return r.value.Start(w, t, now) })
}
func (r *remoteRepository) RenewLeaseBounded(_ context.Context, id job.JobID, w job.WorkerID, t job.LeaseToken, now, expiry time.Time, bound time.Duration) (job.Job, error) {
	return r.transition(id, func() error { return r.value.RenewLease(w, t, now, expiry) })
}
func (r *remoteRepository) CompleteExecution(_ context.Context, id job.JobID, w job.WorkerID, t job.LeaseToken, result json.RawMessage, now time.Time) (job.Job, error) {
	return r.transition(id, func() error { return r.value.Complete(w, t, now, result) })
}
func (r *remoteRepository) FailExecution(_ context.Context, id job.JobID, w job.WorkerID, t job.LeaseToken, now time.Time, message string, retry *time.Time) (job.Job, error) {
	return r.transition(id, func() error { return r.value.Fail(w, t, now, message, retry) })
}
func (r *remoteRepository) FailExecutionPermanently(_ context.Context, id job.JobID, w job.WorkerID, t job.LeaseToken, now time.Time, message string) (job.Job, error) {
	return r.transition(id, func() error { return r.value.FailPermanent(w, t, now, message) })
}

type remoteContextKey struct{}

func remoteFixture(t *testing.T) (http.Handler, *remoteRepository, *remoteClock) {
	t.Helper()
	clock := &remoteClock{time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)}
	value, err := job.New("job-1", "render", json.RawMessage(`{"frame":1}`), 3, clock.now, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	repo := &remoteRepository{value: value}
	h, err := NewWorkerHandler(app.NewWorkerService(repo, clock, remoteTokens{}), "test-credential", http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	return h, repo, clock
}
func workerRequest(h http.Handler, method, path, body, auth string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/v1/worker/jobs/"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	r = r.WithContext(context.WithValue(r.Context(), remoteContextKey{}, "propagated"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

const workerAuth = "Bearer test-credential"
const ownerBody = `{"worker_id":"worker-1","lease_token":"fence-1"}`

func TestWorkerAuthentication(t *testing.T) {
	for _, path := range []string{"claim", "job-1", "job-1/start", "job-1/heartbeat", "job-1/complete", "job-1/fail"} {
		for _, auth := range []string{"", "Bearer wrong", "Basic test-credential", "Bearer", "Bearer test-credential extra"} {
			t.Run(path+auth, func(t *testing.T) {
				h, repo, _ := remoteFixture(t)
				w := workerRequest(h, "POST", path, `{}`, auth)
				if w.Code != 401 || repo.calls != 0 {
					t.Fatalf("status/calls=%d/%d", w.Code, repo.calls)
				}
				if strings.Contains(w.Body.String(), "test-credential") {
					t.Fatal("credential leaked")
				}
			})
		}
	}
	h, repo, _ := remoteFixture(t)
	r := httptest.NewRequest("GET", "/v1/worker/jobs/job-1", nil)
	r.Header.Add("Authorization", workerAuth)
	r.Header.Add("Authorization", workerAuth)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 || repo.calls != 0 {
		t.Fatal("duplicate authorization accepted")
	}
	for _, token := range []string{"", " ", "contains space", "line\nbreak"} {
		if _, err := NewWorkerHandler(nil, token, http.NotFoundHandler()); err == nil {
			t.Fatal("invalid credential accepted")
		}
	}
}
func TestWorkerValidation(t *testing.T) {
	cases := []struct {
		name, path, body string
		status           int
	}{
		{"empty types", "claim", `{"worker_id":"w","supported_types":[]}`, 400},
		{"missing types", "claim", `{"worker_id":"w"}`, 400},
		{"blank type", "claim", `{"worker_id":"w","supported_types":[" "]}`, 400},
		{"blank worker", "claim", `{"worker_id":" ","supported_types":["render"]}`, 400},
		{"unknown field", "claim", `{"task_types":["render"]}`, 400},
		{"trailing value", "claim", `{} {}`, 400},
		{"too many types", "claim", `{"worker_id":"w","supported_types":[` + strings.Repeat(`"render",`, app.MaxSupportedTypes) + `"render"]}`, 400},
		{"oversized trailing whitespace", "claim", `{}` + strings.Repeat(" ", int(maxRequestBodyBytes)), 413},
		{"null", "claim", `null`, 400},
		{"array", "claim", `[]`, 400},
		{"missing ownership", "job-1/start", `{}`, 400},
		{"missing token", "job-1/heartbeat", `{"worker_id":"w"}`, 400},
		{"missing result", "job-1/complete", ownerBody, 400},
		{"invalid policy", "job-1/fail", `{"worker_id":"w","lease_token":"t","classification":"maybe","message":"failed"}`, 400},
		{"missing message", "job-1/fail", `{"worker_id":"w","lease_token":"t","classification":"retryable"}`, 400},
		{"too large", "claim", `{"worker_id":"` + strings.Repeat("x", int(maxRequestBodyBytes)) + `"}`, 413},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			h, repo, _ := remoteFixture(t)
			w := workerRequest(h, "POST", tt.path, tt.body, workerAuth)
			if w.Code != tt.status || repo.calls != 0 {
				t.Fatalf("status/calls=%d/%d body=%s", w.Code, repo.calls, w.Body)
			}
		})
	}
	h, _, _ := remoteFixture(t)
	r := httptest.NewRequest("POST", "/v1/worker/jobs/claim", strings.NewReader(`{}`))
	r.Header.Set("Authorization", workerAuth)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal(w.Code)
	}
}
func TestWorkerLifecycleAndReconciliation(t *testing.T) {
	for _, outcome := range []string{"complete", "retryable", "permanent"} {
		t.Run(outcome, func(t *testing.T) {
			h, repo, clock := remoteFixture(t)
			check := func(method, path, body string, status int) *httptest.ResponseRecorder {
				t.Helper()
				w := workerRequest(h, method, path, body, workerAuth)
				if w.Code != status {
					t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body)
				}
				return w
			}
			check("POST", "claim", `{"worker_id":"worker-1","supported_types":["other"]}`, 204)
			w := check("POST", "claim", `{"worker_id":"worker-1","supported_types":["render"]}`, 200)
			if !strings.Contains(w.Body.String(), `"token":"fence-1"`) || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("claim lease/cache headers missing")
			}
			check("POST", "job-1/start", ownerBody, 200)
			started := *repo.value.StartedAt
			clock.now = clock.now.Add(time.Second)
			check("POST", "job-1/start", ownerBody, 200)
			if repo.value.AttemptsStarted != 1 || !repo.value.StartedAt.Equal(started) {
				t.Fatal("start replay consumed attempt")
			}
			check("POST", "job-1/heartbeat", ownerBody, 200)
			check("POST", "job-1/complete", `{"worker_id":"worker-1","lease_token":"stale","result":null}`, 409)
			path, body := "job-1/complete", `{"worker_id":"worker-1","lease_token":"fence-1","result":{"ok":true}}`
			want := job.StateSucceeded
			if outcome != "complete" {
				path = "job-1/fail"
				body = `{"worker_id":"worker-1","lease_token":"fence-1","classification":"` + outcome + `","message":"task failed"}`
				want = job.StateFailed
				if outcome == "retryable" {
					want = job.StateRetryScheduled
				}
			}
			check("POST", path, body, 200)
			// Model a lost terminal response: inspection reveals that ownership ended.
			w = check("GET", "job-1", "", 200)
			if repo.value.State != want || repo.value.Lease != nil || !strings.Contains(w.Body.String(), `"lease":null`) {
				t.Fatalf("outcome=%s", w.Body)
			}
			check("POST", path, body, 409)
			check("GET", "missing", "", 404)
			if repo.contextValue != "propagated" {
				t.Fatal("request context lost")
			}
		})
	}
}
func TestWorkerSafeUnexpectedError(t *testing.T) {
	h, repo, _ := remoteFixture(t)
	repo.err = errors.New("database detail secret-token")
	w := workerRequest(h, "GET", "job-1", "", workerAuth)
	if w.Code != 500 || strings.Contains(w.Body.String(), "secret-token") {
		t.Fatalf("response=%s", w.Body)
	}
}

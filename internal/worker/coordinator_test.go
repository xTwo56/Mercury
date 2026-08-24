package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

var errNoJob = errors.New("no job")

func TestCoordinatorSuccessfulExecution(t *testing.T) {
	repository := &fakeRepository{jobs: []job.Job{workerJob("job-1", task.SleepTaskType)}}
	handler := &fakeHandler{result: json.RawMessage(`{"duration_ms":1}`), called: make(chan struct{}, 1)}
	coordinator, clock := testCoordinator(t, repository, handler, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	receiveSignal(t, handler.called)
	receiveSignal(t, repository.completed)
	cancel()
	if err := receiveError(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v", err)
	}
	if repository.startCalls != 1 || repository.completeCalls != 1 || repository.failCalls != 0 {
		t.Errorf("lifecycle calls start/complete/fail = %d/%d/%d", repository.startCalls, repository.completeCalls, repository.failCalls)
	}
	if len(repository.claimTokens) != 1 || repository.claimTokens[0] == "" {
		t.Error("claim did not use a lease token")
	}
	if clock.ticker == nil || !clock.ticker.stopped {
		t.Error("poll ticker was not stopped")
	}
}

func TestCoordinatorFailurePaths(t *testing.T) {
	tests := []struct {
		name          string
		taskType      job.TaskType
		handlerError  error
		startError    error
		completeError error
		failError     error
		wantHandler   int32
		wantComplete  int
		wantFail      int
	}{
		{name: "handler failure", taskType: task.SleepTaskType, handlerError: errors.New("execution failed"), wantHandler: 1, wantFail: 1},
		{name: "unknown task", taskType: job.TaskType("unknown"), wantFail: 1},
		{name: "start rejection", taskType: task.SleepTaskType, startError: errors.New("start rejected")},
		{name: "completion persistence failure", taskType: task.SleepTaskType, completeError: errors.New("completion failed"), wantHandler: 1, wantComplete: 1},
		{name: "failure persistence failure", taskType: task.SleepTaskType, handlerError: errors.New("execution failed"), failError: errors.New("failure report failed"), wantHandler: 1, wantFail: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &fakeRepository{jobs: []job.Job{workerJob("job-1", tt.taskType)}, startError: tt.startError, completeError: tt.completeError, failError: tt.failError}
			handler := &fakeHandler{result: json.RawMessage(`null`), err: tt.handlerError, called: make(chan struct{}, 1)}
			coordinator, _ := testCoordinator(t, repository, handler, 1)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- coordinator.Run(ctx) }()
			receiveSignal(t, repository.finished)
			cancel()
			receiveError(t, done)
			if atomic.LoadInt32(&handler.calls) != tt.wantHandler || repository.completeCalls != tt.wantComplete || repository.failCalls != tt.wantFail {
				t.Errorf("handler/complete/fail = %d/%d/%d, want %d/%d/%d", handler.calls, repository.completeCalls, repository.failCalls, tt.wantHandler, tt.wantComplete, tt.wantFail)
			}
		})
	}
}

func TestCoordinatorBoundedConcurrencyAndBackpressure(t *testing.T) {
	repository := &fakeRepository{jobs: []job.Job{workerJob("job-1", task.SleepTaskType), workerJob("job-2", task.SleepTaskType), workerJob("job-3", task.SleepTaskType)}}
	handler := &blockingHandler{entered: make(chan struct{}, 3), release: make(chan struct{}, 3)}
	coordinator, clock := testCoordinatorWithHandler(t, repository, handler, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	receiveSignal(t, handler.entered)
	receiveSignal(t, handler.entered)
	receiveSignal(t, clock.created)
	if repository.claimCount() != 2 {
		t.Fatalf("claims while two slots occupied = %d, want 2", repository.claimCount())
	}
	clock.tick()
	if repository.claimCount() != 2 {
		t.Errorf("claim occurred while slots full: %d", repository.claimCount())
	}
	handler.release <- struct{}{}
	receiveSignal(t, repository.completed)
	clock.tick()
	receiveSignal(t, handler.entered)
	if repository.claimCount() != 3 || atomic.LoadInt32(&handler.maxActive) != 2 {
		t.Errorf("claims/max active = %d/%d, want 3/2", repository.claimCount(), handler.maxActive)
	}
	handler.release <- struct{}{}
	handler.release <- struct{}{}
	receiveSignal(t, repository.completed)
	receiveSignal(t, repository.completed)
	cancel()
	receiveError(t, done)
	if len(repository.claimTokens) != 3 || repository.claimTokens[0] == repository.claimTokens[1] || repository.claimTokens[1] == repository.claimTokens[2] {
		t.Errorf("claim tokens are not fresh: %#v", repository.claimTokens)
	}
}

func TestCoordinatorIdleAndConfiguration(t *testing.T) {
	repository := &fakeRepository{}
	coordinator, clock := testCoordinator(t, repository, &fakeHandler{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	receiveSignal(t, clock.created)
	cancel()
	receiveError(t, done)
	if repository.claimCount() != 1 {
		t.Errorf("idle claim attempts = %d, want 1", repository.claimCount())
	}

	config := validWorkerConfig()
	config.Concurrency = 0
	if _, err := NewCoordinator(repository, task.NewHandlerRegistry(), clock, &fakeTokens{}, nil, config, func(error) bool { return false }); err == nil {
		t.Fatal("NewCoordinator() accepted zero concurrency")
	}
}

func testCoordinator(t *testing.T, repository *fakeRepository, handler *fakeHandler, concurrency int) (*Coordinator, *fakeClock) {
	return testCoordinatorWithHandler(t, repository, handler, concurrency)
}

func testCoordinatorWithHandler(t *testing.T, repository *fakeRepository, handler task.Handler, concurrency int) (*Coordinator, *fakeClock) {
	t.Helper()
	registry := task.NewHandlerRegistry()
	if err := registry.Register(task.SleepTaskType, handler); err != nil {
		t.Fatal(err)
	}
	registry.Seal()
	clock := &fakeClock{now: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)}
	clock.created = make(chan struct{}, 1)
	if repository.completed == nil {
		repository.completed = make(chan struct{}, 10)
	}
	if repository.finished == nil {
		repository.finished = make(chan struct{}, 10)
	}
	repository.claimed = make(map[job.JobID]job.Job)
	config := validWorkerConfig()
	config.Concurrency = concurrency
	coordinator, err := NewCoordinator(repository, registry, clock, &fakeTokens{}, slog.New(slog.NewTextHandler(io.Discard, nil)), config, func(err error) bool { return errors.Is(err, errNoJob) })
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, clock
}

func validWorkerConfig() Config {
	return Config{WorkerID: job.WorkerID("worker-1"), Concurrency: 1, PollInterval: time.Second, LeaseDuration: time.Hour, RetryDelay: time.Minute}
}
func workerJob(id string, taskType job.TaskType) job.Job {
	return job.Job{ID: job.JobID(id), TaskType: taskType, Payload: json.RawMessage(`{"duration_ms":1}`), State: job.StateLeased, MaxAttempts: 3}
}

type fakeRepository struct {
	mu                                   sync.Mutex
	jobs                                 []job.Job
	claimed                              map[job.JobID]job.Job
	claimTokens                          []job.LeaseToken
	startError, completeError, failError error
	startCalls, completeCalls, failCalls int
	completed                            chan struct{}
	finished                             chan struct{}
}

func (r *fakeRepository) ClaimNext(_ context.Context, _ job.WorkerID, token job.LeaseToken, _, _ time.Time) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimTokens = append(r.claimTokens, token)
	if len(r.jobs) == 0 {
		return job.Job{}, errNoJob
	}
	j := r.jobs[0]
	r.jobs = r.jobs[1:]
	r.claimed[j.ID] = j
	return j, nil
}
func (r *fakeRepository) StartExecution(_ context.Context, id job.JobID, _ job.WorkerID, _ job.LeaseToken, _ time.Time) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startCalls++
	if r.startError != nil {
		r.signalFinished()
		return job.Job{}, r.startError
	}
	return r.claimed[id], nil
}
func (r *fakeRepository) CompleteExecution(_ context.Context, id job.JobID, _ job.WorkerID, _ job.LeaseToken, result json.RawMessage, _ time.Time) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completeCalls++
	r.signalCompleted()
	r.signalFinished()
	return job.Job{ID: id, Result: result}, r.completeError
}
func (r *fakeRepository) FailExecution(_ context.Context, id job.JobID, _ job.WorkerID, _ job.LeaseToken, _ time.Time, _ string, _ *time.Time) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failCalls++
	r.signalFinished()
	return job.Job{ID: id}, r.failError
}
func (r *fakeRepository) signalCompleted() {
	if r.completed == nil {
		r.completed = make(chan struct{}, 10)
	}
	r.completed <- struct{}{}
}
func (r *fakeRepository) signalFinished() {
	if r.finished == nil {
		r.finished = make(chan struct{}, 10)
	}
	r.finished <- struct{}{}
}
func (r *fakeRepository) claimCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.claimTokens)
}

type fakeHandler struct {
	result json.RawMessage
	err    error
	calls  int32
	called chan struct{}
}

func (h *fakeHandler) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	atomic.AddInt32(&h.calls, 1)
	if h.called != nil {
		h.called <- struct{}{}
	}
	return h.result, h.err
}

type blockingHandler struct {
	entered, release  chan struct{}
	active, maxActive int32
}

func (h *blockingHandler) Execute(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	active := atomic.AddInt32(&h.active, 1)
	for {
		max := atomic.LoadInt32(&h.maxActive)
		if active <= max || atomic.CompareAndSwapInt32(&h.maxActive, max, active) {
			break
		}
	}
	h.entered <- struct{}{}
	select {
	case <-ctx.Done():
		atomic.AddInt32(&h.active, -1)
		return nil, ctx.Err()
	case <-h.release:
		atomic.AddInt32(&h.active, -1)
		return json.RawMessage(`null`), nil
	}
}

type fakeTokens struct {
	mu sync.Mutex
	n  int
}

func (g *fakeTokens) NewLeaseToken() (job.LeaseToken, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return job.LeaseToken(fmt.Sprintf("token-%d", g.n)), nil
}

type fakeClock struct {
	now     time.Time
	ticker  *fakeTicker
	created chan struct{}
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) NewTicker(time.Duration) Ticker {
	c.ticker = &fakeTicker{ticks: make(chan time.Time, 10)}
	c.created <- struct{}{}
	return c.ticker
}
func (c *fakeClock) tick() { c.ticker.ticks <- c.now }

type fakeTicker struct {
	ticks   chan time.Time
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ticks }
func (t *fakeTicker) Stop()               { t.stopped = true }
func receiveSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(time.Second):
		t.Fatal("timeout")
		return nil
	}
}

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

var (
	errNoJob      = errors.New("no job")
	errMissingJob = errors.New("missing job")
)

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
	if _, err := NewCoordinator(repository, task.NewHandlerRegistry(), clock, &fakeTokens{}, nil, config, func(error) bool { return false }, func(error) bool { return false }); err == nil {
		t.Fatal("NewCoordinator() accepted zero concurrency")
	}
}

func TestCoordinatorHeartbeatRenewalAndOwnershipLoss(t *testing.T) {
	tests := []struct {
		name          string
		renewErrors   []error
		secondTime    time.Time
		wantRenewals  int
		wantComplete  int
		wantCancelled bool
		definitive    bool
	}{
		{name: "successful renewal", secondTime: time.Date(2026, 8, 24, 12, 10, 0, 0, time.UTC), wantRenewals: 1, wantComplete: 1},
		{name: "transient then success", renewErrors: []error{errors.New("temporary"), nil}, secondTime: time.Date(2026, 8, 24, 12, 20, 0, 0, time.UTC), wantRenewals: 2, wantComplete: 1},
		{name: "wrong worker", renewErrors: []error{job.ErrLeaseWorkerMismatch}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "wrong token", renewErrors: []error{job.ErrLeaseTokenMismatch}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "expired lease", renewErrors: []error{job.ErrLeaseExpired}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "invalid state", renewErrors: []error{job.ErrJobNotRunning}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "missing lease", renewErrors: []error{job.ErrLeaseMissing}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "missing job", renewErrors: []error{errMissingJob}, wantRenewals: 1, wantCancelled: true, definitive: true},
		{name: "transient until expiration", renewErrors: []error{errors.New("temporary")}, secondTime: time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC), wantRenewals: 1, wantCancelled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &fakeRepository{jobs: []job.Job{workerJob("job-1", task.SleepTaskType)}, renewErrors: append([]error(nil), tt.renewErrors...), renewed: make(chan struct{}, 4)}
			handler := &blockingHandler{entered: make(chan struct{}, 1), release: make(chan struct{}, 1), exited: make(chan struct{}, 1)}
			coordinator, clock := testCoordinatorWithHandler(t, repository, handler, 1)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- coordinator.Run(ctx) }()
			receiveSignal(t, handler.entered)
			heartbeatTicker := receiveTicker(t, clock.heartbeatCreated)

			clock.setNow(time.Date(2026, 8, 24, 12, 10, 0, 0, time.UTC))
			heartbeatTicker.ticks <- clock.Now()
			if len(tt.renewErrors) > 0 && !tt.definitive {
				receiveSignal(t, repository.renewed)
				clock.setNow(tt.secondTime)
				heartbeatTicker.ticks <- clock.Now()
			}
			if tt.wantCancelled {
				receiveSignal(t, handler.exited)
			} else {
				receiveSignal(t, repository.renewed)
				handler.release <- struct{}{}
				receiveSignal(t, repository.completed)
			}
			cancel()
			receiveError(t, done)
			if repository.renewCount() != tt.wantRenewals || repository.completeCalls != tt.wantComplete || repository.failCalls != 0 {
				t.Errorf("renew/complete/fail = %d/%d/%d, want %d/%d/0", repository.renewCount(), repository.completeCalls, repository.failCalls, tt.wantRenewals, tt.wantComplete)
			}
			if !heartbeatTicker.stopped {
				t.Error("heartbeat ticker was not stopped")
			}
			for _, call := range repository.renewCalls {
				if call.workerID != job.WorkerID("worker-1") || call.token != repository.claimTokens[0] {
					t.Errorf("renewal identifiers changed: %#v", call)
				}
			}
		})
	}
}

func TestCoordinatorHeartbeatCancellationDuringRenewal(t *testing.T) {
	repository := &fakeRepository{
		jobs:         []job.Job{workerJob("job-1", task.SleepTaskType)},
		renewStarted: make(chan struct{}, 1),
		renewExited:  make(chan struct{}, 1),
	}
	handler := &blockingHandler{entered: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	coordinator, clock := testCoordinatorWithHandler(t, repository, handler, 1)
	var logs bytes.Buffer
	coordinator.logger = slog.New(slog.NewTextHandler(&logs, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	receiveSignal(t, handler.entered)
	heartbeatTicker := receiveTicker(t, clock.heartbeatCreated)
	clock.setNow(time.Date(2026, 8, 24, 12, 10, 0, 0, time.UTC))
	heartbeatTicker.ticks <- clock.Now()
	receiveSignal(t, repository.renewStarted)

	// Handler completion cancels the heartbeat while its repository call is
	// blocked. The fake returns the same wrapped cancellation as pgx would.
	handler.release <- struct{}{}
	receiveSignal(t, repository.renewExited)
	receiveSignal(t, repository.completed)
	cancel()
	receiveError(t, done)

	if repository.renewCount() != 1 || repository.completeCalls != 1 || repository.failCalls != 0 {
		t.Errorf("renew/complete/fail = %d/%d/%d, want 1/1/0", repository.renewCount(), repository.completeCalls, repository.failCalls)
	}
	if repository.completeContextErr != nil {
		t.Errorf("terminal persistence context error = %v", repository.completeContextErr)
	}
	if strings.Contains(logs.String(), "renew job lease") {
		t.Errorf("intentional heartbeat cancellation logged as renewal failure: %s", logs.String())
	}
	if !heartbeatTicker.stopped {
		t.Error("heartbeat ticker was not stopped")
	}
	heartbeatTicker.ticks <- clock.Now()
	if repository.renewCount() != 1 {
		t.Errorf("renewals after terminal persistence = %d, want 1", repository.renewCount())
	}
}

func TestCoordinatorLogsGenuineHeartbeatFailure(t *testing.T) {
	repository := &fakeRepository{jobs: []job.Job{workerJob("job-1", task.SleepTaskType)}, renewErrors: []error{errors.New("database unavailable")}, renewed: make(chan struct{}, 1)}
	handler := &blockingHandler{entered: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	coordinator, clock := testCoordinatorWithHandler(t, repository, handler, 1)
	var logs bytes.Buffer
	coordinator.logger = slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	receiveSignal(t, handler.entered)
	heartbeatTicker := receiveTicker(t, clock.heartbeatCreated)
	clock.setNow(time.Date(2026, 8, 24, 12, 10, 0, 0, time.UTC))
	heartbeatTicker.ticks <- clock.Now()
	receiveSignal(t, repository.renewed)
	handler.release <- struct{}{}
	receiveSignal(t, repository.completed)
	cancel()
	receiveError(t, done)
	if !strings.Contains(logs.String(), "renew job lease") || !strings.Contains(logs.String(), "database unavailable") {
		t.Errorf("genuine renewal failure was not logged: %s", logs.String())
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
	clock := &fakeClock{now: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), heartbeatCreated: make(chan *fakeTicker, 10)}
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
	coordinator, err := NewCoordinator(repository, registry, clock, &fakeTokens{}, slog.New(slog.NewTextHandler(io.Discard, nil)), config, func(err error) bool { return errors.Is(err, errNoJob) }, isTestOwnershipLoss)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, clock
}

func isTestOwnershipLoss(err error) bool {
	return errors.Is(err, errMissingJob) || errors.Is(err, job.ErrJobNotRunning) ||
		errors.Is(err, job.ErrLeaseMissing) || errors.Is(err, job.ErrLeaseWorkerMismatch) ||
		errors.Is(err, job.ErrLeaseTokenMismatch) || errors.Is(err, job.ErrLeaseExpired)
}

func validWorkerConfig() Config {
	return Config{WorkerID: job.WorkerID("worker-1"), Concurrency: 1, PollInterval: time.Second, LeaseDuration: time.Hour, RetryDelay: time.Minute, HeartbeatInterval: 10 * time.Minute}
}
func workerJob(id string, taskType job.TaskType) job.Job {
	return job.Job{ID: job.JobID(id), TaskType: taskType, Payload: json.RawMessage(`{"duration_ms":1}`), State: job.StateLeased, MaxAttempts: 3, Lease: &job.Lease{WorkerID: job.WorkerID("worker-1"), Token: job.LeaseToken("token"), ExpiresAt: time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC)}}
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
	renewErrors                          []error
	renewCalls                           []renewCall
	renewed                              chan struct{}
	renewStarted                         chan struct{}
	renewExited                          chan struct{}
	completeContextErr                   error
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
func (r *fakeRepository) CompleteExecution(ctx context.Context, id job.JobID, _ job.WorkerID, _ job.LeaseToken, result json.RawMessage, _ time.Time) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completeCalls++
	r.completeContextErr = ctx.Err()
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

type renewCall struct {
	workerID       job.WorkerID
	token          job.LeaseToken
	now, expiresAt time.Time
}

func (r *fakeRepository) RenewLease(ctx context.Context, id job.JobID, workerID job.WorkerID, token job.LeaseToken, now time.Time, expiresAt time.Time) (job.Job, error) {
	r.mu.Lock()
	r.renewCalls = append(r.renewCalls, renewCall{workerID: workerID, token: token, now: now, expiresAt: expiresAt})
	var err error
	if len(r.renewErrors) > 0 {
		err = r.renewErrors[0]
		r.renewErrors = r.renewErrors[1:]
	}
	renewedSignal := r.renewed
	startedSignal := r.renewStarted
	exitedSignal := r.renewExited
	r.mu.Unlock()
	if startedSignal != nil {
		startedSignal <- struct{}{}
		<-ctx.Done()
		err = fmt.Errorf("begin renewal transaction: %w", ctx.Err())
	}
	if exitedSignal != nil {
		exitedSignal <- struct{}{}
	}
	if renewedSignal != nil {
		renewedSignal <- struct{}{}
	}
	if err != nil {
		return job.Job{}, err
	}
	return job.Job{ID: id, Lease: &job.Lease{WorkerID: workerID, Token: token, ExpiresAt: expiresAt}}, nil
}
func (r *fakeRepository) renewCount() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.renewCalls) }
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
	entered, release, exited chan struct{}
	active, maxActive        int32
}

func (h *blockingHandler) Execute(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	if h.exited != nil {
		defer func() { h.exited <- struct{}{} }()
	}
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
	mu               sync.Mutex
	now              time.Time
	ticker           *fakeTicker
	created          chan struct{}
	heartbeatCreated chan *fakeTicker
}

func (c *fakeClock) Now() time.Time       { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) setNow(now time.Time) { c.mu.Lock(); c.now = now; c.mu.Unlock() }
func (c *fakeClock) NewTicker(interval time.Duration) Ticker {
	ticker := &fakeTicker{ticks: make(chan time.Time, 10)}
	if interval == time.Second {
		c.mu.Lock()
		c.ticker = ticker
		c.mu.Unlock()
		c.created <- struct{}{}
	} else if c.heartbeatCreated != nil {
		c.heartbeatCreated <- ticker
	}
	return ticker
}
func (c *fakeClock) tick() {
	c.mu.Lock()
	ticker := c.ticker
	now := c.now
	c.mu.Unlock()
	ticker.ticks <- now
}

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
func receiveTicker(t *testing.T, ch <-chan *fakeTicker) *fakeTicker {
	t.Helper()
	select {
	case ticker := <-ch:
		return ticker
	case <-time.After(time.Second):
		t.Fatal("timeout")
		return nil
	}
}

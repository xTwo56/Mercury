package remoteworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

func TestRuntimeClaimsRegisteredTypesStartsAndCompletes(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC))
	lifecycle := newFakeLifecycle(clock.Now())
	registry := NewRegistry()
	handled := make(chan struct{}, 1)
	if err := registry.Register("render", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		handled <- struct{}{}
		return json.RawMessage(`{"ok":true}`), nil
	})); err != nil {
		t.Fatal(err)
	}
	runtime := newTestRuntime(t, lifecycle, registry, clock, Config{
		WorkerID: "worker-1", Concurrency: 1, PollInterval: time.Second,
		HeartbeatInterval: 20 * time.Second, UncertainClaimHold: time.Minute, ShutdownTimeout: time.Second,
	})
	cancel, done := runRuntime(runtime)
	waitSignal(t, handled)
	waitSignal(t, lifecycle.completed)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if !reflect.DeepEqual(lifecycle.claimTypes[0], []workerclient.TaskType{"render"}) {
		t.Fatalf("claim types = %v", lifecycle.claimTypes)
	}
	if lifecycle.startCalls != 1 || lifecycle.completeCalls != 1 || lifecycle.failCalls != 0 {
		t.Fatalf("start/complete/fail = %d/%d/%d", lifecycle.startCalls, lifecycle.completeCalls, lifecycle.failCalls)
	}
}

func TestRuntimeDoesNotExecuteBeforeConfirmedStart(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.startErr = workerclient.ErrOwnershipLost
	registry := NewRegistry()
	var calls atomic.Int32
	_ = registry.Register("render", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		return json.RawMessage(`null`), nil
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, lifecycle.started)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran after rejected start")
	}
}

func TestRuntimeConcurrencyAndBackpressure(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.enqueue(lifecycleJob("job-2", clock.Now()))
	lifecycle.enqueue(lifecycleJob("job-3", clock.Now()))
	registry := NewRegistry()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	_ = registry.Register("render", HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return json.RawMessage(`null`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(2))
	cancel, done := runRuntime(runtime)

	// Simply: wait until both jobs are claimed, started, and inside the handler.
	// More precisely: these barriers prove two independent executions can occupy
	// both slots before the backpressure assertion examines another fill attempt.
	waitSignal(t, lifecycle.claimed)
	waitSignal(t, lifecycle.claimed)
	waitSignal(t, lifecycle.started)
	waitSignal(t, lifecycle.started)
	waitSignal(t, entered)
	waitSignal(t, entered)
	runtime.fillSlots(context.Background(), []workerclient.TaskType{"render"})
	lifecycle.mu.Lock()
	claimsWhileFull := lifecycle.claimCalls
	lifecycle.mu.Unlock()
	if claimsWhileFull != 2 {
		t.Fatalf("claims while full = %d, want 2", claimsWhileFull)
	}
	close(release)
	waitSignal(t, lifecycle.completed)
	waitSignal(t, lifecycle.completed)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	for _, id := range []workerclient.JobID{"job-1", "job-2"} {
		if got := lifecycle.jobsByID[id].State; got != workerclient.StateSucceeded {
			t.Fatalf("job %s state = %q, want %q", id, got, workerclient.StateSucceeded)
		}
	}
	if got := lifecycle.jobsByID["job-3"].State; got != workerclient.StateLeased {
		t.Fatalf("unclaimed job state = %q, want %q", got, workerclient.StateLeased)
	}
}

func TestRuntimeRenewsWithStableOwnershipBeforeCompletion(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	registry := NewRegistry()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	_ = registry.Register("render", HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return json.RawMessage(`null`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, entered)
	waitSignal(t, clock.tickerCreated)
	waitSignal(t, clock.tickerCreated)
	clock.Advance(20 * time.Second)
	waitSignal(t, lifecycle.heartbeat)
	close(release)
	waitSignal(t, lifecycle.completed)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.heartbeatCalls != 1 || len(lifecycle.heartbeatTokens) != 1 || lifecycle.heartbeatTokens[0] != "token-job-1" {
		t.Fatalf("heartbeat calls/tokens = %d/%v", lifecycle.heartbeatCalls, lifecycle.heartbeatTokens)
	}
}

func TestRuntimeHeartbeatsAndCancelsOnOwnershipLoss(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.heartbeatErr = workerclient.ErrOwnershipLost
	registry := NewRegistry()
	handlerCancelled := make(chan struct{}, 1)
	handlerEntered := make(chan struct{}, 1)
	_ = registry.Register("render", HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		handlerEntered <- struct{}{}
		<-ctx.Done()
		handlerCancelled <- struct{}{}
		return nil, ctx.Err()
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, handlerEntered)
	waitSignal(t, clock.tickerCreated)
	waitSignal(t, clock.tickerCreated)
	clock.Advance(20 * time.Second)
	waitSignal(t, lifecycle.heartbeat)
	waitSignal(t, handlerCancelled)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.completeCalls != 0 || lifecycle.failCalls != 0 {
		t.Fatal("runtime reported an outcome after ownership loss")
	}
}

func TestRuntimeTransientHeartbeatFailureRunsOnlyToConfirmedExpiry(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.heartbeatErr = uncertainError("heartbeat")
	registry := NewRegistry()
	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	_ = registry.Register("render", HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, entered)
	waitSignal(t, clock.tickerCreated)
	waitSignal(t, clock.tickerCreated)
	clock.Advance(20 * time.Second)
	waitSignal(t, lifecycle.heartbeat)
	select {
	case <-cancelled:
		t.Fatal("transient renewal failure cancelled a still-owned execution")
	default:
	}
	clock.Advance(40 * time.Second)
	waitSignal(t, cancelled)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.completeCalls != 0 || lifecycle.failCalls != 0 {
		t.Fatal("runtime reported after confirmed lease expiry")
	}
}

func TestRuntimeExecutionDeadlineCancelsHandler(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	deadline := clock.Now().Add(time.Minute)
	lifecycle.executionDeadline = &deadline
	registry := NewRegistry()
	cancelled := make(chan struct{}, 1)
	entered := make(chan struct{}, 1)
	_ = registry.Register("render", HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
			t.Errorf("handler deadline = %v, %v; want %v", got, ok, deadline)
		}
		entered <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, entered)
	waitSignal(t, clock.timerCreated)
	clock.Advance(time.Minute)
	waitSignal(t, cancelled)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.completeCalls != 0 || lifecycle.failCalls != 0 {
		t.Fatal("deadline cancellation produced a terminal report")
	}
}

func TestRuntimeReconcilesUncertainStartAndDoesNotReplayOutcome(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.startErr = uncertainError("start")
	lifecycle.completeErr = uncertainError("complete")
	registry := NewRegistry()
	_ = registry.Register("render", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}))
	runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
	cancel, done := runRuntime(runtime)
	waitSignal(t, lifecycle.completed)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.startCalls != 1 || lifecycle.completeCalls != 1 || lifecycle.inspectCalls < 2 {
		t.Fatalf("start/complete/inspect = %d/%d/%d", lifecycle.startCalls, lifecycle.completeCalls, lifecycle.inspectCalls)
	}
}

func TestRuntimeReportsRetryablePermanentAndPanicFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler Handler
		want    workerclient.FailureClassification
	}{
		{name: "retryable default", handler: HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, errors.New("temporary") }), want: workerclient.FailureRetryable},
		{name: "permanent", handler: HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return nil, Permanent(errors.New("bad input"))
		}), want: workerclient.FailurePermanent},
		{name: "panic", handler: HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { panic("secret panic value") }), want: workerclient.FailurePermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newManualClock(time.Now().UTC())
			lifecycle := newFakeLifecycle(clock.Now())
			registry := NewRegistry()
			_ = registry.Register("render", test.handler)
			runtime := newTestRuntime(t, lifecycle, registry, clock, testConfig(1))
			cancel, done := runRuntime(runtime)
			waitSignal(t, lifecycle.failed)
			cancel()
			if err := waitError(t, done); err != nil {
				t.Fatal(err)
			}
			lifecycle.mu.Lock()
			defer lifecycle.mu.Unlock()
			if lifecycle.lastFailure != test.want {
				t.Fatalf("classification = %q, want %q", lifecycle.lastFailure, test.want)
			}
			if test.name == "panic" && lifecycle.lastMessage != errHandlerPanic.Error() {
				t.Fatalf("panic message = %q", lifecycle.lastMessage)
			}
		})
	}
}

func TestRuntimeUncertainClaimRetainsCapacity(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	lifecycle.claimErr = uncertainError("claim")
	registry := NewRegistry()
	_ = registry.Register("render", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`null`), nil }))
	config := testConfig(1)
	config.UncertainClaimHold = time.Minute
	runtime := newTestRuntime(t, lifecycle, registry, clock, config)
	cancel, done := runRuntime(runtime)
	waitSignal(t, lifecycle.claimed)
	waitSignal(t, clock.tickerCreated)

	// Simply: do not move time until the uncertain claim's hold timer exists.
	// More precisely: synchronizing timer creation prevents a race where Advance
	// ran first and moved the timer's eventual deadline forward by another minute.
	waitSignal(t, clock.timerCreated)
	clock.Advance(time.Second)
	lifecycle.mu.Lock()
	claims := lifecycle.claimCalls
	lifecycle.mu.Unlock()
	if claims != 1 {
		t.Fatalf("claims before possible lease expiry = %d", claims)
	}
	lifecycle.mu.Lock()
	lifecycle.claimErr = workerclient.ErrNoJobAvailable
	lifecycle.mu.Unlock()
	clock.Advance(time.Minute)
	waitCondition(t, func() bool { return len(runtime.slots) == 0 })
	clock.Advance(time.Second)
	waitSignal(t, lifecycle.claimed)
	cancel()
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBoundedShutdownCannotForceHandlerExit(t *testing.T) {
	clock := newManualClock(time.Now().UTC())
	lifecycle := newFakeLifecycle(clock.Now())
	registry := NewRegistry()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	_ = registry.Register("render", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		<-release
		return json.RawMessage(`null`), nil
	}))
	config := testConfig(1)
	config.ShutdownTimeout = time.Second
	runtime := newTestRuntime(t, lifecycle, registry, clock, config)
	cancel, done := runRuntime(runtime)
	waitSignal(t, entered)
	waitSignal(t, clock.timerCreated)
	cancel()
	waitSignal(t, clock.timerCreated)
	clock.Advance(time.Second)
	if err := waitError(t, done); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Run() error = %v", err)
	}
	if len(runtime.slots) != 1 {
		t.Fatal("execution capacity released before handler exited")
	}
	close(release)
	waitCondition(t, func() bool { return len(runtime.slots) == 0 })
}

func testConfig(concurrency int) Config {
	return Config{
		WorkerID: "worker-1", Concurrency: concurrency, PollInterval: time.Second,
		HeartbeatInterval: 20 * time.Second, UncertainClaimHold: time.Minute, ShutdownTimeout: time.Second,
	}
}

func newTestRuntime(t *testing.T, lifecycle LifecycleClient, registry *Registry, clock runtimeClock, config Config) *Runtime {
	t.Helper()
	runtime, err := newRuntime(lifecycle, registry, config, slog.New(slog.NewTextHandler(io.Discard, nil)), clock)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func runRuntime(runtime *Runtime) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	return cancel, done
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for test signal")
	}
}

func waitError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runtime")
		return nil
	}
}

func waitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for test condition")
		default:
			goruntime.Gosched()
		}
	}
}

func uncertainError(operation string) error {
	return &workerclient.OperationError{Operation: operation, Kind: workerclient.ErrTransport, Uncertain: true}
}

type fakeLifecycle struct {
	mu                sync.Mutex
	jobs              []workerclient.Job
	claimErr          error
	startErr          error
	heartbeatErr      error
	completeErr       error
	executionDeadline *time.Time
	claimCalls        int
	startCalls        int
	heartbeatCalls    int
	completeCalls     int
	failCalls         int
	inspectCalls      int
	claimTypes        [][]workerclient.TaskType
	jobsByID          map[workerclient.JobID]workerclient.Job
	lastFailure       workerclient.FailureClassification
	lastMessage       string
	heartbeatTokens   []workerclient.LeaseToken
	claimed, started  chan struct{}
	heartbeat         chan struct{}
	completed, failed chan struct{}
}

func newFakeLifecycle(now time.Time) *fakeLifecycle {
	job := lifecycleJob("job-1", now)
	deadline := now.Add(24 * time.Hour)
	return &fakeLifecycle{
		jobs: []workerclient.Job{job}, jobsByID: map[workerclient.JobID]workerclient.Job{job.ID: job}, executionDeadline: &deadline,
		claimed: make(chan struct{}, 16), started: make(chan struct{}, 16), heartbeat: make(chan struct{}, 16),
		completed: make(chan struct{}, 16), failed: make(chan struct{}, 16),
	}
}

func (fake *fakeLifecycle) enqueue(job workerclient.Job) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.jobs = append(fake.jobs, job)
	fake.jobsByID[job.ID] = job
}

func lifecycleJob(id workerclient.JobID, now time.Time) workerclient.Job {
	return workerclient.Job{
		ID: id, TaskType: "render", Payload: json.RawMessage(`{"frame":1}`), State: workerclient.StateLeased,
		Lease: &workerclient.Lease{WorkerID: "worker-1", Token: workerclient.LeaseToken("token-" + string(id)), ExpiresAt: now.Add(time.Minute)},
	}
}

func (fake *fakeLifecycle) Claim(_ context.Context, workerID workerclient.WorkerID, types []workerclient.TaskType) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.claimCalls++
	fake.claimTypes = append(fake.claimTypes, append([]workerclient.TaskType(nil), types...))
	fake.claimed <- struct{}{}
	if fake.claimErr != nil {
		return workerclient.Job{}, fake.claimErr
	}
	for index, job := range fake.jobs {
		if !containsTaskType(types, job.TaskType) {
			continue
		}
		if job.Lease == nil || job.Lease.WorkerID != workerID {
			return workerclient.Job{}, workerclient.ErrOwnershipLost
		}
		fake.jobs = append(fake.jobs[:index], fake.jobs[index+1:]...)
		return job, nil
	}
	return workerclient.Job{}, workerclient.ErrNoJobAvailable
}

func (fake *fakeLifecycle) Start(_ context.Context, id workerclient.JobID, workerID workerclient.WorkerID, token workerclient.LeaseToken) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.startCalls++
	fake.started <- struct{}{}
	job, err := fake.ownedJob(id, workerID, token)
	if err != nil {
		return workerclient.Job{}, err
	}
	if job.State != workerclient.StateLeased && job.State != workerclient.StateRunning {
		return workerclient.Job{}, workerclient.ErrOwnershipLost
	}
	if fake.startErr != nil && !workerclient.OutcomeUncertain(fake.startErr) {
		return workerclient.Job{}, fake.startErr
	}
	job.State = workerclient.StateRunning
	job.ExecutionDeadline = fake.executionDeadline
	fake.jobsByID[id] = job
	return job, fake.startErr
}

func (fake *fakeLifecycle) Heartbeat(_ context.Context, id workerclient.JobID, workerID workerclient.WorkerID, token workerclient.LeaseToken) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.heartbeatCalls++
	fake.heartbeatTokens = append(fake.heartbeatTokens, token)
	fake.heartbeat <- struct{}{}
	job, err := fake.ownedJob(id, workerID, token)
	if err != nil || job.State != workerclient.StateRunning {
		return workerclient.Job{}, workerclient.ErrOwnershipLost
	}
	if fake.heartbeatErr != nil {
		return workerclient.Job{}, fake.heartbeatErr
	}
	job.Lease.ExpiresAt = job.Lease.ExpiresAt.Add(time.Minute)
	fake.jobsByID[id] = job
	return job, nil
}

func (fake *fakeLifecycle) Complete(_ context.Context, id workerclient.JobID, workerID workerclient.WorkerID, token workerclient.LeaseToken, _ json.RawMessage) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.completeCalls++
	fake.completed <- struct{}{}
	job, err := fake.ownedJob(id, workerID, token)
	if err != nil || job.State != workerclient.StateRunning {
		return workerclient.Job{}, workerclient.ErrOwnershipLost
	}
	if fake.completeErr != nil && !workerclient.OutcomeUncertain(fake.completeErr) {
		return workerclient.Job{}, fake.completeErr
	}
	job.State = workerclient.StateSucceeded
	job.Lease = nil
	fake.jobsByID[id] = job
	return job, fake.completeErr
}

func (fake *fakeLifecycle) Fail(_ context.Context, id workerclient.JobID, workerID workerclient.WorkerID, token workerclient.LeaseToken, classification workerclient.FailureClassification, message string) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.failCalls++
	job, err := fake.ownedJob(id, workerID, token)
	if err != nil || job.State != workerclient.StateRunning {
		return workerclient.Job{}, workerclient.ErrOwnershipLost
	}
	fake.lastFailure = classification
	fake.lastMessage = message
	job.State = workerclient.StateFailed
	job.Lease = nil
	fake.jobsByID[id] = job
	fake.failed <- struct{}{}
	return job, nil
}

func (fake *fakeLifecycle) Inspect(_ context.Context, id workerclient.JobID) (workerclient.Job, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.inspectCalls++
	job, ok := fake.jobsByID[id]
	if !ok {
		return workerclient.Job{}, workerclient.ErrJobNotFound
	}
	return job, nil
}

func (fake *fakeLifecycle) ownedJob(id workerclient.JobID, workerID workerclient.WorkerID, token workerclient.LeaseToken) (workerclient.Job, error) {
	job, ok := fake.jobsByID[id]
	if !ok || job.Lease == nil || job.Lease.WorkerID != workerID || job.Lease.Token != token {
		return workerclient.Job{}, workerclient.ErrOwnershipLost
	}
	return job, nil
}

func containsTaskType(types []workerclient.TaskType, target workerclient.TaskType) bool {
	for _, taskType := range types {
		if taskType == target {
			return true
		}
	}
	return false
}

type manualClock struct {
	mu            sync.Mutex
	now           time.Time
	tickers       []*manualTicker
	timers        []*manualTimer
	timerCreated  chan struct{}
	tickerCreated chan struct{}
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, timerCreated: make(chan struct{}, 16), tickerCreated: make(chan struct{}, 16)}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTicker(time.Duration) runtimeTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	ticker := &manualTicker{clock: clock, channel: make(chan time.Time, 16)}
	clock.tickers = append(clock.tickers, ticker)
	clock.tickerCreated <- struct{}{}
	return ticker
}

func (clock *manualClock) NewTimer(duration time.Duration) runtimeTimer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{clock: clock, channel: make(chan time.Time, 1), deadline: clock.now.Add(duration)}
	clock.timers = append(clock.timers, timer)
	clock.timerCreated <- struct{}{}
	if duration <= 0 {
		timer.channel <- clock.now
		timer.fired = true
	}
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	for _, ticker := range clock.tickers {
		if !ticker.stopped {
			select {
			case ticker.channel <- clock.now:
			default:
			}
		}
	}
	for _, timer := range clock.timers {
		if !timer.stopped && !timer.fired && !timer.deadline.After(clock.now) {
			timer.channel <- clock.now
			timer.fired = true
		}
	}
}

type manualTicker struct {
	clock   *manualClock
	channel chan time.Time
	stopped bool
}

func (ticker *manualTicker) C() <-chan time.Time { return ticker.channel }
func (ticker *manualTicker) Stop() {
	ticker.clock.mu.Lock()
	ticker.stopped = true
	ticker.clock.mu.Unlock()
}

type manualTimer struct {
	clock    *manualClock
	channel  chan time.Time
	deadline time.Time
	stopped  bool
	fired    bool
}

func (timer *manualTimer) C() <-chan time.Time { return timer.channel }
func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	timer.stopped = true
	return !timer.fired
}

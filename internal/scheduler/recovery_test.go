package scheduler

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/job"
)

func TestRecoveryServiceSweepOnce(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.FixedZone("test", 5*60*60+30*60))
	repository := &fakeRecoveryRepository{
		results: [][]job.Job{{
			{ID: job.JobID("queued"), State: job.StateQueued},
			{ID: job.JobID("retry"), State: job.StateRetryScheduled},
			{ID: job.JobID("failed"), State: job.StateFailed},
		}},
	}
	service := newTestRecoveryService(t, repository, newFakeClock(now), &fakeLogger{}, RecoveryConfig{
		SweepInterval: time.Minute,
		RetryDelay:    5 * time.Minute,
		BatchSize:     17,
	})

	summary, err := service.SweepOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("SweepOnce() error = %v", err)
	}
	wantSummary := RecoverySummary{Recovered: 3, Queued: 1, RetryScheduled: 1, Failed: 1}
	if summary != wantSummary {
		t.Errorf("SweepOnce() summary = %#v, want %#v", summary, wantSummary)
	}
	calls := repository.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("repository calls = %d, want 1", len(calls))
	}
	wantCall := recoveryCall{now: now, retryAt: now.Add(5 * time.Minute), batchSize: 17}
	if !reflect.DeepEqual(calls[0], wantCall) {
		t.Errorf("repository call = %#v, want %#v", calls[0], wantCall)
	}
}

func TestRecoveryServiceRunImmediateAndPeriodic(t *testing.T) {
	initial := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	periodic := initial.Add(time.Minute)
	clock := newFakeClock(initial)
	repository := &fakeRecoveryRepository{called: make(chan recoveryCall, 2)}
	logger := &fakeLogger{}
	service := newTestRecoveryService(t, repository, clock, logger, RecoveryConfig{
		SweepInterval: time.Minute,
		RetryDelay:    2 * time.Minute,
		BatchSize:     4,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()

	first := receive(t, repository.called)
	if !first.now.Equal(initial) || first.batchSize != 4 {
		t.Errorf("immediate call = %#v, want time %v and batch 4", first, initial)
	}
	ticker := receive(t, clock.created)
	if ticker.interval != time.Minute {
		t.Errorf("ticker interval = %v, want %v", ticker.interval, time.Minute)
	}
	clock.setNow(periodic)
	ticker.tick(periodic)
	second := receive(t, repository.called)
	if !second.now.Equal(periodic) || !second.retryAt.Equal(periodic.Add(2*time.Minute)) {
		t.Errorf("periodic call = %#v, want deterministic time %v", second, periodic)
	}

	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
	if !ticker.isStopped() {
		t.Error("Run() did not stop ticker")
	}
}

func TestRecoveryServiceRunContinuesAfterError(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	repository := &fakeRecoveryRepository{
		errors: []error{errors.New("database unavailable"), nil},
		called: make(chan recoveryCall, 2),
	}
	logger := &fakeLogger{errors: make(chan logRecord, 1)}
	service := newTestRecoveryService(t, repository, clock, logger, RecoveryConfig{
		SweepInterval: time.Minute,
		RetryDelay:    time.Minute,
		BatchSize:     2,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()

	receive(t, repository.called)
	record := receive(t, logger.errors)
	if record.message != "expired lease recovery sweep failed" {
		t.Errorf("logged message = %q, want sweep failure", record.message)
	}
	ticker := receive(t, clock.created)
	clock.setNow(now.Add(time.Minute))
	ticker.tick(now.Add(time.Minute))
	receive(t, repository.called)
	if len(repository.snapshotCalls()) != 2 {
		t.Fatalf("repository calls = %d, want continuation call", len(repository.snapshotCalls()))
	}

	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
}

func TestRecoveryServiceDoesNotOverlapSweeps(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	repository := &fakeRecoveryRepository{hook: func(ctx context.Context, _ recoveryCall) error {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}}
	service := newTestRecoveryService(t, repository, newFakeClock(now), &fakeLogger{}, validRecoveryConfig())

	done := make(chan error, 2)
	go func() {
		_, err := service.SweepOnce(context.Background(), now)
		done <- err
	}()
	receive(t, entered)
	go func() {
		_, err := service.SweepOnce(context.Background(), now.Add(time.Second))
		done <- err
	}()

	select {
	case <-entered:
		t.Fatal("second sweep entered repository while first was active")
	default:
	}
	release <- struct{}{}
	if err := receive(t, done); err != nil {
		t.Fatalf("first SweepOnce() error = %v", err)
	}
	receive(t, entered)
	release <- struct{}{}
	if err := receive(t, done); err != nil {
		t.Fatalf("second SweepOnce() error = %v", err)
	}
	if repository.maximumActive() != 1 {
		t.Errorf("maximum active repository calls = %d, want 1", repository.maximumActive())
	}
}

func TestRecoveryServiceCancellationWhileWaitingForSweep(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	repository := &fakeRecoveryRepository{hook: func(_ context.Context, _ recoveryCall) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	service := newTestRecoveryService(t, repository, newFakeClock(now), &fakeLogger{}, validRecoveryConfig())
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.SweepOnce(context.Background(), now)
		firstDone <- err
	}()
	receive(t, entered)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.SweepOnce(ctx, now); !errors.Is(err, context.Canceled) {
		t.Errorf("waiting SweepOnce() error = %v, want context.Canceled", err)
	}
	close(release)
	if err := receive(t, firstDone); err != nil {
		t.Fatalf("active SweepOnce() error = %v", err)
	}
}

func TestRecoveryServiceConcurrentInstancesRemainIndependent(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	repository := &fakeRecoveryRepository{hook: func(_ context.Context, _ recoveryCall) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	first := newTestRecoveryService(t, repository, newFakeClock(now), &fakeLogger{}, validRecoveryConfig())
	second := newTestRecoveryService(t, repository, newFakeClock(now), &fakeLogger{}, validRecoveryConfig())

	done := make(chan error, 2)
	for _, service := range []*RecoveryService{first, second} {
		go func(service *RecoveryService) {
			_, err := service.SweepOnce(context.Background(), now)
			done <- err
		}(service)
	}
	receive(t, entered)
	receive(t, entered)
	close(release)
	for range 2 {
		if err := receive(t, done); err != nil {
			t.Errorf("SweepOnce() error = %v", err)
		}
	}
	if repository.maximumActive() != 2 {
		t.Errorf("maximum active repository calls = %d, want 2 independent instances", repository.maximumActive())
	}
}

func TestNewRecoveryServiceValidation(t *testing.T) {
	repository := &fakeRecoveryRepository{}
	clock := newFakeClock(time.Now())
	tests := []struct {
		name       string
		repository ExpiredLeaseRepository
		clock      Clock
		config     RecoveryConfig
	}{
		{name: "missing repository", clock: clock, config: validRecoveryConfig()},
		{name: "missing clock", repository: repository, config: validRecoveryConfig()},
		{name: "zero interval", repository: repository, clock: clock, config: RecoveryConfig{RetryDelay: time.Minute, BatchSize: 1}},
		{name: "negative interval", repository: repository, clock: clock, config: RecoveryConfig{SweepInterval: -time.Second, RetryDelay: time.Minute, BatchSize: 1}},
		{name: "zero retry delay", repository: repository, clock: clock, config: RecoveryConfig{SweepInterval: time.Minute, BatchSize: 1}},
		{name: "negative retry delay", repository: repository, clock: clock, config: RecoveryConfig{SweepInterval: time.Minute, RetryDelay: -time.Second, BatchSize: 1}},
		{name: "zero batch", repository: repository, clock: clock, config: RecoveryConfig{SweepInterval: time.Minute, RetryDelay: time.Minute}},
		{name: "negative batch", repository: repository, clock: clock, config: RecoveryConfig{SweepInterval: time.Minute, RetryDelay: time.Minute, BatchSize: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRecoveryService(tt.repository, tt.clock, nil, tt.config); err == nil {
				t.Fatal("NewRecoveryService() error = nil, want validation error")
			}
		})
	}
}

func TestRecoveryServiceSweepOnceRejectsZeroTime(t *testing.T) {
	repository := &fakeRecoveryRepository{}
	service := newTestRecoveryService(t, repository, newFakeClock(time.Now()), &fakeLogger{}, validRecoveryConfig())
	if _, err := service.SweepOnce(context.Background(), time.Time{}); err == nil {
		t.Fatal("SweepOnce() error = nil, want zero-time rejection")
	}
	if len(repository.snapshotCalls()) != 0 {
		t.Errorf("repository calls = %d, want 0", len(repository.snapshotCalls()))
	}
}

func validRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{SweepInterval: time.Minute, RetryDelay: time.Minute, BatchSize: 10}
}

func newTestRecoveryService(t *testing.T, repository ExpiredLeaseRepository, clock Clock, logger Logger, config RecoveryConfig) *RecoveryService {
	t.Helper()
	service, err := NewRecoveryService(repository, clock, logger, config)
	if err != nil {
		t.Fatalf("NewRecoveryService() error = %v", err)
	}
	return service
}

type recoveryCall struct {
	now       time.Time
	retryAt   time.Time
	batchSize int
}

type fakeRecoveryRepository struct {
	mu        sync.Mutex
	calls     []recoveryCall
	results   [][]job.Job
	errors    []error
	called    chan recoveryCall
	hook      func(context.Context, recoveryCall) error
	active    int
	maxActive int
}

func (repository *fakeRecoveryRepository) RecoverExpiredLeases(ctx context.Context, now, retryAt time.Time, batchSize int) ([]job.Job, error) {
	call := recoveryCall{now: now, retryAt: retryAt, batchSize: batchSize}
	repository.mu.Lock()
	index := len(repository.calls)
	repository.calls = append(repository.calls, call)
	repository.active++
	if repository.active > repository.maxActive {
		repository.maxActive = repository.active
	}
	var result []job.Job
	if index < len(repository.results) {
		result = repository.results[index]
	}
	var configuredError error
	if index < len(repository.errors) {
		configuredError = repository.errors[index]
	}
	repository.mu.Unlock()
	defer func() {
		repository.mu.Lock()
		repository.active--
		repository.mu.Unlock()
	}()
	if repository.called != nil {
		repository.called <- call
	}
	if repository.hook != nil {
		if err := repository.hook(ctx, call); err != nil {
			return nil, err
		}
	}
	return result, configuredError
}

func (repository *fakeRecoveryRepository) snapshotCalls() []recoveryCall {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return append([]recoveryCall(nil), repository.calls...)
}

func (repository *fakeRecoveryRepository) maximumActive() int {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.maxActive
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	created chan *fakeTicker
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, created: make(chan *fakeTicker, 4)}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) setNow(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (clock *fakeClock) NewTicker(interval time.Duration) Ticker {
	ticker := &fakeTicker{interval: interval, ticks: make(chan time.Time, 4)}
	clock.created <- ticker
	return ticker
}

type fakeTicker struct {
	mu       sync.Mutex
	interval time.Duration
	ticks    chan time.Time
	stopped  bool
}

func (ticker *fakeTicker) C() <-chan time.Time { return ticker.ticks }
func (ticker *fakeTicker) Stop() {
	ticker.mu.Lock()
	ticker.stopped = true
	ticker.mu.Unlock()
}
func (ticker *fakeTicker) tick(now time.Time) { ticker.ticks <- now }
func (ticker *fakeTicker) isStopped() bool {
	ticker.mu.Lock()
	defer ticker.mu.Unlock()
	return ticker.stopped
}

type logRecord struct {
	message string
	args    []any
}

type fakeLogger struct {
	errors chan logRecord
	infos  chan logRecord
}

func (logger *fakeLogger) ErrorContext(_ context.Context, message string, args ...any) {
	if logger.errors != nil {
		logger.errors <- logRecord{message: message, args: args}
	}
}

func (logger *fakeLogger) InfoContext(_ context.Context, message string, args ...any) {
	if logger.infos != nil {
		logger.infos <- logRecord{message: message, args: args}
	}
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for test signal")
		var zero T
		return zero
	}
}

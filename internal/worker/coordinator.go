// Package worker coordinates bounded task execution.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

// Repository is the persistent lifecycle boundary required by workers.
type Repository interface {
	ClaimNext(context.Context, job.WorkerID, job.LeaseToken, time.Time, time.Time) (job.Job, error)
	StartExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time) (job.Job, error)
	CompleteExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, json.RawMessage, time.Time) (job.Job, error)
	FailExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time, string, *time.Time) (job.Job, error)
	RenewLease(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time, time.Time) (job.Job, error)
}

// Clock supplies worker time and polling tickers.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

// Ticker delivers polling signals.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// TokenGenerator creates a fresh credential for each claim attempt.
type TokenGenerator interface {
	NewLeaseToken() (job.LeaseToken, error)
}

// Config controls worker identity, timing, and concurrency.
type Config struct {
	WorkerID          job.WorkerID
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	RetryDelay        time.Duration
	HeartbeatInterval time.Duration
}

// Coordinator polls for jobs and executes them within a fixed slot bound.
type Coordinator struct {
	repository      Repository
	handlers        *task.HandlerRegistry
	clock           Clock
	tokens          TokenGenerator
	logger          *slog.Logger
	config          Config
	slots           chan struct{}
	active          sync.WaitGroup
	isNoJob         func(error) bool
	isOwnershipLost func(error) bool
}

// NewCoordinator validates dependencies and constructs a worker coordinator.
func NewCoordinator(repository Repository, handlers *task.HandlerRegistry, clock Clock, tokens TokenGenerator, logger *slog.Logger, config Config, isNoJob, isOwnershipLost func(error) bool) (*Coordinator, error) {
	if repository == nil || handlers == nil || clock == nil || tokens == nil || isNoJob == nil || isOwnershipLost == nil {
		return nil, errors.New("worker dependencies must not be nil")
	}
	if config.WorkerID == "" {
		return nil, errors.New("worker ID must not be empty")
	}
	if config.Concurrency <= 0 {
		return nil, errors.New("worker concurrency must be positive")
	}
	if config.PollInterval <= 0 || config.LeaseDuration <= 0 || config.RetryDelay <= 0 || config.HeartbeatInterval <= 0 {
		return nil, errors.New("worker timing values must be positive")
	}
	if config.HeartbeatInterval >= config.LeaseDuration/2 {
		return nil, errors.New("worker heartbeat interval must leave sufficient lease-renewal margin")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{repository: repository, handlers: handlers, clock: clock, tokens: tokens, logger: logger, config: config, slots: make(chan struct{}, config.Concurrency), isNoJob: isNoJob, isOwnershipLost: isOwnershipLost}, nil
}

// Run polls immediately and periodically until cancellation, then waits for active handlers.
func (coordinator *Coordinator) Run(ctx context.Context) error {
	coordinator.fillAvailableSlots(ctx)
	ticker := coordinator.clock.NewTicker(coordinator.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			coordinator.active.Wait()
			return ctx.Err()
		case <-ticker.C():
			coordinator.fillAvailableSlots(ctx)
		}
	}
}

func (coordinator *Coordinator) fillAvailableSlots(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case coordinator.slots <- struct{}{}:
		default:
			return
		}
		if !coordinator.claimOne(ctx) {
			return
		}
	}
}

func (coordinator *Coordinator) claimOne(ctx context.Context) bool {
	release := func() { <-coordinator.slots }
	token, err := coordinator.tokens.NewLeaseToken()
	if err != nil {
		release()
		coordinator.logger.ErrorContext(ctx, "generate lease token", "error", err)
		return false
	}
	now := coordinator.clock.Now()
	claimed, err := coordinator.repository.ClaimNext(ctx, coordinator.config.WorkerID, token, now, now.Add(coordinator.config.LeaseDuration))
	if err != nil {
		release()
		if !coordinator.isNoJob(err) && ctx.Err() == nil {
			coordinator.logger.ErrorContext(ctx, "claim job", "error", err)
		}
		return false
	}
	coordinator.active.Add(1)
	go func() {
		defer coordinator.active.Done()
		defer release()
		coordinator.execute(ctx, claimed, token)
	}()
	return true
}

func (coordinator *Coordinator) execute(ctx context.Context, claimed job.Job, token job.LeaseToken) {
	started, err := coordinator.repository.StartExecution(ctx, claimed.ID, coordinator.config.WorkerID, token, coordinator.clock.Now())
	if err != nil {
		coordinator.logger.ErrorContext(ctx, "start job execution", "job_id", claimed.ID, "task_type", claimed.TaskType, "error", err)
		return
	}
	if started.Lease == nil {
		coordinator.logger.ErrorContext(ctx, "start job execution returned no lease", "job_id", started.ID, "task_type", started.TaskType)
		return
	}
	executionCtx, cancelExecution := context.WithCancel(ctx)
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan heartbeatOutcome, 1)
	go func() { heartbeatDone <- coordinator.heartbeat(heartbeatCtx, cancelExecution, started, token) }()
	handler, err := coordinator.handlers.Lookup(started.TaskType)
	if err != nil {
		stopHeartbeat()
		heartbeat := <-heartbeatDone
		cancelExecution()
		if !heartbeat.ownershipLost && ctx.Err() == nil && coordinator.clock.Now().Before(heartbeat.confirmedExpiration) {
			coordinator.reportFailure(ctx, started, token, err)
		}
		return
	}
	result, err := handler.Execute(executionCtx, started.Payload)
	stopHeartbeat()
	heartbeat := <-heartbeatDone
	cancelExecution()
	// Joining the heartbeat before terminal persistence prevents renewal from
	// racing completion or failure with the same lease credential.
	if heartbeat.ownershipLost || ctx.Err() != nil || !coordinator.clock.Now().Before(heartbeat.confirmedExpiration) {
		return
	}
	if err != nil {
		coordinator.reportFailure(ctx, started, token, err)
		return
	}
	if _, err := coordinator.repository.CompleteExecution(ctx, started.ID, coordinator.config.WorkerID, token, result, coordinator.clock.Now()); err != nil {
		coordinator.logger.ErrorContext(ctx, "complete job execution", "job_id", started.ID, "task_type", started.TaskType, "error", err)
	}
}

type heartbeatOutcome struct {
	confirmedExpiration time.Time
	ownershipLost       bool
}

func (coordinator *Coordinator) heartbeat(ctx context.Context, cancelExecution context.CancelFunc, started job.Job, token job.LeaseToken) heartbeatOutcome {
	confirmed := started.Lease.ExpiresAt
	ticker := coordinator.clock.NewTicker(coordinator.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return heartbeatOutcome{confirmedExpiration: confirmed}
		case <-ticker.C():
			now := coordinator.clock.Now()
			if !now.Before(confirmed) {
				cancelExecution()
				return heartbeatOutcome{confirmedExpiration: confirmed, ownershipLost: true}
			}
			proposed := now.Add(coordinator.config.LeaseDuration)
			if !proposed.After(confirmed) {
				continue
			}
			renewed, err := coordinator.repository.RenewLease(ctx, started.ID, coordinator.config.WorkerID, token, now, proposed)
			if err != nil {
				// A renewal interrupted by this heartbeat's intentional shutdown is
				// lifecycle coordination, not an operational renewal failure.
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return heartbeatOutcome{confirmedExpiration: confirmed}
				}
				if coordinator.isOwnershipLost(err) || !coordinator.clock.Now().Before(confirmed) {
					cancelExecution()
					return heartbeatOutcome{confirmedExpiration: confirmed, ownershipLost: true}
				}
				coordinator.logger.ErrorContext(ctx, "renew job lease", "job_id", started.ID, "task_type", started.TaskType, "error", err)
				continue
			}
			if renewed.Lease == nil || !renewed.Lease.ExpiresAt.After(confirmed) {
				cancelExecution()
				return heartbeatOutcome{confirmedExpiration: confirmed, ownershipLost: true}
			}
			confirmed = renewed.Lease.ExpiresAt
		}
	}
}

func (coordinator *Coordinator) reportFailure(ctx context.Context, started job.Job, token job.LeaseToken, executionError error) {
	now := coordinator.clock.Now()
	retryAt := now.Add(coordinator.config.RetryDelay)
	if _, err := coordinator.repository.FailExecution(ctx, started.ID, coordinator.config.WorkerID, token, now, executionError.Error(), &retryAt); err != nil {
		coordinator.logger.ErrorContext(ctx, "report job execution failure", "job_id", started.ID, "task_type", started.TaskType, "error", err)
	}
}

// RandomTokenGenerator creates unpredictable 128-bit lease tokens.
type RandomTokenGenerator struct{}

// NewLeaseToken creates a fresh hexadecimal lease token.
func (RandomTokenGenerator) NewLeaseToken() (job.LeaseToken, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("read randomness: %w", err)
	}
	return job.LeaseToken(hex.EncodeToString(value[:])), nil
}

type systemClock struct{}

// NewSystemClock returns the production worker clock.
func NewSystemClock() Clock        { return systemClock{} }
func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTicker(interval time.Duration) Ticker {
	return systemTicker{time.NewTicker(interval)}
}

type systemTicker struct{ *time.Ticker }

func (ticker systemTicker) C() <-chan time.Time { return ticker.Ticker.C }

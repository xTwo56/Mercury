// Package scheduler coordinates bounded background maintenance work.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xtwo56/mercury/internal/job"
)

// ExpiredLeaseRepository recovers a bounded batch using persistent locking.
type ExpiredLeaseRepository interface {
	RecoverExpiredLeases(context.Context, time.Time, time.Time, int) ([]job.Job, error)
}

// Clock supplies current time and tickers to the recovery service.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

// Ticker delivers periodic recovery signals.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Logger records background sweep failures.
type Logger interface {
	ErrorContext(context.Context, string, ...any)
	InfoContext(context.Context, string, ...any)
}

// RecoveryConfig controls the bounded recovery loop.
type RecoveryConfig struct {
	SweepInterval time.Duration
	RetryDelay    time.Duration
	BatchSize     int
}

// RecoverySummary describes the state outcomes from one sweep.
type RecoverySummary struct {
	Recovered      int
	Queued         int
	RetryScheduled int
	Failed         int
}

// RecoveryService periodically recovers expired job leases.
type RecoveryService struct {
	repository ExpiredLeaseRepository
	clock      Clock
	logger     Logger
	config     RecoveryConfig

	sweepPermit chan struct{}
}

// NewRecoveryService validates dependencies and constructs a recovery service.
func NewRecoveryService(repository ExpiredLeaseRepository, clock Clock, logger Logger, config RecoveryConfig) (*RecoveryService, error) {
	if repository == nil {
		return nil, errors.New("recovery repository must not be nil")
	}
	if clock == nil {
		return nil, errors.New("recovery clock must not be nil")
	}
	if config.SweepInterval <= 0 {
		return nil, errors.New("recovery sweep interval must be positive")
	}
	if config.RetryDelay <= 0 {
		return nil, errors.New("recovery retry delay must be positive")
	}
	if config.BatchSize <= 0 {
		return nil, errors.New("recovery batch size must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	service := &RecoveryService{
		repository:  repository,
		clock:       clock,
		logger:      logger,
		config:      config,
		sweepPermit: make(chan struct{}, 1),
	}
	service.sweepPermit <- struct{}{}
	return service, nil
}

// SweepOnce recovers at most the configured number of expired leases.
func (s *RecoveryService) SweepOnce(ctx context.Context, now time.Time) (RecoverySummary, error) {
	if now.IsZero() {
		return RecoverySummary{}, errors.New("recovery sweep time must not be zero")
	}

	// Serializing at the service boundary also protects callers that invoke
	// SweepOnce directly while Run is active. Cross-instance safety remains the
	// repository's responsibility through SKIP LOCKED transactions.
	select {
	case <-ctx.Done():
		return RecoverySummary{}, ctx.Err()
	case <-s.sweepPermit:
	}
	defer func() { s.sweepPermit <- struct{}{} }()

	recovered, err := s.repository.RecoverExpiredLeases(ctx, now, now.Add(s.config.RetryDelay), s.config.BatchSize)
	if err != nil {
		return RecoverySummary{}, fmt.Errorf("recover expired leases: %w", err)
	}

	summary := RecoverySummary{Recovered: len(recovered)}
	for _, recoveredJob := range recovered {
		switch recoveredJob.State {
		case job.StateQueued:
			summary.Queued++
		case job.StateRetryScheduled:
			summary.RetryScheduled++
		case job.StateFailed:
			summary.Failed++
		}
	}
	return summary, nil
}

// Run sweeps immediately, then at each configured interval until cancellation.
func (s *RecoveryService) Run(ctx context.Context) error {
	s.sweepAndReport(ctx)

	ticker := s.clock.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			s.sweepAndReport(ctx)
		}
	}
}

func (s *RecoveryService) sweepAndReport(ctx context.Context) {
	summary, err := s.SweepOnce(ctx, s.clock.Now())
	if err != nil {
		if ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "expired lease recovery sweep failed", "error", err)
		}
		return
	}
	if summary.Recovered > 0 {
		s.logger.InfoContext(ctx, "expired lease recovery sweep completed",
			"recovered", summary.Recovered,
			"queued", summary.Queued,
			"retry_scheduled", summary.RetryScheduled,
			"failed", summary.Failed,
		)
	}
}

type systemClock struct{}

// NewSystemClock returns a clock backed by the standard time package.
func NewSystemClock() Clock {
	return systemClock{}
}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) NewTicker(interval time.Duration) Ticker {
	return systemTicker{Ticker: time.NewTicker(interval)}
}

type systemTicker struct {
	*time.Ticker
}

func (ticker systemTicker) C() <-chan time.Time {
	return ticker.Ticker.C
}

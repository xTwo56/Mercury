package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/xtwo56/mercury/internal/job"
)

// ErrInvalidWorkerRequest identifies an invalid lifecycle envelope, before any write.
var ErrInvalidWorkerRequest = errors.New("invalid worker request")

// Remote timing is server-controlled. Workers heartbeat well before the returned
// lease expiration and stop at that expiration if renewal cannot be confirmed.
const (
	RemoteLeaseDuration = time.Minute
	RemoteRetryDelay    = time.Minute
	RemoteMaxExecution  = 24 * time.Hour
	MaxSupportedTypes   = 100
)

// WorkerRepository reuses the same atomic lifecycle operations as local workers.
// Each mutation owns its short database transaction; no transaction spans HTTP.
type WorkerRepository interface {
	ClaimNext(context.Context, job.WorkerID, job.LeaseToken, time.Time, time.Time, ...job.TaskType) (job.Job, error)
	StartExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time) (job.Job, error)
	RenewLeaseBounded(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time, time.Time, time.Duration) (job.Job, error)
	CompleteExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, json.RawMessage, time.Time) (job.Job, error)
	FailExecution(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time, string, *time.Time) (job.Job, error)
	FailExecutionPermanently(context.Context, job.JobID, job.WorkerID, job.LeaseToken, time.Time, string) (job.Job, error)
	GetByID(context.Context, job.JobID) (job.Job, error)
}

// LeaseTokenGenerator supplies unpredictable fencing tokens, independently of API authentication.
type LeaseTokenGenerator interface {
	NewLeaseToken() (job.LeaseToken, error)
}

// WorkerService validates remote requests and supplies trusted timing and tokens.
// The domain and repository, not this adapter, decide whether a transition is legal.
type WorkerService struct {
	repository WorkerRepository
	clock      Clock
	tokens     LeaseTokenGenerator
}

// NewWorkerService connects remote operations to existing lifecycle persistence.
func NewWorkerService(repository WorkerRepository, clock Clock, tokens LeaseTokenGenerator) *WorkerService {
	return &WorkerService{repository: repository, clock: clock, tokens: tokens}
}

// Claim leases only explicitly supported work. A lost response may leave a lease
// whose ID is unknown to the caller; recovery releases it after expiration.
func (s *WorkerService) Claim(ctx context.Context, worker job.WorkerID, types []job.TaskType) (job.Job, error) {
	if blank(string(worker)) || len(types) == 0 || len(types) > MaxSupportedTypes {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	for _, t := range types {
		if blank(string(t)) {
			return job.Job{}, ErrInvalidWorkerRequest
		}
	}
	token, err := s.tokens.NewLeaseToken()
	if err != nil {
		return job.Job{}, err
	}
	now := s.clock.Now()
	return s.repository.ClaimNext(ctx, worker, token, now, now.Add(RemoteLeaseDuration), types...)
}

// Start consumes one attempt, or returns the same live execution on a replay.
func (s *WorkerService) Start(ctx context.Context, id job.JobID, worker job.WorkerID, token job.LeaseToken) (job.Job, error) {
	if !validOwnership(id, worker, token) {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	return s.repository.StartExecution(ctx, id, worker, token, s.clock.Now())
}

// Heartbeat extends a live execution without letting it run indefinitely.
// The repository caps expiration against the first persisted start under lock.
func (s *WorkerService) Heartbeat(ctx context.Context, id job.JobID, worker job.WorkerID, token job.LeaseToken) (job.Job, error) {
	if !validOwnership(id, worker, token) {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	now := s.clock.Now()
	return s.repository.RenewLeaseBounded(ctx, id, worker, token, now, now.Add(RemoteLeaseDuration), RemoteMaxExecution)
}

// Complete reports one outcome. Lost responses require state inspection before
// another report; a cleared or replaced lease cannot accept the old transition.
func (s *WorkerService) Complete(ctx context.Context, id job.JobID, worker job.WorkerID, token job.LeaseToken, result json.RawMessage) (job.Job, error) {
	if !validOwnership(id, worker, token) || !json.Valid(result) {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	return s.repository.CompleteExecution(ctx, id, worker, token, result, s.clock.Now())
}

// Fail distinguishes task policy from transport errors. Permanent failures stop
// immediately; retryable failures use the existing attempt budget and delay.
func (s *WorkerService) Fail(ctx context.Context, id job.JobID, worker job.WorkerID, token job.LeaseToken, classification, message string) (job.Job, error) {
	if !validOwnership(id, worker, token) || blank(message) || len(message) > 4096 {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	now := s.clock.Now()
	switch classification {
	case "permanent":
		return s.repository.FailExecutionPermanently(ctx, id, worker, token, now, message)
	case "retryable":
		retryAt := now.Add(RemoteRetryDelay)
		return s.repository.FailExecution(ctx, id, worker, token, now, message, &retryAt)
	default:
		return job.Job{}, ErrInvalidWorkerRequest
	}
}

// Inspect reads current durable state for reconciliation; it does not reserve work.
func (s *WorkerService) Inspect(ctx context.Context, id job.JobID) (job.Job, error) {
	if blank(string(id)) {
		return job.Job{}, ErrInvalidWorkerRequest
	}
	return s.repository.GetByID(ctx, id)
}

func blank(value string) bool { return strings.TrimSpace(value) == "" }
func validOwnership(id job.JobID, worker job.WorkerID, token job.LeaseToken) bool {
	return !blank(string(id)) && !blank(string(worker)) && !blank(string(token))
}

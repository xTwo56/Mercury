package job

import (
	"errors"
	"time"
)

// leaseExpiredFailure is stable persisted metadata for system-initiated
// recovery, distinguishing lost ownership from a task-reported failure.
const leaseExpiredFailure = "lease expired"

type (
	// WorkerID identifies the worker process that owns a lease.
	WorkerID string
	// LeaseToken is an opaque credential authenticating one claim attempt.
	LeaseToken string
)

// Lease grants one worker temporary, token-authenticated ownership of a job.
// ExpiresAt is an exclusive boundary: the lease is invalid when now is equal
// to or later than it.
type Lease struct {
	WorkerID  WorkerID
	Token     LeaseToken
	ExpiresAt time.Time
}

// Claim leases an available queued or retry-scheduled job to a worker without
// consuming an attempt. Execution capacity is consumed later by Start. All
// transition, budget, availability, identity, and expiry checks occur before
// mutation so a rejected claim leaves the Job unchanged.
func (j *Job) Claim(workerID WorkerID, token LeaseToken, now, expiresAt time.Time) error {
	if !CanTransition(j.State, StateLeased) || j.Lease != nil {
		return errors.New("job cannot transition to leased")
	}
	if j.RemainingAttempts() == 0 {
		return errors.New("job has no attempts remaining")
	}
	if now.IsZero() {
		return errors.New("current time must not be zero")
	}
	if j.AvailableAt.After(now) {
		return errors.New("job is not yet available")
	}
	if workerID == "" {
		return errors.New("worker ID must not be empty")
	}
	if token == "" {
		return errors.New("lease token must not be empty")
	}
	if !expiresAt.After(now) {
		return errors.New("lease expiration must be after current time")
	}

	j.Lease = &Lease{
		WorkerID:  workerID,
		Token:     token,
		ExpiresAt: expiresAt.UTC(),
	}
	j.State = StateLeased
	return nil
}

// ValidateLease verifies that workerID and token identify the current,
// unexpired leased Job. The expiration check deliberately treats equality as
// expired, preventing a worker and lease-recovery process from both acting at
// the boundary.
func (j Job) ValidateLease(workerID WorkerID, token LeaseToken, now time.Time) error {
	if j.State != StateLeased || j.Lease == nil {
		return errors.New("job has no active lease")
	}
	if workerID != j.Lease.WorkerID {
		return errors.New("lease worker does not match")
	}
	if token != j.Lease.Token {
		return errors.New("lease token does not match")
	}
	if !now.Before(j.Lease.ExpiresAt) {
		return errors.New("lease has expired")
	}
	return nil
}

// RecoverExpiredLease performs Mercury-initiated recovery without worker
// credentials after a lease reaches its expiration boundary. A leased Job
// returns immediately to queued because no attempt began. A running Job records
// a lease-expiration failure and either schedules retryAt or becomes terminal
// when its attempt budget is exhausted. Successful recovery clears ownership;
// validation completes before mutation so failures leave the Job unchanged.
func (j *Job) RecoverExpiredLease(now, retryAt time.Time) error {
	if now.IsZero() {
		return errors.New("current time must not be zero")
	}
	if j.Lease == nil {
		return errors.New("job has no lease")
	}
	if j.State != StateLeased && j.State != StateRunning {
		return errors.New("job does not have a recoverable lease")
	}
	if now.Before(j.Lease.ExpiresAt) {
		return errors.New("lease has not expired")
	}

	if j.State == StateLeased {
		j.Lease = nil
		j.State = StateQueued
		j.AvailableAt = now.UTC()
		return nil
	}

	retryable := j.RemainingAttempts() > 0
	if retryable {
		if retryAt.IsZero() {
			return errors.New("retry time must not be zero")
		}
		if !retryAt.After(now) {
			return errors.New("retry time must be after recovery time")
		}
	}

	failedAt := now.UTC()
	j.LastError = leaseExpiredFailure
	j.FailedAt = &failedAt
	j.Lease = nil
	j.StartedAt = nil
	if retryable {
		j.State = StateRetryScheduled
		j.AvailableAt = retryAt.UTC()
	} else {
		j.State = StateFailed
	}
	return nil
}

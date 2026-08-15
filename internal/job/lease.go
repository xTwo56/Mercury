package job

import (
	"errors"
	"time"
)

type (
	WorkerID   string
	LeaseToken string
)

// Lease grants a worker temporary ownership of a job.
type Lease struct {
	WorkerID  WorkerID
	Token     LeaseToken
	ExpiresAt time.Time
}

// Claim leases an available queued or retry-scheduled job to a worker.
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

// ValidateLease verifies that a worker holds the job's unexpired lease.
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

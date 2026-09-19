package job

import (
	"errors"
	"time"
)

var (
	// ErrJobNotRunning identifies renewal rejected outside the running state.
	ErrJobNotRunning = errors.New("job is not running")
	// ErrLeaseMissing identifies renewal rejected because no current lease exists.
	ErrLeaseMissing = errors.New("job has no lease")
	// ErrLeaseWorkerMismatch identifies renewal attempted by a different worker.
	ErrLeaseWorkerMismatch = errors.New("lease worker does not match")
	// ErrLeaseTokenMismatch identifies renewal attempted with a stale or invalid token.
	ErrLeaseTokenMismatch = errors.New("lease token does not match")
	// ErrLeaseExpired identifies renewal at or after the confirmed expiration.
	ErrLeaseExpired = errors.New("lease has expired")
)

// RenewLease authenticates a running worker and moves its lease expiration
// strictly forward. Requiring the proposed expiration to exceed both now and
// the confirmed expiration prevents delayed or concurrent heartbeats from
// shortening the lease. Only ExpiresAt changes after successful validation.
func (j *Job) RenewLease(workerID WorkerID, token LeaseToken, now, newExpiresAt time.Time) error {
	if now.IsZero() {
		return errors.New("current time must not be zero")
	}
	if newExpiresAt.IsZero() {
		return errors.New("new lease expiration must not be zero")
	}
	if j.State != StateRunning {
		return ErrJobNotRunning
	}
	if j.Lease == nil {
		return ErrLeaseMissing
	}
	if workerID != j.Lease.WorkerID {
		return ErrLeaseWorkerMismatch
	}
	if token != j.Lease.Token {
		return ErrLeaseTokenMismatch
	}
	if !now.Before(j.Lease.ExpiresAt) {
		return ErrLeaseExpired
	}
	if !newExpiresAt.After(now) {
		return errors.New("new lease expiration must be after current time")
	}
	if !newExpiresAt.After(j.Lease.ExpiresAt) {
		return errors.New("new lease expiration must extend the lease")
	}

	j.Lease.ExpiresAt = newExpiresAt.UTC()
	return nil
}

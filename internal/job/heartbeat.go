package job

import (
	"errors"
	"time"
)

// RenewLease extends the current lease held by a running job.
func (j *Job) RenewLease(workerID WorkerID, token LeaseToken, now, newExpiresAt time.Time) error {
	if now.IsZero() {
		return errors.New("current time must not be zero")
	}
	if newExpiresAt.IsZero() {
		return errors.New("new lease expiration must not be zero")
	}
	if j.State != StateRunning {
		return errors.New("job is not running")
	}
	if j.Lease == nil {
		return errors.New("job has no lease")
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
	if !newExpiresAt.After(now) {
		return errors.New("new lease expiration must be after current time")
	}
	if !newExpiresAt.After(j.Lease.ExpiresAt) {
		return errors.New("new lease expiration must extend the lease")
	}

	j.Lease.ExpiresAt = newExpiresAt.UTC()
	return nil
}

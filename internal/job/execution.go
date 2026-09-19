package job

import (
	"encoding/json"
	"errors"
	"time"
)

// Start authenticates the current lease holder and transitions a leased Job to
// running. It records the supplied time in UTC and consumes exactly one attempt
// only after every validation succeeds, so retries and rejected starts cannot
// accidentally inflate AttemptsStarted.
func (j *Job) Start(workerID WorkerID, token LeaseToken, now time.Time) error {
	if now.IsZero() {
		return errors.New("current time must not be zero")
	}
	// An uncertain start response may be retried with the same live lease.
	// Validate ownership again without consuming another attempt or moving time.
	if j.State == StateRunning {
		if j.Lease == nil {
			return ErrLeaseMissing
		}
		if j.Lease.WorkerID != workerID {
			return ErrLeaseWorkerMismatch
		}
		if j.Lease.Token != token {
			return ErrLeaseTokenMismatch
		}
		if !now.Before(j.Lease.ExpiresAt) {
			return ErrLeaseExpired
		}
		return nil
	}
	if !CanTransition(j.State, StateRunning) {
		return errors.New("job cannot transition to running")
	}
	if j.RemainingAttempts() == 0 {
		return errors.New("job has no attempts remaining")
	}
	if err := j.ValidateLease(workerID, token, now); err != nil {
		return err
	}

	startedAt := now.UTC()
	j.StartedAt = &startedAt
	j.State = StateRunning
	j.AttemptsStarted++
	return nil
}

// Complete authenticates an unexpired running lease and records successful
// terminal output. It defensively copies valid JSON (including JSON null),
// records the UTC completion time, transitions to succeeded, and clears the
// lease so stale workers cannot submit another terminal outcome.
func (j *Job) Complete(workerID WorkerID, token LeaseToken, now time.Time, result json.RawMessage) error {
	if now.IsZero() {
		return errors.New("completion time must not be zero")
	}
	if !CanTransition(j.State, StateSucceeded) {
		return errors.New("job cannot transition to succeeded")
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
	if !json.Valid(result) {
		return errors.New("result must contain valid JSON")
	}

	completedAt := now.UTC()
	j.Result = append(json.RawMessage(nil), result...)
	j.CompletedAt = &completedAt
	j.State = StateSucceeded
	j.Lease = nil
	return nil
}

// Fail authenticates an unexpired running lease and records the latest task
// failure. When another attempt remains, retryAt must be strictly later than
// now and becomes the next claim boundary; otherwise the Job becomes terminally
// failed. Either successful path clears its lease and per-execution StartedAt,
// while validation errors leave all fields unchanged.
func (j *Job) Fail(workerID WorkerID, token LeaseToken, now time.Time, message string, retryAt *time.Time) error {
	return j.fail(workerID, token, now, message, retryAt, false)
}

// FailPermanent ends this job even when attempts remain. It uses the same
// ownership checks and accounting as retryable failure; it never invents attempts.
func (j *Job) FailPermanent(workerID WorkerID, token LeaseToken, now time.Time, message string) error {
	return j.fail(workerID, token, now, message, nil, true)
}

func (j *Job) fail(workerID WorkerID, token LeaseToken, now time.Time, message string, retryAt *time.Time, permanent bool) error {
	if now.IsZero() {
		return errors.New("failure time must not be zero")
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
	if message == "" {
		return errors.New("failure message must not be empty")
	}

	retryable := !permanent && j.AttemptsStarted < j.MaxAttempts
	if retryable {
		if retryAt == nil || retryAt.IsZero() {
			return errors.New("retry time must not be zero")
		}
		if !retryAt.After(now) {
			return errors.New("retry time must be after failure time")
		}
	}

	failedAt := now.UTC()
	j.LastError = message
	j.FailedAt = &failedAt
	j.Lease = nil
	j.StartedAt = nil
	if retryable {
		j.AvailableAt = retryAt.UTC()
		j.State = StateRetryScheduled
	} else {
		j.State = StateFailed
	}
	return nil
}

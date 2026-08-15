package job

import (
	"encoding/json"
	"errors"
	"time"
)

// Start transitions a leased job to running for the worker holding its lease.
func (j *Job) Start(workerID WorkerID, token LeaseToken, now time.Time) error {
	if now.IsZero() {
		return errors.New("current time must not be zero")
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

// Complete records the successful result of a running job.
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

// Fail records an execution failure and either schedules a retry or ends the job.
func (j *Job) Fail(workerID WorkerID, token LeaseToken, now time.Time, message string, retryAt *time.Time) error {
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

	retryable := j.AttemptsStarted < j.MaxAttempts
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

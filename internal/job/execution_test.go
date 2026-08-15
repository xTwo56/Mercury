package job

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestJobStart(t *testing.T) {
	claimTime := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	expiresAt := claimTime.Add(10 * time.Minute)
	location := time.FixedZone("start", -4*60*60)
	startTime := claimTime.Add(5 * time.Minute).In(location)

	claimedJob := func(t *testing.T) Job {
		t.Helper()
		job := newTestJob(t, claimTime, claimTime)
		if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, expiresAt); err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return job
	}

	tests := []struct {
		name     string
		prepare  func(*testing.T) Job
		workerID WorkerID
		token    LeaseToken
		now      time.Time
		wantErr  bool
	}{
		{name: "successful start", prepare: claimedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: startTime},
		{
			name: "duplicate start",
			prepare: func(t *testing.T) Job {
				job := claimedJob(t)
				if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), startTime); err != nil {
					t.Fatalf("initial Start() error = %v", err)
				}
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: startTime.Add(time.Second), wantErr: true,
		},
		{name: "wrong state", prepare: func(t *testing.T) Job { return newTestJob(t, claimTime, claimTime) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: startTime, wantErr: true},
		{
			name: "missing lease",
			prepare: func(t *testing.T) Job {
				job := newTestJob(t, claimTime, claimTime)
				job.State = StateLeased
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: startTime, wantErr: true,
		},
		{name: "wrong worker", prepare: claimedJob, workerID: WorkerID("worker-2"), token: LeaseToken("token-1"), now: startTime, wantErr: true},
		{name: "wrong token", prepare: claimedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-2"), now: startTime, wantErr: true},
		{name: "zero current time", prepare: claimedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), wantErr: true},
		{name: "expired lease", prepare: claimedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: expiresAt.Add(time.Nanosecond), wantErr: true},
		{name: "exact expiry boundary", prepare: claimedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: expiresAt, wantErr: true},
		{
			name: "exhausted attempts",
			prepare: func(t *testing.T) Job {
				job := claimedJob(t)
				job.AttemptsStarted = job.MaxAttempts
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: startTime, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.prepare(t)
			before := job

			err := job.Start(tt.workerID, tt.token, tt.now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !reflect.DeepEqual(job, before) {
					t.Errorf("Start() mutated job on failure: got %#v, want %#v", job, before)
				}
				return
			}

			if job.State != StateRunning {
				t.Errorf("Job.State = %q, want %q", job.State, StateRunning)
			}
			if job.AttemptsStarted != before.AttemptsStarted+1 {
				t.Errorf("Job.AttemptsStarted = %d, want %d", job.AttemptsStarted, before.AttemptsStarted+1)
			}
			wantStartedAt := tt.now.UTC()
			if job.StartedAt == nil || !job.StartedAt.Equal(wantStartedAt) {
				t.Errorf("Job.StartedAt = %v, want %v", job.StartedAt, wantStartedAt)
			}
			if job.StartedAt != nil && job.StartedAt.Location() != time.UTC {
				t.Errorf("Job.StartedAt location = %v, want UTC", job.StartedAt.Location())
			}
		})
	}
}

func TestJobComplete(t *testing.T) {
	claimTime := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	expiresAt := claimTime.Add(10 * time.Minute)
	location := time.FixedZone("complete", 5*60*60+30*60)
	completionTime := claimTime.Add(5 * time.Minute).In(location)

	leasedJob := func(t *testing.T, expiration time.Time) Job {
		t.Helper()
		job := newTestJob(t, claimTime, claimTime)
		if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, expiration); err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return job
	}
	runningJob := func(t *testing.T, expiration time.Time) Job {
		t.Helper()
		job := leasedJob(t, expiration)
		if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), claimTime.Add(time.Minute)); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		return job
	}

	tests := []struct {
		name     string
		prepare  func(*testing.T) Job
		workerID WorkerID
		token    LeaseToken
		now      time.Time
		result   json.RawMessage
		wantErr  bool
	}{
		{name: "successful completion", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{"sent":true}`)},
		{name: "null result", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`null`)},
		{name: "wrong state", prepare: func(t *testing.T) Job { return leasedJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true},
		{
			name: "missing lease",
			prepare: func(t *testing.T) Job {
				job := newTestJob(t, claimTime, claimTime)
				job.State = StateRunning
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true,
		},
		{name: "wrong worker", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-2"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true},
		{name: "wrong token", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-2"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true},
		{name: "expired lease", prepare: func(t *testing.T) Job { return runningJob(t, completionTime.Add(-time.Nanosecond)) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true},
		{name: "exact expiry boundary", prepare: func(t *testing.T) Job { return runningJob(t, completionTime) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{}`), wantErr: true},
		{name: "invalid result", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime, result: json.RawMessage(`{"sent":}`), wantErr: true},
		{name: "zero completion time", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), result: json.RawMessage(`{}`), wantErr: true},
		{
			name: "duplicate completion",
			prepare: func(t *testing.T) Job {
				job := runningJob(t, expiresAt)
				if err := job.Complete(WorkerID("worker-1"), LeaseToken("token-1"), completionTime, json.RawMessage(`{}`)); err != nil {
					t.Fatalf("initial Complete() error = %v", err)
				}
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: completionTime.Add(time.Second), result: json.RawMessage(`{}`), wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.prepare(t)
			before := cloneExecutionJob(job)

			err := job.Complete(tt.workerID, tt.token, tt.now, tt.result)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Complete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !reflect.DeepEqual(job, before) {
					t.Errorf("Complete() mutated job on failure: got %#v, want %#v", job, before)
				}
				return
			}

			if job.State != StateSucceeded {
				t.Errorf("Job.State = %q, want %q", job.State, StateSucceeded)
			}
			if job.Lease != nil {
				t.Errorf("Job.Lease = %#v, want nil", job.Lease)
			}
			wantCompletedAt := tt.now.UTC()
			if job.CompletedAt == nil || !job.CompletedAt.Equal(wantCompletedAt) {
				t.Errorf("Job.CompletedAt = %v, want %v", job.CompletedAt, wantCompletedAt)
			}
			if job.CompletedAt != nil && job.CompletedAt.Location() != time.UTC {
				t.Errorf("Job.CompletedAt location = %v, want UTC", job.CompletedAt.Location())
			}
			if !reflect.DeepEqual(job.Result, tt.result) {
				t.Errorf("Job.Result = %s, want %s", job.Result, tt.result)
			}
		})
	}
}

func TestJobCompleteCopiesResult(t *testing.T) {
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	job := newTestJob(t, now, now)
	if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), now, now.Add(time.Hour)); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), now.Add(time.Minute)); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	result := json.RawMessage(`{"value":"original"}`)

	if err := job.Complete(WorkerID("worker-1"), LeaseToken("token-1"), now.Add(2*time.Minute), result); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	result[10] = 'X'
	if string(job.Result) != `{"value":"original"}` {
		t.Errorf("Job.Result = %q after source mutation, want an independent copy", job.Result)
	}
}

func TestJobFail(t *testing.T) {
	claimTime := time.Date(2026, time.August, 15, 8, 0, 0, 0, time.UTC)
	location := time.FixedZone("failure", 5*60*60+30*60)
	failureTime := claimTime.Add(5 * time.Minute).In(location)
	expiresAt := claimTime.Add(10 * time.Minute)
	retryAt := claimTime.Add(20 * time.Minute).In(location)

	leasedJob := func(t *testing.T, maxAttempts int, expiration time.Time) Job {
		t.Helper()
		job := newTestJob(t, claimTime, claimTime)
		job.MaxAttempts = maxAttempts
		if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, expiration); err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return job
	}
	runningJob := func(t *testing.T, maxAttempts int, expiration time.Time) Job {
		t.Helper()
		job := leasedJob(t, maxAttempts, expiration)
		if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), claimTime.Add(time.Minute)); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		return job
	}

	tests := []struct {
		name      string
		prepare   func(*testing.T) Job
		workerID  WorkerID
		token     LeaseToken
		now       time.Time
		message   string
		retryAt   *time.Time
		wantState State
		wantErr   bool
	}{
		{name: "retryable failure", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "temporary error", retryAt: &retryAt, wantState: StateRetryScheduled},
		{name: "exhausted attempts", prepare: func(t *testing.T) Job { return runningJob(t, 1, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "permanent error", retryAt: &retryAt, wantState: StateFailed},
		{name: "wrong state", prepare: func(t *testing.T) Job { return leasedJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true},
		{
			name: "missing lease",
			prepare: func(t *testing.T) Job {
				job := newTestJob(t, claimTime, claimTime)
				job.State = StateRunning
				job.AttemptsStarted = 1
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true,
		},
		{name: "wrong worker", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-2"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true},
		{name: "wrong token", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-2"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true},
		{name: "expired lease", prepare: func(t *testing.T) Job { return runningJob(t, 3, failureTime.Add(-time.Nanosecond)) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true},
		{name: "exact expiry boundary", prepare: func(t *testing.T) Job { return runningJob(t, 3, failureTime) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &retryAt, wantErr: true},
		{name: "zero failure time", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), message: "error", retryAt: &retryAt, wantErr: true},
		{name: "empty message", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, retryAt: &retryAt, wantErr: true},
		{name: "missing retry time", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", wantErr: true},
		{name: "zero retry time", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: new(time.Time), wantErr: true},
		{name: "retry at failure time", prepare: func(t *testing.T) Job { return runningJob(t, 3, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime, message: "error", retryAt: &failureTime, wantErr: true},
		{
			name: "terminal duplicate failure",
			prepare: func(t *testing.T) Job {
				job := runningJob(t, 1, expiresAt)
				if err := job.Fail(WorkerID("worker-1"), LeaseToken("token-1"), failureTime, "failed", nil); err != nil {
					t.Fatalf("initial Fail() error = %v", err)
				}
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: failureTime.Add(time.Second), message: "again", wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.prepare(t)
			before := cloneExecutionJob(job)

			err := job.Fail(tt.workerID, tt.token, tt.now, tt.message, tt.retryAt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Fail() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !reflect.DeepEqual(job, before) {
					t.Errorf("Fail() mutated job on failure: got %#v, want %#v", job, before)
				}
				return
			}

			if job.State != tt.wantState {
				t.Errorf("Job.State = %q, want %q", job.State, tt.wantState)
			}
			if job.Lease != nil || job.StartedAt != nil {
				t.Errorf("Fail() retained execution data: Lease=%#v StartedAt=%v", job.Lease, job.StartedAt)
			}
			if job.LastError != tt.message {
				t.Errorf("Job.LastError = %q, want %q", job.LastError, tt.message)
			}
			wantFailedAt := tt.now.UTC()
			if job.FailedAt == nil || !job.FailedAt.Equal(wantFailedAt) || job.FailedAt.Location() != time.UTC {
				t.Errorf("Job.FailedAt = %v, want UTC %v", job.FailedAt, wantFailedAt)
			}
			if tt.wantState == StateRetryScheduled && !job.AvailableAt.Equal(tt.retryAt.UTC()) {
				t.Errorf("Job.AvailableAt = %v, want %v", job.AvailableAt, tt.retryAt.UTC())
			}
			if tt.wantState == StateFailed && !job.AvailableAt.Equal(before.AvailableAt) {
				t.Errorf("terminal failure changed AvailableAt to %v, want %v", job.AvailableAt, before.AvailableAt)
			}
		})
	}
}

func TestJobClaimScheduledRetry(t *testing.T) {
	claimTime := time.Date(2026, time.August, 15, 8, 0, 0, 0, time.UTC)
	failureTime := claimTime.Add(2 * time.Minute)
	retryAt := failureTime.Add(10 * time.Minute)

	failedForRetry := func(t *testing.T) Job {
		t.Helper()
		job := newTestJob(t, claimTime, claimTime)
		if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, claimTime.Add(time.Hour)); err != nil {
			t.Fatalf("initial Claim() error = %v", err)
		}
		if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), claimTime.Add(time.Minute)); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		if err := job.Fail(WorkerID("worker-1"), LeaseToken("token-1"), failureTime, "temporary", &retryAt); err != nil {
			t.Fatalf("Fail() error = %v", err)
		}
		return job
	}

	tests := []struct {
		name    string
		now     time.Time
		wantErr bool
	}{
		{name: "premature retry claim", now: retryAt.Add(-time.Nanosecond), wantErr: true},
		{name: "eligible at retry time", now: retryAt},
		{name: "eligible after retry time", now: retryAt.Add(time.Second)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := failedForRetry(t)
			before := cloneExecutionJob(job)
			err := job.Claim(WorkerID("worker-2"), LeaseToken("token-2"), tt.now, tt.now.Add(time.Minute))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Claim() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !reflect.DeepEqual(job, before) {
				t.Errorf("Claim() mutated job on failure: got %#v, want %#v", job, before)
			}
			if !tt.wantErr {
				if job.State != StateLeased || job.AttemptsStarted != before.AttemptsStarted {
					t.Errorf("Claim() job state/count = %q/%d, want %q/%d", job.State, job.AttemptsStarted, StateLeased, before.AttemptsStarted)
				}
			}
		})
	}
}

func cloneExecutionJob(job Job) Job {
	if job.Lease != nil {
		lease := *job.Lease
		job.Lease = &lease
	}
	if job.StartedAt != nil {
		startedAt := *job.StartedAt
		job.StartedAt = &startedAt
	}
	if job.CompletedAt != nil {
		completedAt := *job.CompletedAt
		job.CompletedAt = &completedAt
	}
	if job.FailedAt != nil {
		failedAt := *job.FailedAt
		job.FailedAt = &failedAt
	}
	job.Result = append(json.RawMessage(nil), job.Result...)
	return job
}

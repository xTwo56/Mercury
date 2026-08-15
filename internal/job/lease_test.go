package job

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestJobClaim(t *testing.T) {
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	location := time.FixedZone("claim", 2*60*60)
	expiresAt := now.Add(5 * time.Minute).In(location)

	tests := []struct {
		name            string
		availableAt     time.Time
		workerID        WorkerID
		token           LeaseToken
		now             time.Time
		expiresAt       time.Time
		claimed         bool
		attemptsStarted int
		wantErr         bool
	}{
		{name: "available now", availableAt: now, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: expiresAt},
		{name: "available in past", availableAt: now.Add(-time.Minute), workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: expiresAt},
		{name: "premature claim", availableAt: now.Add(time.Nanosecond), workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: expiresAt, wantErr: true},
		{name: "duplicate claim", availableAt: now, workerID: WorkerID("worker-2"), token: LeaseToken("token-2"), now: now, expiresAt: expiresAt, claimed: true, wantErr: true},
		{name: "empty worker", availableAt: now, token: LeaseToken("token-1"), now: now, expiresAt: expiresAt, wantErr: true},
		{name: "empty token", availableAt: now, workerID: WorkerID("worker-1"), now: now, expiresAt: expiresAt, wantErr: true},
		{name: "zero current time", availableAt: now, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), expiresAt: expiresAt, wantErr: true},
		{name: "expiration equal to now", availableAt: now, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: now, wantErr: true},
		{name: "expiration before now", availableAt: now, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: now.Add(-time.Nanosecond), wantErr: true},
		{name: "exhausted attempts", availableAt: now, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, expiresAt: expiresAt, attemptsStarted: 3, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := newTestJob(t, now.Add(-time.Hour), tt.availableAt)
			job.AttemptsStarted = tt.attemptsStarted
			if tt.claimed {
				if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), now, expiresAt); err != nil {
					t.Fatalf("initial Claim() error = %v", err)
				}
			}
			before := job

			err := job.Claim(tt.workerID, tt.token, tt.now, tt.expiresAt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Claim() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !reflect.DeepEqual(job, before) {
					t.Errorf("Claim() mutated job on failure: got %#v, want %#v", job, before)
				}
				return
			}

			if job.State != StateLeased {
				t.Errorf("Job.State = %q, want %q", job.State, StateLeased)
			}
			if job.AttemptsStarted != before.AttemptsStarted {
				t.Errorf("Job.AttemptsStarted = %d, want unchanged value %d", job.AttemptsStarted, before.AttemptsStarted)
			}
			wantLease := &Lease{WorkerID: tt.workerID, Token: tt.token, ExpiresAt: tt.expiresAt.UTC()}
			if !reflect.DeepEqual(job.Lease, wantLease) {
				t.Errorf("Job.Lease = %#v, want %#v", job.Lease, wantLease)
			}
		})
	}
}

func TestJobValidateLease(t *testing.T) {
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	expiresAt := now.Add(5 * time.Minute)
	job := newTestJob(t, now, now)
	if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), now, expiresAt); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	unleasedJob := newTestJob(t, now, now)

	tests := []struct {
		name     string
		job      Job
		workerID WorkerID
		token    LeaseToken
		now      time.Time
		wantErr  bool
	}{
		{name: "valid lease", job: job, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: expiresAt.Add(-time.Nanosecond)},
		{name: "wrong worker", job: job, workerID: WorkerID("worker-2"), token: LeaseToken("token-1"), now: now, wantErr: true},
		{name: "wrong token", job: job, workerID: WorkerID("worker-1"), token: LeaseToken("token-2"), now: now, wantErr: true},
		{name: "expired at boundary", job: job, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: expiresAt, wantErr: true},
		{name: "expired after boundary", job: job, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: expiresAt.Add(time.Nanosecond), wantErr: true},
		{name: "job without lease", job: unleasedJob, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: now, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.ValidateLease(tt.workerID, tt.token, tt.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateLease() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJobRecoverExpiredLease(t *testing.T) {
	claimTime := time.Date(2026, time.August, 16, 8, 0, 0, 0, time.UTC)
	expiresAt := claimTime.Add(5 * time.Minute)
	location := time.FixedZone("recovery", 5*60*60+30*60)
	recoveryTime := expiresAt.In(location)
	retryAt := expiresAt.Add(10 * time.Minute).In(location)

	leasedJob := func(t *testing.T, maxAttempts int) Job {
		t.Helper()
		job := newTestJob(t, claimTime, claimTime)
		job.MaxAttempts = maxAttempts
		if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, expiresAt); err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return job
	}
	runningJob := func(t *testing.T, maxAttempts int) Job {
		t.Helper()
		job := leasedJob(t, maxAttempts)
		if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), claimTime.Add(time.Minute)); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		return job
	}

	tests := []struct {
		name          string
		prepare       func(*testing.T) Job
		now           time.Time
		retryAt       time.Time
		wantState     State
		wantAvailable time.Time
		wantErr       bool
	}{
		{name: "leased recovery at exact expiry", prepare: func(t *testing.T) Job { return leasedJob(t, 3) }, now: recoveryTime, wantState: StateQueued, wantAvailable: recoveryTime.UTC()},
		{name: "leased recovery after expiry", prepare: func(t *testing.T) Job { return leasedJob(t, 3) }, now: recoveryTime.Add(time.Second), wantState: StateQueued, wantAvailable: recoveryTime.Add(time.Second).UTC()},
		{name: "running retry", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, now: recoveryTime, retryAt: retryAt, wantState: StateRetryScheduled, wantAvailable: retryAt.UTC()},
		{name: "running exhausted", prepare: func(t *testing.T) Job { return runningJob(t, 1) }, now: recoveryTime, wantState: StateFailed},
		{name: "before expiry", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, now: expiresAt.Add(-time.Nanosecond), retryAt: retryAt, wantErr: true},
		{name: "zero current time", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, retryAt: retryAt, wantErr: true},
		{name: "zero retry time", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, now: recoveryTime, wantErr: true},
		{name: "retry at recovery time", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, now: recoveryTime, retryAt: recoveryTime, wantErr: true},
		{name: "retry before recovery time", prepare: func(t *testing.T) Job { return runningJob(t, 3) }, now: recoveryTime, retryAt: recoveryTime.Add(-time.Nanosecond), wantErr: true},
		{name: "queued without lease", prepare: func(t *testing.T) Job { return newTestJob(t, claimTime, claimTime) }, now: recoveryTime, retryAt: retryAt, wantErr: true},
		{
			name: "invalid succeeded state",
			prepare: func(t *testing.T) Job {
				job := leasedJob(t, 3)
				job.State = StateSucceeded
				return job
			},
			now: recoveryTime, retryAt: retryAt, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.prepare(t)
			before := cloneExecutionJob(job)

			err := job.RecoverExpiredLease(tt.now, tt.retryAt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("RecoverExpiredLease() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !reflect.DeepEqual(job, before) {
					t.Errorf("RecoverExpiredLease() mutated job on failure: got %#v, want %#v", job, before)
				}
				return
			}

			if job.State != tt.wantState {
				t.Errorf("Job.State = %q, want %q", job.State, tt.wantState)
			}
			if job.Lease != nil {
				t.Errorf("Job.Lease = %#v, want nil", job.Lease)
			}
			if job.AttemptsStarted != before.AttemptsStarted {
				t.Errorf("Job.AttemptsStarted = %d, want unchanged value %d", job.AttemptsStarted, before.AttemptsStarted)
			}
			if !tt.wantAvailable.IsZero() && !job.AvailableAt.Equal(tt.wantAvailable) {
				t.Errorf("Job.AvailableAt = %v, want %v", job.AvailableAt, tt.wantAvailable)
			}
			if !tt.wantAvailable.IsZero() && job.AvailableAt.Location() != time.UTC {
				t.Errorf("Job.AvailableAt location = %v, want UTC", job.AvailableAt.Location())
			}
			if tt.wantState == StateQueued {
				if job.StartedAt != nil || job.FailedAt != nil || job.LastError != "" {
					t.Errorf("leased recovery recorded execution failure: StartedAt=%v FailedAt=%v LastError=%q", job.StartedAt, job.FailedAt, job.LastError)
				}
				return
			}
			if job.StartedAt != nil {
				t.Errorf("Job.StartedAt = %v, want nil", job.StartedAt)
			}
			if job.LastError != "lease expired" {
				t.Errorf("Job.LastError = %q, want %q", job.LastError, "lease expired")
			}
			wantFailedAt := tt.now.UTC()
			if job.FailedAt == nil || !job.FailedAt.Equal(wantFailedAt) || job.FailedAt.Location() != time.UTC {
				t.Errorf("Job.FailedAt = %v, want UTC %v", job.FailedAt, wantFailedAt)
			}
			if tt.wantState == StateFailed && !job.AvailableAt.Equal(before.AvailableAt) {
				t.Errorf("terminal recovery changed AvailableAt to %v, want %v", job.AvailableAt, before.AvailableAt)
			}
		})
	}
}

func TestJobRejectsStaleCompletionAfterLeaseRecovery(t *testing.T) {
	claimTime := time.Date(2026, time.August, 16, 8, 0, 0, 0, time.UTC)
	expiresAt := claimTime.Add(5 * time.Minute)
	retryAt := expiresAt.Add(time.Minute)

	tests := []struct {
		name        string
		maxAttempts int
	}{
		{name: "retry scheduled", maxAttempts: 3},
		{name: "terminal failure", maxAttempts: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := newTestJob(t, claimTime, claimTime)
			job.MaxAttempts = tt.maxAttempts
			if err := job.Claim(WorkerID("worker-1"), LeaseToken("token-1"), claimTime, expiresAt); err != nil {
				t.Fatalf("Claim() error = %v", err)
			}
			if err := job.Start(WorkerID("worker-1"), LeaseToken("token-1"), claimTime.Add(time.Minute)); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if err := job.RecoverExpiredLease(expiresAt, retryAt); err != nil {
				t.Fatalf("RecoverExpiredLease() error = %v", err)
			}
			before := cloneExecutionJob(job)

			err := job.Complete(WorkerID("worker-1"), LeaseToken("token-1"), expiresAt.Add(time.Nanosecond), json.RawMessage(`{}`))
			if err == nil {
				t.Fatal("Complete() error = nil, want stale completion rejected")
			}
			if !reflect.DeepEqual(job, before) {
				t.Errorf("Complete() mutated recovered job: got %#v, want %#v", job, before)
			}
		})
	}
}

func newTestJob(t *testing.T, createdAt, availableAt time.Time) Job {
	t.Helper()
	job, err := New(JobID("job-1"), TaskType("test"), json.RawMessage(`{}`), 3, createdAt, availableAt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return job
}

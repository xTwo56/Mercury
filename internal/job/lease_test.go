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
		name        string
		availableAt time.Time
		workerID    WorkerID
		token       LeaseToken
		now         time.Time
		expiresAt   time.Time
		claimed     bool
		wantErr     bool
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := newTestJob(t, now.Add(-time.Hour), tt.availableAt)
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

func newTestJob(t *testing.T, createdAt, availableAt time.Time) Job {
	t.Helper()
	job, err := New(JobID("job-1"), TaskType("test"), json.RawMessage(`{}`), createdAt, availableAt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return job
}

package job

import (
	"reflect"
	"testing"
	"time"
)

func TestJobRenewLease(t *testing.T) {
	claimTime := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	heartbeatTime := claimTime.Add(5 * time.Minute)
	expiresAt := claimTime.Add(10 * time.Minute)
	location := time.FixedZone("renewal", 5*60*60+30*60)
	newExpiresAt := claimTime.Add(20 * time.Minute).In(location)

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
		name         string
		prepare      func(*testing.T) Job
		workerID     WorkerID
		token        LeaseToken
		now          time.Time
		newExpiresAt time.Time
		wantErr      bool
	}{
		{name: "successful renewal", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt},
		{name: "wrong state", prepare: func(t *testing.T) Job { return leasedJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true},
		{
			name: "missing lease",
			prepare: func(t *testing.T) Job {
				job := newTestJob(t, claimTime, claimTime)
				job.State = StateRunning
				return job
			},
			workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true,
		},
		{name: "wrong worker", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-2"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true},
		{name: "wrong token", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-2"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true},
		{name: "expired lease", prepare: func(t *testing.T) Job { return runningJob(t, heartbeatTime.Add(-time.Nanosecond)) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true},
		{name: "exact expiry boundary", prepare: func(t *testing.T) Job { return runningJob(t, heartbeatTime) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: newExpiresAt, wantErr: true},
		{name: "expiration equal to existing", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: expiresAt, wantErr: true},
		{name: "expiration before existing", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: expiresAt.Add(-time.Nanosecond), wantErr: true},
		{name: "expiration not after now", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, newExpiresAt: heartbeatTime, wantErr: true},
		{name: "zero current time", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), newExpiresAt: newExpiresAt, wantErr: true},
		{name: "zero new expiration", prepare: func(t *testing.T) Job { return runningJob(t, expiresAt) }, workerID: WorkerID("worker-1"), token: LeaseToken("token-1"), now: heartbeatTime, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.prepare(t)
			before := cloneJobWithLease(job)
			want := cloneJobWithLease(job)
			if !tt.wantErr {
				want.Lease.ExpiresAt = tt.newExpiresAt.UTC()
			}

			err := job.RenewLease(tt.workerID, tt.token, tt.now, tt.newExpiresAt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("RenewLease() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !reflect.DeepEqual(job, before) {
				t.Errorf("RenewLease() mutated job on failure: got %#v, want %#v", job, before)
			}
			if !tt.wantErr && !reflect.DeepEqual(job, want) {
				t.Errorf("RenewLease() job = %#v, want only expiration changed: %#v", job, want)
			}
		})
	}
}

func cloneJobWithLease(job Job) Job {
	if job.Lease != nil {
		lease := *job.Lease
		job.Lease = &lease
	}
	return job
}

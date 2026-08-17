package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtwo56/mercury/internal/job"
)

const testDatabaseEnvironment = "MERCURY_TEST_DATABASE_URL"

func TestJobRepositoryIntegration(t *testing.T) {
	databaseURL := os.Getenv(testDatabaseEnvironment)
	if databaseURL == "" {
		t.Skip("set MERCURY_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close test database connection: %v", err)
		}
	})

	schema := "mercury_test_" + time.Now().UTC().Format("20060102_150405_000000000")
	qualifiedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+qualifiedSchema); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+qualifiedSchema+" CASCADE"); err != nil {
			t.Errorf("drop isolated test schema: %v", err)
		}
	})
	if _, err := conn.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
		t.Fatalf("set test search path: %v", err)
	}

	migration, err := os.ReadFile(upMigrationPath(t))
	if err != nil {
		t.Fatalf("read jobs migration: %v", err)
	}
	if _, err := conn.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply jobs migration: %v", err)
	}

	repository := NewJobRepository(conn)
	zone := time.FixedZone("integration", 5*60*60+30*60)
	createdAt := time.Date(2026, time.August, 16, 12, 0, 0, 123000000, zone)

	t.Run("insertion", func(t *testing.T) {
		j := integrationJob("inserted", createdAt)
		if err := repository.Create(ctx, j); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE id = $1", string(j.ID)).Scan(&count); err != nil {
			t.Fatalf("count inserted job: %v", err)
		}
		if count != 1 {
			t.Fatalf("inserted row count = %d, want 1", count)
		}
	})

	t.Run("complete round trip", func(t *testing.T) {
		startedAt := createdAt.Add(time.Minute)
		completedAt := createdAt.Add(2 * time.Minute)
		failedAt := createdAt.Add(3 * time.Minute)
		leaseExpiresAt := createdAt.Add(10 * time.Minute)

		tests := []struct {
			name string
			job  job.Job
		}{
			{name: "nullable fields", job: integrationJob("roundtrip-queued", createdAt)},
			{
				name: "active lease",
				job: job.Job{
					ID: job.JobID("roundtrip-running"), TaskType: job.TaskType("render"),
					Payload: json.RawMessage(`{"frame":1}`), State: job.StateRunning,
					MaxAttempts: 4, AttemptsStarted: 2, CreatedAt: createdAt,
					AvailableAt: createdAt, StartedAt: &startedAt,
					Lease: &job.Lease{WorkerID: job.WorkerID("worker-1"), Token: job.LeaseToken("token-1"), ExpiresAt: leaseExpiresAt},
				},
			},
			{
				name: "successful result",
				job: job.Job{
					ID: job.JobID("roundtrip-succeeded"), TaskType: job.TaskType("render"),
					Payload: json.RawMessage(`{"frame":2}`), State: job.StateSucceeded,
					MaxAttempts: 4, AttemptsStarted: 1, CreatedAt: createdAt,
					AvailableAt: createdAt, StartedAt: &startedAt, CompletedAt: &completedAt,
					Result: json.RawMessage(`{"url":"https://example.test/result"}`),
				},
			},
			{
				name: "terminal failure",
				job: job.Job{
					ID: job.JobID("roundtrip-failed"), TaskType: job.TaskType("render"),
					Payload: json.RawMessage(`{"frame":3}`), State: job.StateFailed,
					MaxAttempts: 2, AttemptsStarted: 2, CreatedAt: createdAt,
					AvailableAt: createdAt, LastError: "lease expired", FailedAt: &failedAt,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if err := repository.Create(ctx, tt.job); err != nil {
					t.Fatalf("Create() error = %v", err)
				}
				got, err := repository.GetByID(ctx, tt.job.ID)
				if err != nil {
					t.Fatalf("GetByID() error = %v", err)
				}
				assertJobEqual(t, got, normalizedJob(tt.job))
			})
		}
	})

	t.Run("missing job", func(t *testing.T) {
		_, err := repository.GetByID(ctx, job.JobID("missing"))
		if !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("GetByID() error = %v, want ErrJobNotFound", err)
		}
	})

	t.Run("duplicate ID", func(t *testing.T) {
		j := integrationJob("duplicate", createdAt)
		if err := repository.Create(ctx, j); err != nil {
			t.Fatalf("first Create() error = %v", err)
		}
		if err := repository.Create(ctx, j); err == nil {
			t.Fatal("second Create() error = nil, want duplicate-key error")
		}
	})

	truncateJobs := func(t *testing.T) {
		t.Helper()
		if _, err := conn.Exec(ctx, "TRUNCATE jobs"); err != nil {
			t.Fatalf("truncate jobs: %v", err)
		}
	}
	claimNow := createdAt.Add(time.Hour)
	claimExpiresAt := claimNow.Add(5 * time.Minute)

	t.Run("claim next", func(t *testing.T) {
		truncateJobs(t)
		later := integrationJob("claim-later", claimNow.Add(-10*time.Minute))
		later.AvailableAt = claimNow.Add(-time.Minute)
		earliest := integrationJob("claim-earliest", claimNow.Add(-20*time.Minute))
		earliest.AvailableAt = claimNow.Add(-2 * time.Minute)
		for _, j := range []job.Job{later, earliest} {
			if err := repository.Create(ctx, j); err != nil {
				t.Fatalf("Create(%q) error = %v", j.ID, err)
			}
		}

		claimed, err := repository.ClaimNext(ctx, job.WorkerID("worker-1"), job.LeaseToken("token-1"), claimNow, claimExpiresAt)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
		}
		if claimed.ID != earliest.ID {
			t.Errorf("ClaimNext() ID = %q, want %q", claimed.ID, earliest.ID)
		}
		if claimed.State != job.StateLeased || claimed.Lease == nil {
			t.Fatalf("ClaimNext() state/lease = %q/%#v, want leased job", claimed.State, claimed.Lease)
		}
		if claimed.Lease.WorkerID != job.WorkerID("worker-1") || claimed.Lease.Token != job.LeaseToken("token-1") || !claimed.Lease.ExpiresAt.Equal(claimExpiresAt) {
			t.Errorf("ClaimNext() lease = %#v, want supplied lease values", claimed.Lease)
		}
		if claimed.AttemptsStarted != 0 {
			t.Errorf("ClaimNext() AttemptsStarted = %d, want 0", claimed.AttemptsStarted)
		}

		persisted, err := repository.GetByID(ctx, claimed.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		assertJobEqual(t, persisted, normalizedJob(claimed))
	})

	t.Run("claim retry scheduled job", func(t *testing.T) {
		truncateJobs(t)
		j := integrationJob("retry-ready", claimNow.Add(-time.Hour))
		j.State = job.StateRetryScheduled
		j.AvailableAt = claimNow
		j.AttemptsStarted = 1
		if err := repository.Create(ctx, j); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		claimed, err := repository.ClaimNext(ctx, job.WorkerID("worker-1"), job.LeaseToken("token-1"), claimNow, claimExpiresAt)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
		}
		if claimed.ID != j.ID || claimed.State != job.StateLeased || claimed.AttemptsStarted != j.AttemptsStarted {
			t.Errorf("ClaimNext() job = %#v, want retry job leased without consuming an attempt", claimed)
		}
	})

	t.Run("skip future job", func(t *testing.T) {
		truncateJobs(t)
		future := integrationJob("future", claimNow)
		future.AvailableAt = claimNow.Add(time.Minute)
		ready := integrationJob("ready", claimNow.Add(-time.Minute))
		ready.AvailableAt = claimNow
		for _, j := range []job.Job{future, ready} {
			if err := repository.Create(ctx, j); err != nil {
				t.Fatalf("Create(%q) error = %v", j.ID, err)
			}
		}

		claimed, err := repository.ClaimNext(ctx, job.WorkerID("worker-1"), job.LeaseToken("token-1"), claimNow, claimExpiresAt)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
		}
		if claimed.ID != ready.ID {
			t.Errorf("ClaimNext() ID = %q, want %q", claimed.ID, ready.ID)
		}
	})

	t.Run("skip non-claimable state", func(t *testing.T) {
		truncateJobs(t)
		terminal := integrationJob("succeeded", claimNow.Add(-time.Hour))
		terminal.State = job.StateSucceeded
		terminal.AttemptsStarted = 1
		ready := integrationJob("queued", claimNow.Add(-time.Minute))
		for _, j := range []job.Job{terminal, ready} {
			if err := repository.Create(ctx, j); err != nil {
				t.Fatalf("Create(%q) error = %v", j.ID, err)
			}
		}

		claimed, err := repository.ClaimNext(ctx, job.WorkerID("worker-1"), job.LeaseToken("token-1"), claimNow, claimExpiresAt)
		if err != nil {
			t.Fatalf("ClaimNext() error = %v", err)
		}
		if claimed.ID != ready.ID {
			t.Errorf("ClaimNext() ID = %q, want %q", claimed.ID, ready.ID)
		}
	})

	t.Run("empty queue", func(t *testing.T) {
		truncateJobs(t)
		_, err := repository.ClaimNext(ctx, job.WorkerID("worker-1"), job.LeaseToken("token-1"), claimNow, claimExpiresAt)
		if !errors.Is(err, ErrNoJobAvailable) {
			t.Fatalf("ClaimNext() error = %v, want ErrNoJobAvailable", err)
		}
	})

	t.Run("concurrent workers claim one job", func(t *testing.T) {
		truncateJobs(t)
		j := integrationJob("contended", claimNow.Add(-time.Minute))
		if err := repository.Create(ctx, j); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		secondConn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect second worker: %v", err)
		}
		defer func() {
			if err := secondConn.Close(ctx); err != nil {
				t.Errorf("close second worker connection: %v", err)
			}
		}()
		if _, err := secondConn.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
			t.Fatalf("set second worker search path: %v", err)
		}
		secondRepository := NewJobRepository(secondConn)

		type claimResult struct {
			job job.Job
			err error
		}
		start := make(chan struct{})
		results := make(chan claimResult, 2)
		claim := func(repository *JobRepository, workerID job.WorkerID, token job.LeaseToken) {
			<-start
			claimed, err := repository.ClaimNext(ctx, workerID, token, claimNow, claimExpiresAt)
			results <- claimResult{job: claimed, err: err}
		}
		go claim(repository, job.WorkerID("worker-1"), job.LeaseToken("token-1"))
		go claim(secondRepository, job.WorkerID("worker-2"), job.LeaseToken("token-2"))
		close(start)

		var successes, unavailable int
		for range 2 {
			result := <-results
			switch {
			case result.err == nil:
				successes++
				if result.job.ID != j.ID {
					t.Errorf("ClaimNext() ID = %q, want %q", result.job.ID, j.ID)
				}
			case errors.Is(result.err, ErrNoJobAvailable):
				unavailable++
			default:
				t.Errorf("ClaimNext() unexpected error = %v", result.err)
			}
		}
		if successes != 1 || unavailable != 1 {
			t.Errorf("concurrent outcomes = %d successes, %d unavailable; want 1 and 1", successes, unavailable)
		}
	})

	startNow := claimNow.Add(time.Minute)
	startLeaseExpiresAt := startNow.Add(5 * time.Minute)
	leasableJob := func(id string, state job.State, attemptsStarted, maxAttempts int, leaseExpiresAt time.Time) job.Job {
		j := integrationJob(id, claimNow.Add(-time.Hour))
		j.State = state
		j.AttemptsStarted = attemptsStarted
		j.MaxAttempts = maxAttempts
		j.Lease = &job.Lease{
			WorkerID:  job.WorkerID("worker-1"),
			Token:     job.LeaseToken("token-1"),
			ExpiresAt: leaseExpiresAt,
		}
		return j
	}

	t.Run("start execution", func(t *testing.T) {
		truncateJobs(t)
		leased := leasableJob("start-success", job.StateLeased, 1, 3, startLeaseExpiresAt)
		if err := repository.Create(ctx, leased); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		started, err := repository.StartExecution(ctx, leased.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), startNow)
		if err != nil {
			t.Fatalf("StartExecution() error = %v", err)
		}
		if started.State != job.StateRunning {
			t.Errorf("StartExecution() state = %q, want %q", started.State, job.StateRunning)
		}
		if started.AttemptsStarted != leased.AttemptsStarted+1 {
			t.Errorf("StartExecution() AttemptsStarted = %d, want %d", started.AttemptsStarted, leased.AttemptsStarted+1)
		}
		if started.StartedAt == nil || !started.StartedAt.Equal(startNow) || started.StartedAt.Location() != time.UTC {
			t.Errorf("StartExecution() StartedAt = %v, want UTC %v", started.StartedAt, startNow.UTC())
		}
		if !reflect.DeepEqual(started.Lease, normalizedJob(leased).Lease) {
			t.Errorf("StartExecution() lease = %#v, want preserved lease %#v", started.Lease, leased.Lease)
		}

		persisted, err := repository.GetByID(ctx, leased.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		assertJobEqual(t, persisted, normalizedJob(started))
	})

	t.Run("start missing job", func(t *testing.T) {
		truncateJobs(t)
		_, err := repository.StartExecution(ctx, job.JobID("missing"), job.WorkerID("worker-1"), job.LeaseToken("token-1"), startNow)
		if !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("StartExecution() error = %v, want ErrJobNotFound", err)
		}
	})

	t.Run("rejected starts roll back", func(t *testing.T) {
		tests := []struct {
			name     string
			job      job.Job
			workerID job.WorkerID
			token    job.LeaseToken
			now      time.Time
		}{
			{name: "wrong worker", job: leasableJob("start-wrong-worker", job.StateLeased, 0, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-2"), token: job.LeaseToken("token-1"), now: startNow},
			{name: "wrong token", job: leasableJob("start-wrong-token", job.StateLeased, 0, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-2"), now: startNow},
			{name: "expired lease", job: leasableJob("start-expired", job.StateLeased, 0, 3, startNow.Add(-time.Microsecond)), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: startNow},
			{name: "exact expiry", job: leasableJob("start-at-expiry", job.StateLeased, 0, 3, startNow), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: startNow},
			{name: "invalid source state", job: leasableJob("start-running", job.StateRunning, 1, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: startNow},
			{name: "exhausted attempts", job: leasableJob("start-exhausted", job.StateLeased, 3, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: startNow},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				truncateJobs(t)
				if err := repository.Create(ctx, tt.job); err != nil {
					t.Fatalf("Create() error = %v", err)
				}

				if _, err := repository.StartExecution(ctx, tt.job.ID, tt.workerID, tt.token, tt.now); err == nil {
					t.Fatal("StartExecution() error = nil, want rejection")
				}
				persisted, err := repository.GetByID(ctx, tt.job.ID)
				if err != nil {
					t.Fatalf("GetByID() error = %v", err)
				}
				assertJobEqual(t, persisted, normalizedJob(tt.job))
			})
		}
	})

	t.Run("concurrent duplicate starts", func(t *testing.T) {
		truncateJobs(t)
		leased := leasableJob("start-contended", job.StateLeased, 0, 3, startLeaseExpiresAt)
		if err := repository.Create(ctx, leased); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		secondConn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect second starter: %v", err)
		}
		defer func() {
			if err := secondConn.Close(ctx); err != nil {
				t.Errorf("close second starter connection: %v", err)
			}
		}()
		if _, err := secondConn.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
			t.Fatalf("set second starter search path: %v", err)
		}
		secondRepository := NewJobRepository(secondConn)

		type startResult struct {
			job job.Job
			err error
		}
		start := make(chan struct{})
		results := make(chan startResult, 2)
		startExecution := func(repository *JobRepository) {
			<-start
			started, err := repository.StartExecution(ctx, leased.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), startNow)
			results <- startResult{job: started, err: err}
		}
		go startExecution(repository)
		go startExecution(secondRepository)
		close(start)

		var successes, rejections int
		for range 2 {
			result := <-results
			if result.err == nil {
				successes++
				if result.job.AttemptsStarted != 1 {
					t.Errorf("successful StartExecution() AttemptsStarted = %d, want 1", result.job.AttemptsStarted)
				}
			} else {
				rejections++
			}
		}
		if successes != 1 || rejections != 1 {
			t.Errorf("concurrent outcomes = %d successes, %d rejections; want 1 and 1", successes, rejections)
		}

		persisted, err := repository.GetByID(ctx, leased.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if persisted.State != job.StateRunning || persisted.AttemptsStarted != 1 {
			t.Errorf("persisted state/count = %q/%d, want %q/1", persisted.State, persisted.AttemptsStarted, job.StateRunning)
		}
	})

	renewNow := startNow.Add(time.Minute)
	renewedExpiresAt := startLeaseExpiresAt.Add(5 * time.Minute)
	runningLeasedJob := func(id string, leaseExpiresAt time.Time) job.Job {
		j := leasableJob(id, job.StateRunning, 1, 3, leaseExpiresAt)
		startedAt := startNow
		j.StartedAt = &startedAt
		return j
	}

	t.Run("renew lease", func(t *testing.T) {
		truncateJobs(t)
		running := runningLeasedJob("renew-success", startLeaseExpiresAt)
		if err := repository.Create(ctx, running); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		renewed, err := repository.RenewLease(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), renewNow, renewedExpiresAt)
		if err != nil {
			t.Fatalf("RenewLease() error = %v", err)
		}
		want := normalizedJob(running)
		want.Lease.ExpiresAt = renewedExpiresAt.UTC()
		assertJobEqual(t, renewed, want)
		if renewed.Lease.ExpiresAt.Location() != time.UTC {
			t.Errorf("RenewLease() expiration location = %v, want UTC", renewed.Lease.ExpiresAt.Location())
		}

		persisted, err := repository.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		assertJobEqual(t, persisted, want)
	})

	t.Run("renew missing job lease", func(t *testing.T) {
		truncateJobs(t)
		_, err := repository.RenewLease(ctx, job.JobID("missing"), job.WorkerID("worker-1"), job.LeaseToken("token-1"), renewNow, renewedExpiresAt)
		if !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("RenewLease() error = %v, want ErrJobNotFound", err)
		}
	})

	t.Run("rejected renewals roll back", func(t *testing.T) {
		zeroTime := time.Time{}
		tests := []struct {
			name         string
			job          job.Job
			workerID     job.WorkerID
			token        job.LeaseToken
			now          time.Time
			newExpiresAt time.Time
		}{
			{name: "non-running state", job: leasableJob("renew-leased", job.StateLeased, 0, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewedExpiresAt},
			{name: "wrong worker", job: runningLeasedJob("renew-wrong-worker", startLeaseExpiresAt), workerID: job.WorkerID("worker-2"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewedExpiresAt},
			{name: "wrong token", job: runningLeasedJob("renew-wrong-token", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-2"), now: renewNow, newExpiresAt: renewedExpiresAt},
			{name: "expired lease", job: runningLeasedJob("renew-expired", renewNow.Add(-time.Microsecond)), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewedExpiresAt},
			{name: "exact expiry", job: runningLeasedJob("renew-at-expiry", renewNow), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewedExpiresAt},
			{name: "zero current time", job: runningLeasedJob("renew-zero-now", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: zeroTime, newExpiresAt: renewedExpiresAt},
			{name: "zero new expiration", job: runningLeasedJob("renew-zero-expiration", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: zeroTime},
			{name: "expiration equal to now", job: runningLeasedJob("renew-equal-now", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewNow},
			{name: "expiration before now", job: runningLeasedJob("renew-before-now", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: renewNow.Add(-time.Microsecond)},
			{name: "retain expiration", job: runningLeasedJob("renew-retain", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: startLeaseExpiresAt},
			{name: "shorten expiration", job: runningLeasedJob("renew-shorten", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), now: renewNow, newExpiresAt: startLeaseExpiresAt.Add(-time.Microsecond)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				truncateJobs(t)
				if err := repository.Create(ctx, tt.job); err != nil {
					t.Fatalf("Create() error = %v", err)
				}

				if _, err := repository.RenewLease(ctx, tt.job.ID, tt.workerID, tt.token, tt.now, tt.newExpiresAt); err == nil {
					t.Fatal("RenewLease() error = nil, want rejection")
				}
				persisted, err := repository.GetByID(ctx, tt.job.ID)
				if err != nil {
					t.Fatalf("GetByID() error = %v", err)
				}
				assertJobEqual(t, persisted, normalizedJob(tt.job))
			})
		}
	})

	t.Run("concurrent lease renewals never shorten or change ownership", func(t *testing.T) {
		truncateJobs(t)
		running := runningLeasedJob("renew-contended", startLeaseExpiresAt)
		if err := repository.Create(ctx, running); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		secondConn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect second heartbeat: %v", err)
		}
		defer func() {
			if err := secondConn.Close(ctx); err != nil {
				t.Errorf("close second heartbeat connection: %v", err)
			}
		}()
		if _, err := secondConn.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
			t.Fatalf("set second heartbeat search path: %v", err)
		}
		secondRepository := NewJobRepository(secondConn)

		shorterExtension := startLeaseExpiresAt.Add(5 * time.Minute)
		longerExtension := startLeaseExpiresAt.Add(10 * time.Minute)
		start := make(chan struct{})
		results := make(chan error, 2)
		renew := func(repository *JobRepository, expiration time.Time) {
			<-start
			_, err := repository.RenewLease(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), renewNow, expiration)
			results <- err
		}
		go renew(repository, shorterExtension)
		go renew(secondRepository, longerExtension)
		close(start)

		var successes int
		for range 2 {
			if err := <-results; err == nil {
				successes++
			}
		}
		if successes < 1 {
			t.Fatal("concurrent RenewLease() calls both failed, want at least the longest extension to succeed")
		}

		persisted, err := repository.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if !persisted.Lease.ExpiresAt.Equal(longerExtension) {
			t.Errorf("persisted expiration = %v, want longest extension %v", persisted.Lease.ExpiresAt, longerExtension)
		}
		if persisted.Lease.WorkerID != running.Lease.WorkerID || persisted.Lease.Token != running.Lease.Token {
			t.Errorf("persisted owner/token = %q/%q, want %q/%q", persisted.Lease.WorkerID, persisted.Lease.Token, running.Lease.WorkerID, running.Lease.Token)
		}
		if persisted.State != running.State || persisted.AttemptsStarted != running.AttemptsStarted || !reflect.DeepEqual(persisted.StartedAt, normalizedJob(running).StartedAt) {
			t.Errorf("renewal changed execution fields: got state/count/start %q/%d/%v", persisted.State, persisted.AttemptsStarted, persisted.StartedAt)
		}
	})

	completeNow := renewNow
	t.Run("complete execution", func(t *testing.T) {
		truncateJobs(t)
		running := runningLeasedJob("complete-success", startLeaseExpiresAt)
		if err := repository.Create(ctx, running); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		result := json.RawMessage(`{"output":"accepted"}`)

		completed, err := repository.CompleteExecution(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), result, completeNow)
		if err != nil {
			t.Fatalf("CompleteExecution() error = %v", err)
		}
		want := normalizedJob(running)
		want.State = job.StateSucceeded
		want.Lease = nil
		want.Result = json.RawMessage(`{"output":"accepted"}`)
		want.CompletedAt = utcPointer(&completeNow)
		assertJobEqual(t, completed, want)
		if completed.CompletedAt == nil || completed.CompletedAt.Location() != time.UTC {
			t.Errorf("CompleteExecution() CompletedAt = %v, want UTC", completed.CompletedAt)
		}
		if completed.AttemptsStarted != running.AttemptsStarted {
			t.Errorf("CompleteExecution() AttemptsStarted = %d, want unchanged %d", completed.AttemptsStarted, running.AttemptsStarted)
		}
		result[2] = 'X'
		if !jsonEqual(completed.Result, want.Result) {
			t.Errorf("CompleteExecution() result changed after source mutation: %s", completed.Result)
		}

		persisted, err := repository.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		assertJobEqual(t, persisted, want)
		if persisted.Lease != nil {
			t.Errorf("persisted lease = %#v, want nil", persisted.Lease)
		}
	})

	t.Run("complete missing job", func(t *testing.T) {
		truncateJobs(t)
		_, err := repository.CompleteExecution(ctx, job.JobID("missing"), job.WorkerID("worker-1"), job.LeaseToken("token-1"), json.RawMessage(`null`), completeNow)
		if !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("CompleteExecution() error = %v, want ErrJobNotFound", err)
		}
	})

	t.Run("rejected completions roll back", func(t *testing.T) {
		tests := []struct {
			name     string
			job      job.Job
			workerID job.WorkerID
			token    job.LeaseToken
			result   json.RawMessage
			now      time.Time
		}{
			{name: "invalid source state", job: leasableJob("complete-leased", job.StateLeased, 0, 3, startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{}`), now: completeNow},
			{name: "wrong worker", job: runningLeasedJob("complete-wrong-worker", startLeaseExpiresAt), workerID: job.WorkerID("worker-2"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{}`), now: completeNow},
			{name: "wrong token", job: runningLeasedJob("complete-wrong-token", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-2"), result: json.RawMessage(`{}`), now: completeNow},
			{name: "expired lease", job: runningLeasedJob("complete-expired", completeNow.Add(-time.Microsecond)), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{}`), now: completeNow},
			{name: "exact expiry", job: runningLeasedJob("complete-at-expiry", completeNow), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{}`), now: completeNow},
			{name: "invalid result", job: runningLeasedJob("complete-invalid-result", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{"broken":}`), now: completeNow},
			{name: "empty result", job: runningLeasedJob("complete-empty-result", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: nil, now: completeNow},
			{name: "zero completion time", job: runningLeasedJob("complete-zero-time", startLeaseExpiresAt), workerID: job.WorkerID("worker-1"), token: job.LeaseToken("token-1"), result: json.RawMessage(`{}`)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				truncateJobs(t)
				if err := repository.Create(ctx, tt.job); err != nil {
					t.Fatalf("Create() error = %v", err)
				}

				if _, err := repository.CompleteExecution(ctx, tt.job.ID, tt.workerID, tt.token, tt.result, tt.now); err == nil {
					t.Fatal("CompleteExecution() error = nil, want rejection")
				}
				persisted, err := repository.GetByID(ctx, tt.job.ID)
				if err != nil {
					t.Fatalf("GetByID() error = %v", err)
				}
				assertJobEqual(t, persisted, normalizedJob(tt.job))
			})
		}
	})

	t.Run("stale worker cannot complete after ownership changes", func(t *testing.T) {
		truncateJobs(t)
		running := runningLeasedJob("complete-new-owner", startLeaseExpiresAt)
		running.Lease.WorkerID = job.WorkerID("worker-2")
		running.Lease.Token = job.LeaseToken("token-2")
		if err := repository.Create(ctx, running); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		if _, err := repository.CompleteExecution(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), json.RawMessage(`{"owner":1}`), completeNow); err == nil {
			t.Fatal("stale CompleteExecution() error = nil, want rejection")
		}
		completed, err := repository.CompleteExecution(ctx, running.ID, job.WorkerID("worker-2"), job.LeaseToken("token-2"), json.RawMessage(`{"owner":2}`), completeNow)
		if err != nil {
			t.Fatalf("current owner CompleteExecution() error = %v", err)
		}
		if !jsonEqual(completed.Result, json.RawMessage(`{"owner":2}`)) {
			t.Errorf("completed result = %s, want current owner's result", completed.Result)
		}
	})

	t.Run("concurrent completions accept one terminal result", func(t *testing.T) {
		truncateJobs(t)
		running := runningLeasedJob("complete-contended", startLeaseExpiresAt)
		if err := repository.Create(ctx, running); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		secondConn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect second completer: %v", err)
		}
		defer func() {
			if err := secondConn.Close(ctx); err != nil {
				t.Errorf("close second completer connection: %v", err)
			}
		}()
		if _, err := secondConn.Exec(ctx, "SET search_path TO "+qualifiedSchema); err != nil {
			t.Fatalf("set second completer search path: %v", err)
		}
		secondRepository := NewJobRepository(secondConn)

		type completionResult struct {
			job job.Job
			err error
		}
		start := make(chan struct{})
		results := make(chan completionResult, 2)
		complete := func(repository *JobRepository, result json.RawMessage) {
			<-start
			completed, err := repository.CompleteExecution(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), result, completeNow)
			results <- completionResult{job: completed, err: err}
		}
		go complete(repository, json.RawMessage(`{"winner":1}`))
		go complete(secondRepository, json.RawMessage(`{"winner":2}`))
		close(start)

		var successes, rejections int
		var accepted json.RawMessage
		for range 2 {
			result := <-results
			if result.err == nil {
				successes++
				accepted = append(json.RawMessage(nil), result.job.Result...)
			} else {
				rejections++
			}
		}
		if successes != 1 || rejections != 1 {
			t.Errorf("concurrent outcomes = %d successes, %d rejections; want 1 and 1", successes, rejections)
		}

		persisted, err := repository.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if persisted.State != job.StateSucceeded || !jsonEqual(persisted.Result, accepted) {
			t.Errorf("persisted state/result = %q/%s, want succeeded accepted result %s", persisted.State, persisted.Result, accepted)
		}
		if persisted.Lease != nil {
			t.Errorf("persisted lease = %#v, want nil", persisted.Lease)
		}

		if _, err := repository.CompleteExecution(ctx, running.ID, job.WorkerID("worker-1"), job.LeaseToken("token-1"), json.RawMessage(`{"overwrite":true}`), completeNow.Add(time.Second)); err == nil {
			t.Fatal("duplicate CompleteExecution() error = nil, want rejection")
		}
		afterDuplicate, err := repository.GetByID(ctx, running.ID)
		if err != nil {
			t.Fatalf("GetByID() after duplicate error = %v", err)
		}
		if !jsonEqual(afterDuplicate.Result, accepted) {
			t.Errorf("duplicate completion overwrote result: got %s, want %s", afterDuplicate.Result, accepted)
		}
	})
}

func integrationJob(id string, createdAt time.Time) job.Job {
	return job.Job{
		ID: job.JobID(id), TaskType: job.TaskType("email"), Payload: json.RawMessage(`{"to":"user@example.test"}`),
		State: job.StateQueued, MaxAttempts: 3, CreatedAt: createdAt, AvailableAt: createdAt,
	}
}

func normalizedJob(j job.Job) job.Job {
	j.CreatedAt = j.CreatedAt.UTC()
	j.AvailableAt = j.AvailableAt.UTC()
	if j.Lease != nil {
		lease := *j.Lease
		lease.ExpiresAt = lease.ExpiresAt.UTC()
		j.Lease = &lease
	}
	j.StartedAt = utcPointer(j.StartedAt)
	j.CompletedAt = utcPointer(j.CompletedAt)
	j.FailedAt = utcPointer(j.FailedAt)
	return j
}

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func assertJobEqual(t *testing.T, got, want job.Job) {
	t.Helper()
	gotPayload, wantPayload := got.Payload, want.Payload
	gotResult, wantResult := got.Result, want.Result
	got.Payload, want.Payload = nil, nil
	got.Result, want.Result = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetByID() = %#v, want %#v", got, want)
	}
	if !jsonEqual(gotPayload, wantPayload) {
		t.Errorf("GetByID() payload = %s, want JSON equivalent to %s", gotPayload, wantPayload)
	}
	if !jsonEqual(gotResult, wantResult) {
		t.Errorf("GetByID() result = %s, want JSON equivalent to %s", gotResult, wantResult)
	}
}

func jsonEqual(left, right json.RawMessage) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func upMigrationPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "migrations", "000001_create_jobs.up.sql")
}

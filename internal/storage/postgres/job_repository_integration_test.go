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

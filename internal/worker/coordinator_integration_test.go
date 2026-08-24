package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/storage/postgres"
	"github.com/xtwo56/mercury/internal/task"
	"github.com/xtwo56/mercury/internal/worker"
)

func TestCoordinatorPostgreSQLIntegration(t *testing.T) {
	databaseURL := os.Getenv("MERCURY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set MERCURY_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	schema := "mercury_worker_" + time.Now().UTC().Format("20060102_150405_000000000")
	qualified := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+qualified); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(ctx, "DROP SCHEMA "+qualified+" CASCADE") })
	if _, err := conn.Exec(ctx, "SET search_path TO "+qualified); err != nil {
		t.Fatal(err)
	}
	for _, path := range integrationMigrationPaths(t) {
		migration, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	}

	repository := postgres.NewJobRepository(conn)
	now := time.Now().UTC()
	submitted, err := job.New(job.JobID("worker-sleep"), task.SleepTaskType, json.RawMessage(`{"duration_ms":250}`), 1, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(ctx, submitted); err != nil {
		t.Fatal(err)
	}
	observed := &observedRepository{JobRepository: repository, completed: make(chan struct{}, 1), renewed: make(chan struct{}, 16)}
	handlers := task.NewHandlerRegistry()
	sleep, _ := task.NewSleepHandler(task.NewSystemTimerFactory())
	if err := handlers.Register(task.SleepTaskType, sleep); err != nil {
		t.Fatal(err)
	}
	handlers.Seal()
	coordinator, err := worker.NewCoordinator(observed, handlers, worker.NewSystemClock(), worker.RandomTokenGenerator{}, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Config{
		WorkerID: job.WorkerID("integration-worker"), Concurrency: 1, PollInterval: time.Second,
		LeaseDuration: 90 * time.Millisecond, RetryDelay: time.Minute, HeartbeatInterval: 20 * time.Millisecond,
	}, func(err error) bool { return errors.Is(err, postgres.ErrNoJobAvailable) }, func(err error) bool {
		return errors.Is(err, postgres.ErrJobNotFound) || errors.Is(err, job.ErrJobNotRunning) || errors.Is(err, job.ErrLeaseMissing) || errors.Is(err, job.ErrLeaseWorkerMismatch) || errors.Is(err, job.ErrLeaseTokenMismatch) || errors.Is(err, job.ErrLeaseExpired)
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(runCtx) }()
	for range 2 {
		select {
		case <-observed.renewed:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for lease renewal")
		}
	}
	// Wait beyond the original lease duration while heartbeats continue, then
	// prove recovery cannot reclaim the actively owned execution.
	time.Sleep(70 * time.Millisecond)
	recoveryNow := time.Now().UTC()
	recovered, err := repository.RecoverExpiredLeases(ctx, recoveryNow, recoveryNow.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Fatalf("recovered actively heartbeating jobs = %v", recovered)
	}
	select {
	case <-observed.completed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for completion")
	}
	cancel()
	<-done
	persisted, err := repository.GetByID(ctx, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != job.StateSucceeded || persisted.AttemptsStarted != 1 || persisted.StartedAt == nil || persisted.CompletedAt == nil || persisted.Lease != nil {
		t.Errorf("persisted lifecycle = %#v", persisted)
	}
	if observed.renewalCount.Load() < 2 {
		t.Errorf("renewal count = %d, want at least 2", observed.renewalCount.Load())
	}
	if string(persisted.Result) != `{"duration_ms":250}` {
		t.Errorf("result = %s", persisted.Result)
	}
}

type observedRepository struct {
	*postgres.JobRepository
	completed    chan struct{}
	renewed      chan struct{}
	renewalCount atomic.Int32
}

func (repository *observedRepository) RenewLease(ctx context.Context, id job.JobID, workerID job.WorkerID, token job.LeaseToken, now, expiresAt time.Time) (job.Job, error) {
	renewed, err := repository.JobRepository.RenewLease(ctx, id, workerID, token, now, expiresAt)
	if err == nil {
		repository.renewalCount.Add(1)
		repository.renewed <- struct{}{}
	}
	return renewed, err
}

func (repository *observedRepository) CompleteExecution(ctx context.Context, id job.JobID, workerID job.WorkerID, token job.LeaseToken, result json.RawMessage, now time.Time) (job.Job, error) {
	completed, err := repository.JobRepository.CompleteExecution(ctx, id, workerID, token, result, now)
	if err == nil {
		repository.completed <- struct{}{}
	}
	return completed, err
}

func integrationMigrationPaths(t *testing.T) []string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve path")
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "..", "..", "..", "migrations", "*.up.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("find migrations: %v", err)
	}
	return paths
}

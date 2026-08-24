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
	submitted, err := job.New(job.JobID("worker-sleep"), task.SleepTaskType, json.RawMessage(`{"duration_ms":1}`), 1, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(ctx, submitted); err != nil {
		t.Fatal(err)
	}
	observed := &observedRepository{JobRepository: repository, completed: make(chan struct{}, 1)}
	handlers := task.NewHandlerRegistry()
	sleep, _ := task.NewSleepHandler(task.NewSystemTimerFactory())
	if err := handlers.Register(task.SleepTaskType, sleep); err != nil {
		t.Fatal(err)
	}
	handlers.Seal()
	coordinator, err := worker.NewCoordinator(observed, handlers, worker.NewSystemClock(), worker.RandomTokenGenerator{}, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Config{
		WorkerID: job.WorkerID("integration-worker"), Concurrency: 1, PollInterval: time.Second,
		LeaseDuration: time.Minute, RetryDelay: time.Minute,
	}, func(err error) bool { return errors.Is(err, postgres.ErrNoJobAvailable) })
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- coordinator.Run(runCtx) }()
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
	if string(persisted.Result) != `{"duration_ms":1}` {
		t.Errorf("result = %s", persisted.Result)
	}
}

type observedRepository struct {
	*postgres.JobRepository
	completed chan struct{}
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

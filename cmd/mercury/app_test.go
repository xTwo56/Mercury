package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xtwo56/mercury/internal/job"
)

func TestProductionDatabaseIntegration(t *testing.T) {
	databaseURL := os.Getenv("MERCURY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set MERCURY_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	db, err := productionDependencies().openDatabase(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("openDatabase() error = %v", err)
	}
	t.Cleanup(db.Close)
	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
}

func TestRunApplicationCancellationAndCleanShutdown(t *testing.T) {
	db := &fakeDatabase{}
	runner := recoveryRunnerFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	dependencies := fakeDependencies(db, runner)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := runApplication(ctx, validConfig(), discardLogger(), dependencies); err != nil {
		t.Fatalf("runApplication() error = %v", err)
	}
	if !db.closed {
		t.Error("runApplication() did not close database pool")
	}
}

func TestRunApplicationShutdownLogsRoleWithoutCancellationError(t *testing.T) {
	var output strings.Builder
	logger := slog.New(slog.NewTextHandler(&output, nil))
	db := &fakeDatabase{}
	started := make(chan struct{}, 1)
	runner := recoveryRunnerFunc(func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	configuration := validConfig()
	configuration.Role = roleWorker
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runApplication(ctx, configuration, logger, fakeDependencies(db, runner)) }()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runApplication() error = %v", err)
	}
	if !strings.Contains(output.String(), "role=worker") {
		t.Errorf("startup log did not include selected role: %s", output.String())
	}
	if strings.Contains(output.String(), "level=ERROR") || strings.Contains(output.String(), "context canceled") {
		t.Errorf("graceful shutdown produced misleading error log: %s", output.String())
	}
}

func TestRunApplicationStartupFailures(t *testing.T) {
	openFailure := errors.New("open failed with postgres://user:secret@database/db")
	pingFailure := errors.New("ping failed")
	buildFailure := errors.New("build failed")
	tests := []struct {
		name         string
		dependencies applicationDependencies
		wantCause    error
	}{
		{
			name: "open database",
			dependencies: applicationDependencies{
				openDatabase: func(context.Context, string) (database, error) { return nil, openFailure },
			},
			wantCause: openFailure,
		},
		{
			name: "ping database",
			dependencies: func() applicationDependencies {
				db := &fakeDatabase{pingError: pingFailure}
				dependencies := fakeDependencies(db, recoveryRunnerFunc(func(context.Context) error { return nil }))
				return dependencies
			}(),
			wantCause: pingFailure,
		},
		{
			name: "build scheduler",
			dependencies: func() applicationDependencies {
				db := &fakeDatabase{}
				return applicationDependencies{
					openDatabase: func(context.Context, string) (database, error) { return db, nil },
					buildAPI: func(database, config, *slog.Logger) (serviceRunner, error) {
						return nil, buildFailure
					},
				}
			}(),
			wantCause: buildFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runApplication(context.Background(), validConfig(), discardLogger(), tt.dependencies)
			if !errors.Is(err, tt.wantCause) {
				t.Fatalf("runApplication() error = %v, want cause %v", err, tt.wantCause)
			}
			if strings.Contains(err.Error(), "postgres://") || strings.Contains(err.Error(), "secret") {
				t.Errorf("runApplication() error exposed database URL: %v", err)
			}
		})
	}
}

func TestRunApplicationClosesPoolOnStartupAndRuntimeFailures(t *testing.T) {
	tests := []struct {
		name   string
		db     *fakeDatabase
		runner recoveryRunner
	}{
		{name: "ping failure", db: &fakeDatabase{pingError: errors.New("ping failed")}, runner: recoveryRunnerFunc(func(context.Context) error { return nil })},
		{name: "runtime failure", db: &fakeDatabase{}, runner: recoveryRunnerFunc(func(context.Context) error { return errors.New("scheduler failed") })},
		{name: "unexpected stop", db: &fakeDatabase{}, runner: recoveryRunnerFunc(func(context.Context) error { return nil })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runApplication(context.Background(), validConfig(), discardLogger(), fakeDependencies(tt.db, tt.runner)); err == nil {
				t.Fatal("runApplication() error = nil, want failure")
			}
			if !tt.db.closed {
				t.Error("runApplication() did not close database pool")
			}
		})
	}
}

func TestRunApplicationValidatesBeforeOpeningDatabase(t *testing.T) {
	opened := false
	dependencies := applicationDependencies{openDatabase: func(context.Context, string) (database, error) {
		opened = true
		return &fakeDatabase{}, nil
	}}
	configuration := validConfig()
	configuration.RecoveryBatchSize = 0
	if err := runApplication(context.Background(), configuration, discardLogger(), dependencies); err == nil {
		t.Fatal("runApplication() error = nil, want configuration error")
	}
	if opened {
		t.Error("runApplication() opened database before validating configuration")
	}
}

func TestRunApplicationRoleComponentMatrix(t *testing.T) {
	tests := []struct {
		role                   runtimeRole
		api, scheduler, worker int32
	}{
		{role: roleAPI, api: 1},
		{role: roleScheduler, scheduler: 1},
		{role: roleWorker, worker: 1},
		{role: roleAll, api: 1, scheduler: 1, worker: 1},
	}
	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			db := &fakeDatabase{}
			var apiBuilt, schedulerBuilt, workerBuilt atomic.Int32
			started := make(chan string, 3)
			factory := func(name string, count *atomic.Int32) func(database, config, *slog.Logger) (serviceRunner, error) {
				return func(database, config, *slog.Logger) (serviceRunner, error) {
					count.Add(1)
					return recoveryRunnerFunc(func(ctx context.Context) error {
						started <- name
						<-ctx.Done()
						return ctx.Err()
					}), nil
				}
			}
			dependencies := applicationDependencies{
				openDatabase:   func(context.Context, string) (database, error) { return db, nil },
				buildAPI:       factory("api", &apiBuilt),
				buildScheduler: factory("scheduler", &schedulerBuilt),
				buildWorker:    factory("worker", &workerBuilt),
			}
			configuration := validConfig()
			configuration.Role = tt.role
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runApplication(ctx, configuration, discardLogger(), dependencies) }()
			wantStarted := int(tt.api + tt.scheduler + tt.worker)
			for range wantStarted {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for role components")
				}
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("runApplication() error = %v", err)
			}
			if apiBuilt.Load() != tt.api || schedulerBuilt.Load() != tt.scheduler || workerBuilt.Load() != tt.worker {
				t.Errorf("constructed api/scheduler/worker = %d/%d/%d, want %d/%d/%d", apiBuilt.Load(), schedulerBuilt.Load(), workerBuilt.Load(), tt.api, tt.scheduler, tt.worker)
			}
			if !db.closed {
				t.Error("database was not closed")
			}
		})
	}
}

func TestRunApplicationFailureCancelsSiblingsBeforeClosingDatabase(t *testing.T) {
	failure := errors.New("API failed")
	started := make(chan struct{}, 3)
	releaseFailure := make(chan struct{})
	var stopped atomic.Int32
	db := &fakeDatabase{closeCheck: func() {
		if stopped.Load() != 2 {
			t.Errorf("database closed before siblings stopped: %d", stopped.Load())
		}
	}}
	dependencies := applicationDependencies{
		openDatabase: func(context.Context, string) (database, error) { return db, nil },
		buildAPI: func(database, config, *slog.Logger) (serviceRunner, error) {
			return recoveryRunnerFunc(func(context.Context) error { started <- struct{}{}; <-releaseFailure; return failure }), nil
		},
		buildScheduler: func(database, config, *slog.Logger) (serviceRunner, error) {
			return recoveryRunnerFunc(func(ctx context.Context) error { started <- struct{}{}; <-ctx.Done(); stopped.Add(1); return ctx.Err() }), nil
		},
		buildWorker: func(database, config, *slog.Logger) (serviceRunner, error) {
			return recoveryRunnerFunc(func(ctx context.Context) error { started <- struct{}{}; <-ctx.Done(); stopped.Add(1); return ctx.Err() }), nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- runApplication(context.Background(), validConfig(), discardLogger(), dependencies) }()
	for range 3 {
		<-started
	}
	close(releaseFailure)
	err := <-done
	if !errors.Is(err, failure) {
		t.Fatalf("runApplication() error = %v, want %v", err, failure)
	}
	if !db.closed {
		t.Error("database was not closed")
	}
}

func TestRunApplicationPartialConstructionFailureStartsNothing(t *testing.T) {
	failure := errors.New("scheduler construction failed")
	db := &fakeDatabase{}
	var apiBuilt, apiStarted, workerBuilt atomic.Int32
	dependencies := applicationDependencies{
		openDatabase: func(context.Context, string) (database, error) { return db, nil },
		buildAPI: func(database, config, *slog.Logger) (serviceRunner, error) {
			apiBuilt.Add(1)
			return recoveryRunnerFunc(func(context.Context) error { apiStarted.Add(1); return nil }), nil
		},
		buildScheduler: func(database, config, *slog.Logger) (serviceRunner, error) { return nil, failure },
		buildWorker:    func(database, config, *slog.Logger) (serviceRunner, error) { workerBuilt.Add(1); return nil, nil },
	}
	err := runApplication(context.Background(), validConfig(), discardLogger(), dependencies)
	if !errors.Is(err, failure) {
		t.Fatalf("runApplication() error = %v, want %v", err, failure)
	}
	if apiBuilt.Load() != 1 || apiStarted.Load() != 0 || workerBuilt.Load() != 0 || !db.closed {
		t.Errorf("partial startup built/started/worker/closed = %d/%d/%d/%v", apiBuilt.Load(), apiStarted.Load(), workerBuilt.Load(), db.closed)
	}
}

func validConfig() config {
	return config{
		Role:                    roleAll,
		DatabaseURL:             "postgres://user:secret@database/mercury",
		RecoveryInterval:        defaultRecoveryInterval,
		RecoveryRetryDelay:      defaultRecoveryRetryDelay,
		RecoveryBatchSize:       defaultRecoveryBatchSize,
		HTTPListenAddress:       defaultHTTPListenAddress,
		HTTPReadTimeout:         defaultHTTPReadTimeout,
		HTTPReadHeaderTimeout:   defaultHTTPHeaderTimeout,
		HTTPWriteTimeout:        defaultHTTPWriteTimeout,
		HTTPIdleTimeout:         defaultHTTPIdleTimeout,
		HTTPShutdownTimeout:     defaultHTTPShutdownTimeout,
		WorkerID:                job.WorkerID("worker-test"),
		WorkerConcurrency:       defaultWorkerConcurrency,
		WorkerPollInterval:      defaultWorkerPollInterval,
		WorkerLeaseDuration:     defaultWorkerLeaseDuration,
		WorkerRetryDelay:        defaultWorkerRetryDelay,
		WorkerHeartbeatInterval: defaultWorkerLeaseDuration / 3,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fakeDependencies(db *fakeDatabase, runner recoveryRunner) applicationDependencies {
	return applicationDependencies{
		openDatabase:   func(context.Context, string) (database, error) { return db, nil },
		buildAPI:       func(database, config, *slog.Logger) (serviceRunner, error) { return runner, nil },
		buildScheduler: func(database, config, *slog.Logger) (serviceRunner, error) { return runner, nil },
		buildWorker:    func(database, config, *slog.Logger) (serviceRunner, error) { return runner, nil },
	}
}

type recoveryRunnerFunc func(context.Context) error

func (run recoveryRunnerFunc) Run(ctx context.Context) error { return run(ctx) }

type fakeDatabase struct {
	pingError  error
	closed     bool
	closeCheck func()
}

func (db *fakeDatabase) Ping(context.Context) error { return db.pingError }
func (db *fakeDatabase) Close() {
	if db.closeCheck != nil {
		db.closeCheck()
	}
	db.closed = true
}
func (db *fakeDatabase) Begin(context.Context) (pgx.Tx, error) {
	panic("unexpected Begin call")
}
func (db *fakeDatabase) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}
func (db *fakeDatabase) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}
func (db *fakeDatabase) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	panic("unexpected QueryRow call")
}

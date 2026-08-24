package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

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
					buildServices: func(database, config, *slog.Logger) (applicationServices, error) {
						return applicationServices{}, buildFailure
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

func validConfig() config {
	return config{
		DatabaseURL:           "postgres://user:secret@database/mercury",
		RecoveryInterval:      defaultRecoveryInterval,
		RecoveryRetryDelay:    defaultRecoveryRetryDelay,
		RecoveryBatchSize:     defaultRecoveryBatchSize,
		HTTPListenAddress:     defaultHTTPListenAddress,
		HTTPReadTimeout:       defaultHTTPReadTimeout,
		HTTPReadHeaderTimeout: defaultHTTPHeaderTimeout,
		HTTPWriteTimeout:      defaultHTTPWriteTimeout,
		HTTPIdleTimeout:       defaultHTTPIdleTimeout,
		HTTPShutdownTimeout:   defaultHTTPShutdownTimeout,
		WorkerID:              job.WorkerID("worker-test"), WorkerConcurrency: defaultWorkerConcurrency,
		WorkerPollInterval: defaultWorkerPollInterval, WorkerLeaseDuration: defaultWorkerLeaseDuration,
		WorkerRetryDelay: defaultWorkerRetryDelay,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fakeDependencies(db *fakeDatabase, runner recoveryRunner) applicationDependencies {
	return applicationDependencies{
		openDatabase: func(context.Context, string) (database, error) { return db, nil },
		buildServices: func(database, config, *slog.Logger) (applicationServices, error) {
			return applicationServices{recovery: runner, http: runner, worker: runner}, nil
		},
	}
}

type recoveryRunnerFunc func(context.Context) error

func (run recoveryRunnerFunc) Run(ctx context.Context) error { return run(ctx) }

type fakeDatabase struct {
	pingError error
	closed    bool
}

func (db *fakeDatabase) Ping(context.Context) error { return db.pingError }
func (db *fakeDatabase) Close()                     { db.closed = true }
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

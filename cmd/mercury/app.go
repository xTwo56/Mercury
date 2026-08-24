package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	jobapp "github.com/xtwo56/mercury/internal/app"
	"github.com/xtwo56/mercury/internal/httpapi"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/scheduler"
	"github.com/xtwo56/mercury/internal/storage/postgres"
	"github.com/xtwo56/mercury/internal/storage/postgres/generated"
	"github.com/xtwo56/mercury/internal/task"
	"github.com/xtwo56/mercury/internal/worker"
)

type database interface {
	generated.DBTX
	Begin(context.Context) (pgx.Tx, error)
	Ping(context.Context) error
	Close()
}

type recoveryRunner interface {
	Run(context.Context) error
}

type applicationDependencies struct {
	openDatabase  func(context.Context, string) (database, error)
	buildServices func(database, config, *slog.Logger) (applicationServices, error)
}

type applicationServices struct {
	recovery recoveryRunner
	http     recoveryRunner
	handlers *task.HandlerRegistry
	worker   recoveryRunner
}

func productionDependencies() applicationDependencies {
	return applicationDependencies{
		openDatabase: func(ctx context.Context, databaseURL string) (database, error) {
			return pgxpool.New(ctx, databaseURL)
		},
		buildServices: func(db database, configuration config, logger *slog.Logger) (applicationServices, error) {
			repository := postgres.NewJobRepository(db)
			recovery, err := scheduler.NewRecoveryService(repository, scheduler.NewSystemClock(), logger, scheduler.RecoveryConfig{
				SweepInterval: configuration.RecoveryInterval,
				RetryDelay:    configuration.RecoveryRetryDelay,
				BatchSize:     configuration.RecoveryBatchSize,
			})
			if err != nil {
				return applicationServices{}, err
			}
			registry := task.NewRegistry(map[job.TaskType]task.Validator{
				task.SleepTaskType: task.SleepValidator{},
			})
			handlers := task.NewHandlerRegistry()
			sleepHandler, err := task.NewSleepHandler(task.NewSystemTimerFactory())
			if err != nil {
				return applicationServices{}, err
			}
			if err := handlers.Register(task.SleepTaskType, sleepHandler); err != nil {
				return applicationServices{}, err
			}
			handlers.Seal()
			workerCoordinator, err := worker.NewCoordinator(repository, handlers, worker.NewSystemClock(), worker.RandomTokenGenerator{}, logger, worker.Config{
				WorkerID: configuration.WorkerID, Concurrency: configuration.WorkerConcurrency,
				PollInterval: configuration.WorkerPollInterval, LeaseDuration: configuration.WorkerLeaseDuration,
				RetryDelay: configuration.WorkerRetryDelay, HeartbeatInterval: configuration.WorkerHeartbeatInterval,
			}, func(err error) bool { return errors.Is(err, postgres.ErrNoJobAvailable) }, func(err error) bool {
				return errors.Is(err, postgres.ErrJobNotFound) || errors.Is(err, job.ErrJobNotRunning) ||
					errors.Is(err, job.ErrLeaseMissing) || errors.Is(err, job.ErrLeaseWorkerMismatch) ||
					errors.Is(err, job.ErrLeaseTokenMismatch) || errors.Is(err, job.ErrLeaseExpired)
			})
			if err != nil {
				return applicationServices{}, err
			}
			jobs, err := jobapp.NewJobService(repository, registry, jobapp.SystemClock{}, jobapp.RandomIDGenerator{}, func(err error) bool {
				return errors.Is(err, postgres.ErrJobNotFound)
			})
			if err != nil {
				return applicationServices{}, err
			}
			httpServer, err := httpapi.NewServer(httpapi.ServerConfig{
				ListenAddress:     configuration.HTTPListenAddress,
				ReadTimeout:       configuration.HTTPReadTimeout,
				ReadHeaderTimeout: configuration.HTTPReadHeaderTimeout,
				WriteTimeout:      configuration.HTTPWriteTimeout,
				IdleTimeout:       configuration.HTTPIdleTimeout,
				ShutdownTimeout:   configuration.HTTPShutdownTimeout,
			}, httpapi.NewHandler(jobs), logger)
			if err != nil {
				return applicationServices{}, err
			}
			return applicationServices{recovery: recovery, http: httpServer, handlers: handlers, worker: workerCoordinator}, nil
		},
	}
}

func runApplication(ctx context.Context, configuration config, logger *slog.Logger, dependencies applicationDependencies) error {
	if err := configuration.validate(); err != nil {
		return fmt.Errorf("validate configuration: %w", err)
	}

	db, err := dependencies.openDatabase(ctx, configuration.DatabaseURL)
	if err != nil {
		return privateError("create PostgreSQL pool", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return privateError("verify PostgreSQL connection", err)
	}

	services, err := dependencies.buildServices(db, configuration, logger)
	if err != nil {
		return fmt.Errorf("construct application services: %w", err)
	}

	logger.InfoContext(ctx, "Mercury scheduler started",
		"recovery_interval", configuration.RecoveryInterval,
		"recovery_batch_size", configuration.RecoveryBatchSize,
	)
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type serviceResult struct {
		name string
		err  error
	}
	results := make(chan serviceResult, 3)
	go func() { results <- serviceResult{name: "recovery service", err: services.recovery.Run(serviceCtx)} }()
	go func() { results <- serviceResult{name: "HTTP service", err: services.http.Run(serviceCtx)} }()
	go func() { results <- serviceResult{name: "worker service", err: services.worker.Run(serviceCtx)} }()

	var failure error
	for range 3 {
		result := <-results
		if ctx.Err() == nil && failure == nil {
			if result.err != nil {
				failure = fmt.Errorf("run %s: %w", result.name, result.err)
			} else {
				failure = fmt.Errorf("%s stopped unexpectedly", result.name)
			}
			cancel()
		}
	}
	if failure != nil {
		return failure
	}
	logger.InfoContext(context.Background(), "Mercury services stopped")
	return nil
}

type privateApplicationError struct {
	message string
	cause   error
}

func privateError(message string, cause error) error {
	return privateApplicationError{message: message, cause: cause}
}

func (err privateApplicationError) Error() string { return err.message }
func (err privateApplicationError) Unwrap() error { return err.cause }

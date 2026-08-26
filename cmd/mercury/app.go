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
	openDatabase   func(context.Context, string) (database, error)
	buildAPI       func(database, config, *slog.Logger) (serviceRunner, error)
	buildScheduler func(database, config, *slog.Logger) (serviceRunner, error)
	buildWorker    func(database, config, *slog.Logger) (serviceRunner, error)
}

type serviceRunner interface{ Run(context.Context) error }

type namedService struct {
	name   string
	runner serviceRunner
}

func productionDependencies() applicationDependencies {
	return applicationDependencies{
		openDatabase: func(ctx context.Context, databaseURL string) (database, error) {
			return pgxpool.New(ctx, databaseURL)
		},
		buildScheduler: func(db database, configuration config, logger *slog.Logger) (serviceRunner, error) {
			repository := postgres.NewJobRepository(db)
			return scheduler.NewRecoveryService(repository, scheduler.NewSystemClock(), logger, scheduler.RecoveryConfig{
				SweepInterval: configuration.RecoveryInterval,
				RetryDelay:    configuration.RecoveryRetryDelay,
				BatchSize:     configuration.RecoveryBatchSize,
			})
		},
		buildWorker: func(db database, configuration config, logger *slog.Logger) (serviceRunner, error) {
			repository := postgres.NewJobRepository(db)
			handlers := task.NewHandlerRegistry()
			sleepHandler, err := task.NewSleepHandler(task.NewSystemTimerFactory())
			if err != nil {
				return nil, err
			}
			if err := handlers.Register(task.SleepTaskType, sleepHandler); err != nil {
				return nil, err
			}
			handlers.Seal()
			return worker.NewCoordinator(repository, handlers, worker.NewSystemClock(), worker.RandomTokenGenerator{}, logger, worker.Config{
				WorkerID: configuration.WorkerID, Concurrency: configuration.WorkerConcurrency,
				PollInterval: configuration.WorkerPollInterval, LeaseDuration: configuration.WorkerLeaseDuration,
				RetryDelay: configuration.WorkerRetryDelay, HeartbeatInterval: configuration.WorkerHeartbeatInterval,
			}, func(err error) bool { return errors.Is(err, postgres.ErrNoJobAvailable) }, func(err error) bool {
				return errors.Is(err, postgres.ErrJobNotFound) || errors.Is(err, job.ErrJobNotRunning) ||
					errors.Is(err, job.ErrLeaseMissing) || errors.Is(err, job.ErrLeaseWorkerMismatch) ||
					errors.Is(err, job.ErrLeaseTokenMismatch) || errors.Is(err, job.ErrLeaseExpired)
			})
		},
		buildAPI: func(db database, configuration config, logger *slog.Logger) (serviceRunner, error) {
			repository := postgres.NewJobRepository(db)
			registry := task.NewRegistry(map[job.TaskType]task.Validator{
				task.SleepTaskType: task.SleepValidator{},
			})
			jobs, err := jobapp.NewJobService(repository, registry, jobapp.SystemClock{}, jobapp.RandomIDGenerator{},
				func(err error) bool { return errors.Is(err, postgres.ErrJobNotFound) },
				func(err error) bool { return errors.Is(err, postgres.ErrIdempotencyConflict) },
			)
			if err != nil {
				return nil, err
			}
			return httpapi.NewServer(httpapi.ServerConfig{
				ListenAddress:     configuration.HTTPListenAddress,
				ReadTimeout:       configuration.HTTPReadTimeout,
				ReadHeaderTimeout: configuration.HTTPReadHeaderTimeout,
				WriteTimeout:      configuration.HTTPWriteTimeout,
				IdleTimeout:       configuration.HTTPIdleTimeout,
				ShutdownTimeout:   configuration.HTTPShutdownTimeout,
			}, httpapi.NewHandler(jobs), logger)
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

	services, err := buildRoleServices(db, configuration, logger, dependencies)
	if err != nil {
		return fmt.Errorf("construct application services: %w", err)
	}

	logger.InfoContext(ctx, "Mercury started", "role", configuration.Role)
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type serviceResult struct {
		name string
		err  error
	}
	results := make(chan serviceResult, len(services))
	for _, service := range services {
		service := service
		go func() { results <- serviceResult{name: service.name, err: service.runner.Run(serviceCtx)} }()
	}

	var failure error
	for range services {
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

func buildRoleServices(db database, configuration config, logger *slog.Logger, dependencies applicationDependencies) ([]namedService, error) {
	services := make([]namedService, 0, 3)
	build := func(enabled bool, name string, factory func(database, config, *slog.Logger) (serviceRunner, error)) error {
		if !enabled {
			return nil
		}
		if factory == nil {
			return fmt.Errorf("%s factory is missing", name)
		}
		runner, err := factory(db, configuration, logger)
		if err != nil {
			return fmt.Errorf("build %s: %w", name, err)
		}
		if runner == nil {
			return fmt.Errorf("build %s: service is nil", name)
		}
		services = append(services, namedService{name: name, runner: runner})
		return nil
	}
	if err := build(configuration.Role.includesAPI(), "HTTP service", dependencies.buildAPI); err != nil {
		return nil, err
	}
	if err := build(configuration.Role.includesScheduler(), "recovery service", dependencies.buildScheduler); err != nil {
		return nil, err
	}
	if err := build(configuration.Role.includesWorker(), "worker service", dependencies.buildWorker); err != nil {
		return nil, err
	}
	return services, nil
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

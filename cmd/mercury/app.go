package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtwo56/mercury/internal/scheduler"
	"github.com/xtwo56/mercury/internal/storage/postgres"
	"github.com/xtwo56/mercury/internal/storage/postgres/generated"
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
	buildRecovery func(database, config, *slog.Logger) (recoveryRunner, error)
}

func productionDependencies() applicationDependencies {
	return applicationDependencies{
		openDatabase: func(ctx context.Context, databaseURL string) (database, error) {
			return pgxpool.New(ctx, databaseURL)
		},
		buildRecovery: func(db database, configuration config, logger *slog.Logger) (recoveryRunner, error) {
			repository := postgres.NewJobRepository(db)
			return scheduler.NewRecoveryService(repository, scheduler.NewSystemClock(), logger, scheduler.RecoveryConfig{
				SweepInterval: configuration.RecoveryInterval,
				RetryDelay:    configuration.RecoveryRetryDelay,
				BatchSize:     configuration.RecoveryBatchSize,
			})
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

	recovery, err := dependencies.buildRecovery(db, configuration, logger)
	if err != nil {
		return fmt.Errorf("construct recovery service: %w", err)
	}

	logger.InfoContext(ctx, "Mercury scheduler started",
		"recovery_interval", configuration.RecoveryInterval,
		"recovery_batch_size", configuration.RecoveryBatchSize,
	)
	err = recovery.Run(ctx)
	if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {
		logger.InfoContext(context.Background(), "Mercury scheduler stopped")
		return nil
	}
	if err != nil {
		return fmt.Errorf("run recovery service: %w", err)
	}
	return errors.New("recovery service stopped unexpectedly")
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

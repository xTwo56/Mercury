package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xtwo56/mercury/internal/storage/postgres"
)

const (
	databaseURLEnvironment      = "MERCURY_DATABASE_URL"
	recoveryIntervalEnvironment = "MERCURY_RECOVERY_INTERVAL"
	recoveryBatchEnvironment    = "MERCURY_RECOVERY_BATCH_SIZE"

	defaultRecoveryInterval   = time.Minute
	defaultRecoveryRetryDelay = time.Minute
	defaultRecoveryBatchSize  = 100
)

type config struct {
	DatabaseURL        string
	RecoveryInterval   time.Duration
	RecoveryRetryDelay time.Duration
	RecoveryBatchSize  int
}

func loadConfig(getenv func(string) string) (config, error) {
	databaseURL := strings.TrimSpace(getenv(databaseURLEnvironment))
	if databaseURL == "" {
		return config{}, fmt.Errorf("%s is required", databaseURLEnvironment)
	}

	interval, err := environmentDuration(getenv, recoveryIntervalEnvironment, defaultRecoveryInterval)
	if err != nil {
		return config{}, err
	}
	batchSize, err := environmentInteger(getenv, recoveryBatchEnvironment, defaultRecoveryBatchSize)
	if err != nil {
		return config{}, err
	}

	loaded := config{
		DatabaseURL:        databaseURL,
		RecoveryInterval:   interval,
		RecoveryRetryDelay: defaultRecoveryRetryDelay,
		RecoveryBatchSize:  batchSize,
	}
	if err := loaded.validate(); err != nil {
		return config{}, err
	}
	return loaded, nil
}

func (configuration config) validate() error {
	if strings.TrimSpace(configuration.DatabaseURL) == "" {
		return errors.New("PostgreSQL connection string is required")
	}
	if configuration.RecoveryInterval <= 0 {
		return errors.New("recovery interval must be positive")
	}
	if configuration.RecoveryRetryDelay <= 0 {
		return errors.New("recovery retry delay must be positive")
	}
	if configuration.RecoveryBatchSize <= 0 || configuration.RecoveryBatchSize > postgres.MaxRecoveryBatchSize {
		return fmt.Errorf("recovery batch size must be between 1 and %d", postgres.MaxRecoveryBatchSize)
	}
	return nil
}

func environmentDuration(getenv func(string) string, name string, defaultValue time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration", name)
	}
	return duration, nil
}

func environmentInteger(getenv func(string) string, name string, defaultValue int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return defaultValue, nil
	}
	integer, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid integer", name)
	}
	return integer, nil
}

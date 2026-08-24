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
	databaseURLEnvironment         = "MERCURY_DATABASE_URL"
	recoveryIntervalEnvironment    = "MERCURY_RECOVERY_INTERVAL"
	recoveryBatchEnvironment       = "MERCURY_RECOVERY_BATCH_SIZE"
	httpListenEnvironment          = "MERCURY_HTTP_LISTEN_ADDRESS"
	httpReadTimeoutEnvironment     = "MERCURY_HTTP_READ_TIMEOUT"
	httpHeaderTimeoutEnvironment   = "MERCURY_HTTP_READ_HEADER_TIMEOUT"
	httpWriteTimeoutEnvironment    = "MERCURY_HTTP_WRITE_TIMEOUT"
	httpIdleTimeoutEnvironment     = "MERCURY_HTTP_IDLE_TIMEOUT"
	httpShutdownTimeoutEnvironment = "MERCURY_HTTP_SHUTDOWN_TIMEOUT"

	defaultRecoveryInterval    = time.Minute
	defaultRecoveryRetryDelay  = time.Minute
	defaultRecoveryBatchSize   = 100
	defaultHTTPListenAddress   = ":8080"
	defaultHTTPReadTimeout     = 10 * time.Second
	defaultHTTPHeaderTimeout   = 5 * time.Second
	defaultHTTPWriteTimeout    = 30 * time.Second
	defaultHTTPIdleTimeout     = time.Minute
	defaultHTTPShutdownTimeout = 10 * time.Second
)

type config struct {
	DatabaseURL           string
	RecoveryInterval      time.Duration
	RecoveryRetryDelay    time.Duration
	RecoveryBatchSize     int
	HTTPListenAddress     string
	HTTPReadTimeout       time.Duration
	HTTPReadHeaderTimeout time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	HTTPShutdownTimeout   time.Duration
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
	httpReadTimeout, err := environmentDuration(getenv, httpReadTimeoutEnvironment, defaultHTTPReadTimeout)
	if err != nil {
		return config{}, err
	}
	httpHeaderTimeout, err := environmentDuration(getenv, httpHeaderTimeoutEnvironment, defaultHTTPHeaderTimeout)
	if err != nil {
		return config{}, err
	}
	httpWriteTimeout, err := environmentDuration(getenv, httpWriteTimeoutEnvironment, defaultHTTPWriteTimeout)
	if err != nil {
		return config{}, err
	}
	httpIdleTimeout, err := environmentDuration(getenv, httpIdleTimeoutEnvironment, defaultHTTPIdleTimeout)
	if err != nil {
		return config{}, err
	}
	httpShutdownTimeout, err := environmentDuration(getenv, httpShutdownTimeoutEnvironment, defaultHTTPShutdownTimeout)
	if err != nil {
		return config{}, err
	}
	httpListenAddress := strings.TrimSpace(getenv(httpListenEnvironment))
	if httpListenAddress == "" {
		httpListenAddress = defaultHTTPListenAddress
	}

	loaded := config{
		DatabaseURL:           databaseURL,
		RecoveryInterval:      interval,
		RecoveryRetryDelay:    defaultRecoveryRetryDelay,
		RecoveryBatchSize:     batchSize,
		HTTPListenAddress:     httpListenAddress,
		HTTPReadTimeout:       httpReadTimeout,
		HTTPReadHeaderTimeout: httpHeaderTimeout,
		HTTPWriteTimeout:      httpWriteTimeout,
		HTTPIdleTimeout:       httpIdleTimeout,
		HTTPShutdownTimeout:   httpShutdownTimeout,
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
	if strings.TrimSpace(configuration.HTTPListenAddress) == "" {
		return errors.New("HTTP listen address must not be empty")
	}
	if configuration.HTTPReadTimeout <= 0 || configuration.HTTPReadHeaderTimeout <= 0 || configuration.HTTPWriteTimeout <= 0 || configuration.HTTPIdleTimeout <= 0 || configuration.HTTPShutdownTimeout <= 0 {
		return errors.New("HTTP server timeouts must be positive")
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

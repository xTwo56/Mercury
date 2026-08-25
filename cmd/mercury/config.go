package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/storage/postgres"
)

const (
	roleEnvironment                = "MERCURY_ROLE"
	databaseURLEnvironment         = "MERCURY_DATABASE_URL"
	recoveryIntervalEnvironment    = "MERCURY_RECOVERY_INTERVAL"
	recoveryBatchEnvironment       = "MERCURY_RECOVERY_BATCH_SIZE"
	httpListenEnvironment          = "MERCURY_HTTP_LISTEN_ADDRESS"
	httpReadTimeoutEnvironment     = "MERCURY_HTTP_READ_TIMEOUT"
	httpHeaderTimeoutEnvironment   = "MERCURY_HTTP_READ_HEADER_TIMEOUT"
	httpWriteTimeoutEnvironment    = "MERCURY_HTTP_WRITE_TIMEOUT"
	httpIdleTimeoutEnvironment     = "MERCURY_HTTP_IDLE_TIMEOUT"
	httpShutdownTimeoutEnvironment = "MERCURY_HTTP_SHUTDOWN_TIMEOUT"
	workerIDEnvironment            = "MERCURY_WORKER_ID"
	workerConcurrencyEnvironment   = "MERCURY_WORKER_CONCURRENCY"
	workerPollIntervalEnvironment  = "MERCURY_WORKER_POLL_INTERVAL"
	workerLeaseDurationEnvironment = "MERCURY_WORKER_LEASE_DURATION"
	workerHeartbeatEnvironment     = "MERCURY_WORKER_HEARTBEAT_INTERVAL"

	defaultRecoveryInterval    = time.Minute
	defaultRecoveryRetryDelay  = time.Minute
	defaultRecoveryBatchSize   = 100
	defaultHTTPListenAddress   = ":8080"
	defaultHTTPReadTimeout     = 10 * time.Second
	defaultHTTPHeaderTimeout   = 5 * time.Second
	defaultHTTPWriteTimeout    = 30 * time.Second
	defaultHTTPIdleTimeout     = time.Minute
	defaultHTTPShutdownTimeout = 10 * time.Second
	defaultWorkerConcurrency   = 4
	defaultWorkerPollInterval  = time.Second
	defaultWorkerLeaseDuration = 25 * time.Hour
	defaultWorkerRetryDelay    = time.Minute
)

type runtimeRole string

const (
	roleAll       runtimeRole = "all"
	roleAPI       runtimeRole = "api"
	roleScheduler runtimeRole = "scheduler"
	roleWorker    runtimeRole = "worker"
)

type config struct {
	Role                    runtimeRole
	DatabaseURL             string
	RecoveryInterval        time.Duration
	RecoveryRetryDelay      time.Duration
	RecoveryBatchSize       int
	HTTPListenAddress       string
	HTTPReadTimeout         time.Duration
	HTTPReadHeaderTimeout   time.Duration
	HTTPWriteTimeout        time.Duration
	HTTPIdleTimeout         time.Duration
	HTTPShutdownTimeout     time.Duration
	WorkerID                job.WorkerID
	WorkerConcurrency       int
	WorkerPollInterval      time.Duration
	WorkerLeaseDuration     time.Duration
	WorkerRetryDelay        time.Duration
	WorkerHeartbeatInterval time.Duration
}

func loadConfig(getenv func(string) string) (config, error) {
	role, err := parseRole(getenv(roleEnvironment))
	if err != nil {
		return config{}, err
	}
	databaseURL := strings.TrimSpace(getenv(databaseURLEnvironment))
	if databaseURL == "" {
		return config{}, fmt.Errorf("%s is required", databaseURLEnvironment)
	}

	loaded := config{Role: role, DatabaseURL: databaseURL}
	if role.includesScheduler() {
		loaded.RecoveryInterval, err = environmentDuration(getenv, recoveryIntervalEnvironment, defaultRecoveryInterval)
		if err != nil {
			return config{}, err
		}
		loaded.RecoveryBatchSize, err = environmentInteger(getenv, recoveryBatchEnvironment, defaultRecoveryBatchSize)
		if err != nil {
			return config{}, err
		}
		loaded.RecoveryRetryDelay = defaultRecoveryRetryDelay
	}
	if role.includesAPI() {
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
		loaded.HTTPListenAddress = httpListenAddress
		loaded.HTTPReadTimeout = httpReadTimeout
		loaded.HTTPReadHeaderTimeout = httpHeaderTimeout
		loaded.HTTPWriteTimeout = httpWriteTimeout
		loaded.HTTPIdleTimeout = httpIdleTimeout
		loaded.HTTPShutdownTimeout = httpShutdownTimeout
	}
	if role.includesWorker() {
		workerID := job.WorkerID(strings.TrimSpace(getenv(workerIDEnvironment)))
		if workerID == "" {
			workerID = defaultWorkerID()
		}
		workerConcurrency, err := environmentInteger(getenv, workerConcurrencyEnvironment, defaultWorkerConcurrency)
		if err != nil {
			return config{}, err
		}
		workerPollInterval, err := environmentDuration(getenv, workerPollIntervalEnvironment, defaultWorkerPollInterval)
		if err != nil {
			return config{}, err
		}
		workerLeaseDuration, err := environmentDuration(getenv, workerLeaseDurationEnvironment, defaultWorkerLeaseDuration)
		if err != nil {
			return config{}, err
		}
		workerHeartbeatInterval, err := environmentDuration(getenv, workerHeartbeatEnvironment, workerLeaseDuration/3)
		if err != nil {
			return config{}, err
		}
		loaded.WorkerID = workerID
		loaded.WorkerConcurrency = workerConcurrency
		loaded.WorkerPollInterval = workerPollInterval
		loaded.WorkerLeaseDuration = workerLeaseDuration
		loaded.WorkerRetryDelay = defaultWorkerRetryDelay
		loaded.WorkerHeartbeatInterval = workerHeartbeatInterval
	}
	if err := loaded.validate(); err != nil {
		return config{}, err
	}
	return loaded, nil
}

func parseRole(value string) (runtimeRole, error) {
	switch role := runtimeRole(strings.ToLower(strings.TrimSpace(value))); role {
	case "", roleAll:
		return roleAll, nil
	case roleAPI, roleScheduler, roleWorker:
		return role, nil
	default:
		return "", fmt.Errorf("%s must be one of all, api, scheduler, or worker", roleEnvironment)
	}
}

func (role runtimeRole) includesAPI() bool       { return role == roleAll || role == roleAPI }
func (role runtimeRole) includesScheduler() bool { return role == roleAll || role == roleScheduler }
func (role runtimeRole) includesWorker() bool    { return role == roleAll || role == roleWorker }

func (configuration config) validate() error {
	if strings.TrimSpace(configuration.DatabaseURL) == "" {
		return errors.New("PostgreSQL connection string is required")
	}
	switch configuration.Role {
	case roleAll, roleAPI, roleScheduler, roleWorker:
	default:
		return errors.New("runtime role is invalid")
	}
	if configuration.Role.includesScheduler() && configuration.RecoveryInterval <= 0 {
		return errors.New("recovery interval must be positive")
	}
	if configuration.Role.includesScheduler() && configuration.RecoveryRetryDelay <= 0 {
		return errors.New("recovery retry delay must be positive")
	}
	if configuration.Role.includesScheduler() && (configuration.RecoveryBatchSize <= 0 || configuration.RecoveryBatchSize > postgres.MaxRecoveryBatchSize) {
		return fmt.Errorf("recovery batch size must be between 1 and %d", postgres.MaxRecoveryBatchSize)
	}
	if configuration.Role.includesAPI() && strings.TrimSpace(configuration.HTTPListenAddress) == "" {
		return errors.New("HTTP listen address must not be empty")
	}
	if configuration.Role.includesAPI() && (configuration.HTTPReadTimeout <= 0 || configuration.HTTPReadHeaderTimeout <= 0 || configuration.HTTPWriteTimeout <= 0 || configuration.HTTPIdleTimeout <= 0 || configuration.HTTPShutdownTimeout <= 0) {
		return errors.New("HTTP server timeouts must be positive")
	}
	if configuration.Role.includesWorker() && strings.TrimSpace(string(configuration.WorkerID)) == "" {
		return errors.New("worker ID must not be empty")
	}
	if configuration.Role.includesWorker() && configuration.WorkerConcurrency <= 0 {
		return errors.New("worker concurrency must be positive")
	}
	if configuration.Role.includesWorker() && (configuration.WorkerPollInterval <= 0 || configuration.WorkerLeaseDuration <= 0 || configuration.WorkerRetryDelay <= 0) {
		return errors.New("worker timing values must be positive")
	}
	if configuration.Role.includesWorker() && (configuration.WorkerHeartbeatInterval <= 0 || configuration.WorkerHeartbeatInterval >= configuration.WorkerLeaseDuration/2) {
		return errors.New("worker heartbeat interval must be positive and leave sufficient lease-renewal margin")
	}
	return nil
}

func defaultWorkerID() job.WorkerID {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}
	return job.WorkerID(fmt.Sprintf("%s-%d", hostname, os.Getpid()))
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

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/storage/postgres"
	"github.com/xtwo56/mercury/internal/task"
)

func TestLoadConfigDefaults(t *testing.T) {
	configuration, err := loadConfig(environment(map[string]string{
		databaseURLEnvironment: "postgres://mercury:secret@database/mercury",
	}))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if configuration.Role != roleAll {
		t.Errorf("Role = %q, want %q", configuration.Role, roleAll)
	}
	if configuration.RecoveryInterval != defaultRecoveryInterval {
		t.Errorf("RecoveryInterval = %v, want %v", configuration.RecoveryInterval, defaultRecoveryInterval)
	}
	if configuration.RecoveryBatchSize != defaultRecoveryBatchSize {
		t.Errorf("RecoveryBatchSize = %d, want %d", configuration.RecoveryBatchSize, defaultRecoveryBatchSize)
	}
	if configuration.RecoveryRetryDelay != defaultRecoveryRetryDelay {
		t.Errorf("RecoveryRetryDelay = %v, want %v", configuration.RecoveryRetryDelay, defaultRecoveryRetryDelay)
	}
	if configuration.HTTPListenAddress != defaultHTTPListenAddress ||
		configuration.HTTPReadTimeout != defaultHTTPReadTimeout ||
		configuration.HTTPReadHeaderTimeout != defaultHTTPHeaderTimeout ||
		configuration.HTTPWriteTimeout != defaultHTTPWriteTimeout ||
		configuration.HTTPIdleTimeout != defaultHTTPIdleTimeout ||
		configuration.HTTPShutdownTimeout != defaultHTTPShutdownTimeout {
		t.Errorf("HTTP defaults = %#v, want configured defaults", configuration)
	}
	if configuration.WorkerID == "" || configuration.WorkerConcurrency != defaultWorkerConcurrency ||
		configuration.WorkerPollInterval != defaultWorkerPollInterval || configuration.WorkerLeaseDuration != defaultWorkerLeaseDuration ||
		configuration.WorkerHeartbeatInterval != defaultWorkerLeaseDuration/3 {
		t.Errorf("worker defaults = %#v, want non-empty identity and configured defaults", configuration)
	}
	if configuration.WorkerLeaseDuration <= time.Duration(task.MaxSleepDurationMS)*time.Millisecond {
		t.Errorf("default worker lease %v must exceed maximum sleep duration", configuration.WorkerLeaseDuration)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	configuration, err := loadConfig(environment(map[string]string{
		databaseURLEnvironment:         "postgres://database/mercury",
		recoveryIntervalEnvironment:    "30s",
		recoveryBatchEnvironment:       "25",
		httpListenEnvironment:          "127.0.0.1:9090",
		httpReadTimeoutEnvironment:     "2s",
		httpHeaderTimeoutEnvironment:   "3s",
		httpWriteTimeoutEnvironment:    "4s",
		httpIdleTimeoutEnvironment:     "5s",
		httpShutdownTimeoutEnvironment: "6s",
		workerLeaseDurationEnvironment: "30s",
		workerHeartbeatEnvironment:     "8s",
	}))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if configuration.RecoveryInterval != 30*time.Second || configuration.RecoveryBatchSize != 25 {
		t.Errorf("loadConfig() = %#v, want interval 30s and batch 25", configuration)
	}
	if configuration.HTTPListenAddress != "127.0.0.1:9090" || configuration.HTTPReadTimeout != 2*time.Second ||
		configuration.HTTPReadHeaderTimeout != 3*time.Second || configuration.HTTPWriteTimeout != 4*time.Second ||
		configuration.HTTPIdleTimeout != 5*time.Second || configuration.HTTPShutdownTimeout != 6*time.Second {
		t.Errorf("HTTP overrides = %#v, want configured address and timeouts", configuration)
	}
	if configuration.WorkerLeaseDuration != 30*time.Second || configuration.WorkerHeartbeatInterval != 8*time.Second {
		t.Errorf("worker timing overrides = %#v", configuration)
	}
}

func TestLoadConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "missing database URL", values: map[string]string{}},
		{name: "blank database URL", values: map[string]string{databaseURLEnvironment: "  "}},
		{name: "invalid role", values: map[string]string{databaseURLEnvironment: "postgres://database/db", roleEnvironment: "other"}},
		{name: "invalid interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "often"}},
		{name: "zero interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "0s"}},
		{name: "negative interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "-1s"}},
		{name: "invalid batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "many"}},
		{name: "zero batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "0"}},
		{name: "negative batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "-1"}},
		{name: "oversized batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "1001"}},
		{name: "invalid HTTP read timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpReadTimeoutEnvironment: "slow"}},
		{name: "zero HTTP read timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpReadTimeoutEnvironment: "0s"}},
		{name: "negative HTTP header timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpHeaderTimeoutEnvironment: "-1s"}},
		{name: "zero HTTP write timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpWriteTimeoutEnvironment: "0s"}},
		{name: "zero HTTP idle timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpIdleTimeoutEnvironment: "0s"}},
		{name: "zero HTTP shutdown timeout", values: map[string]string{databaseURLEnvironment: "postgres://database/db", httpShutdownTimeoutEnvironment: "0s"}},
		{name: "zero worker concurrency", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerConcurrencyEnvironment: "0"}},
		{name: "invalid worker concurrency", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerConcurrencyEnvironment: "many"}},
		{name: "zero worker poll interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerPollIntervalEnvironment: "0s"}},
		{name: "zero worker lease duration", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerLeaseDurationEnvironment: "0s"}},
		{name: "invalid worker heartbeat", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerHeartbeatEnvironment: "often"}},
		{name: "zero worker heartbeat", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerHeartbeatEnvironment: "0s"}},
		{name: "heartbeat without margin", values: map[string]string{databaseURLEnvironment: "postgres://database/db", workerLeaseDurationEnvironment: "30s", workerHeartbeatEnvironment: "15s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(environment(tt.values))
			if err == nil {
				t.Fatal("loadConfig() error = nil, want validation error")
			}
			for _, value := range tt.values {
				if strings.Contains(err.Error(), value) && strings.Contains(value, "postgres://") {
					t.Errorf("loadConfig() error exposed database URL: %v", err)
				}
			}
		})
	}

	if postgres.MaxRecoveryBatchSize != 1000 {
		t.Errorf("MaxRecoveryBatchSize = %d, update oversized fixture", postgres.MaxRecoveryBatchSize)
	}
}

func TestRoleParsingAndRelevantConfiguration(t *testing.T) {
	for _, role := range []runtimeRole{roleAPI, roleScheduler, roleWorker, roleAll} {
		t.Run(string(role), func(t *testing.T) {
			configuration, err := loadConfig(environment(map[string]string{
				databaseURLEnvironment:        "postgres://database/db",
				roleEnvironment:               string(role),
				httpReadTimeoutEnvironment:    map[bool]string{true: "bad"}[!role.includesAPI()],
				recoveryIntervalEnvironment:   map[bool]string{true: "bad"}[!role.includesScheduler()],
				workerPollIntervalEnvironment: map[bool]string{true: "bad"}[!role.includesWorker()],
			}))
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if configuration.Role != role {
				t.Errorf("Role = %q, want %q", configuration.Role, role)
			}
			if !role.includesAPI() && (configuration.HTTPListenAddress != "" || configuration.HTTPReadTimeout != 0) {
				t.Errorf("excluded API configuration was loaded: %#v", configuration)
			}
			if !role.includesScheduler() && configuration.RecoveryInterval != 0 {
				t.Errorf("excluded scheduler configuration was loaded: %#v", configuration)
			}
			if !role.includesWorker() && configuration.WorkerID != "" {
				t.Errorf("excluded worker configuration was loaded: %#v", configuration)
			}
		})
	}
}

func TestConfigValidationIsRoleSpecific(t *testing.T) {
	tests := []struct {
		name       string
		role       runtimeRole
		invalidate func(*config)
		wantError  bool
	}{
		{name: "api ignores scheduler", role: roleAPI, invalidate: func(c *config) { c.RecoveryInterval = 0 }},
		{name: "api ignores worker", role: roleAPI, invalidate: func(c *config) { c.WorkerID = "" }},
		{name: "scheduler ignores HTTP", role: roleScheduler, invalidate: func(c *config) { c.HTTPListenAddress = "" }},
		{name: "scheduler ignores worker", role: roleScheduler, invalidate: func(c *config) { c.WorkerID = "" }},
		{name: "worker ignores HTTP", role: roleWorker, invalidate: func(c *config) { c.HTTPListenAddress = "" }},
		{name: "worker ignores scheduler", role: roleWorker, invalidate: func(c *config) { c.RecoveryInterval = 0 }},
		{name: "worker requires ID", role: roleWorker, invalidate: func(c *config) { c.WorkerID = "" }, wantError: true},
		{name: "all requires ID", role: roleAll, invalidate: func(c *config) { c.WorkerID = "" }, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configuration := validConfig()
			configuration.Role = tt.role
			tt.invalidate(&configuration)
			err := configuration.validate()
			if (err != nil) != tt.wantError {
				t.Errorf("validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func environment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

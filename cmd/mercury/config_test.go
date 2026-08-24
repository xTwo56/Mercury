package main

import (
	"strings"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/storage/postgres"
)

func TestLoadConfigDefaults(t *testing.T) {
	configuration, err := loadConfig(environment(map[string]string{
		databaseURLEnvironment: "postgres://mercury:secret@database/mercury",
	}))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
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
}

func TestLoadConfigOverrides(t *testing.T) {
	configuration, err := loadConfig(environment(map[string]string{
		databaseURLEnvironment:      "postgres://database/mercury",
		recoveryIntervalEnvironment: "30s",
		recoveryBatchEnvironment:    "25",
	}))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if configuration.RecoveryInterval != 30*time.Second || configuration.RecoveryBatchSize != 25 {
		t.Errorf("loadConfig() = %#v, want interval 30s and batch 25", configuration)
	}
}

func TestLoadConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "missing database URL", values: map[string]string{}},
		{name: "blank database URL", values: map[string]string{databaseURLEnvironment: "  "}},
		{name: "invalid interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "often"}},
		{name: "zero interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "0s"}},
		{name: "negative interval", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryIntervalEnvironment: "-1s"}},
		{name: "invalid batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "many"}},
		{name: "zero batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "0"}},
		{name: "negative batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "-1"}},
		{name: "oversized batch", values: map[string]string{databaseURLEnvironment: "postgres://database/db", recoveryBatchEnvironment: "1001"}},
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

func environment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// Package task defines reusable task submission contracts.
package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/xtwo56/mercury/internal/job"
)

const (
	// SleepTaskType identifies the demonstration sleep task.
	SleepTaskType job.TaskType = "sleep"
	// MaxSleepDurationMS bounds sleep execution to 24 hours.
	MaxSleepDurationMS int64 = 24 * 60 * 60 * 1000
)

// SleepPayload is shared by submission validation and task execution.
type SleepPayload struct {
	DurationMS int64 `json:"duration_ms"`
}

// ErrUnsupportedType indicates that no task definition is registered.
var ErrUnsupportedType = errors.New("unsupported task type")

// ErrInvalidPayload indicates that payload JSON violates a task contract.
var ErrInvalidPayload = errors.New("invalid task payload")

// Validator validates a task's JSON payload.
type Validator interface {
	Validate(json.RawMessage) error
}

// Registry maps task types to their submission contracts.
type Registry struct {
	definitions map[job.TaskType]Validator
}

// NewRegistry creates a registry from task definitions.
func NewRegistry(definitions map[job.TaskType]Validator) *Registry {
	copied := make(map[job.TaskType]Validator, len(definitions))
	for taskType, validator := range definitions {
		copied[taskType] = validator
	}
	return &Registry{definitions: copied}
}

// Validate checks a payload against its registered task definition.
func (registry *Registry) Validate(taskType job.TaskType, payload json.RawMessage) error {
	validator, ok := registry.definitions[taskType]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnsupportedType, taskType)
	}
	if err := validator.Validate(payload); err != nil {
		return fmt.Errorf("%w for %s: %v", ErrInvalidPayload, taskType, err)
	}
	return nil
}

// SleepValidator validates the demonstration sleep task contract.
type SleepValidator struct{}

// Validate requires one positive duration_ms integer and no unknown fields.
func (SleepValidator) Validate(payload json.RawMessage) error {
	_, err := DecodeSleepPayload(payload)
	return err
}

// DecodeSleepPayload strictly decodes and validates the shared sleep contract.
func DecodeSleepPayload(payload json.RawMessage) (SleepPayload, error) {
	var request SleepPayload
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return SleepPayload{}, errors.New("payload must be a JSON object containing duration_ms")
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return SleepPayload{}, err
	}
	if request.DurationMS <= 0 {
		return SleepPayload{}, errors.New("duration_ms must be a positive integer")
	}
	if request.DurationMS > MaxSleepDurationMS {
		return SleepPayload{}, fmt.Errorf("duration_ms must not exceed %d", MaxSleepDurationMS)
	}
	return request, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("payload must contain one JSON value")
	}
	return nil
}

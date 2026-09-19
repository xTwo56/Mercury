// Package job defines Mercury's submitted-work aggregate and its lifecycle
// rules. A Job is created queued, temporarily owned through a Lease, and then
// advanced by authenticated execution operations. The package has no clock,
// token generator, persistence, or synchronization of its own: callers supply
// time and credentials and must serialize mutations to a shared job.
package job

import (
	"encoding/json"
	"errors"
	"time"
)

type (
	// JobID uniquely identifies a submitted job across Mercury.
	JobID string
	// TaskType selects the registered task contract and execution handler.
	TaskType string
)

// Job is a submitted unit of work together with its current lifecycle state.
// Payload and Result contain JSON owned by the Job. Lease and lifecycle
// timestamps capture the current execution, while attempt and failure fields
// retain the information needed to decide whether another execution may start.
type Job struct {
	ID              JobID
	TaskType        TaskType
	Payload         json.RawMessage
	State           State
	MaxAttempts     int
	AttemptsStarted int
	CreatedAt       time.Time
	AvailableAt     time.Time
	Lease           *Lease
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Result          json.RawMessage
	LastError       string
	FailedAt        *time.Time
}

// New validates submitted data and creates a queued Job that becomes claimable
// at availableAt. The caller supplies identity and timestamps so creation is
// deterministic and testable. Payload is defensively copied, timestamps are
// stored in UTC, and no execution attempt is consumed during submission.
func New(id JobID, taskType TaskType, payload json.RawMessage, maxAttempts int, createdAt, availableAt time.Time) (Job, error) {
	if id == "" {
		return Job{}, errors.New("job ID must not be empty")
	}
	if taskType == "" {
		return Job{}, errors.New("task type must not be empty")
	}
	if !json.Valid(payload) {
		return Job{}, errors.New("payload must contain valid JSON")
	}
	if maxAttempts <= 0 {
		return Job{}, errors.New("maximum attempts must be greater than zero")
	}
	if createdAt.IsZero() {
		return Job{}, errors.New("created at must not be zero")
	}
	if availableAt.IsZero() {
		return Job{}, errors.New("available at must not be zero")
	}
	if availableAt.Before(createdAt) {
		return Job{}, errors.New("available at must not be earlier than created at")
	}

	return Job{
		ID:          id,
		TaskType:    taskType,
		Payload:     append(json.RawMessage(nil), payload...),
		State:       StateQueued,
		MaxAttempts: maxAttempts,
		CreatedAt:   createdAt.UTC(),
		AvailableAt: availableAt.UTC(),
	}, nil
}

// RemainingAttempts reports how many more leased-to-running transitions the
// attempt budget permits. It clamps exhausted or inconsistent persisted counts
// to zero so callers never observe an underflowed budget.
func (j Job) RemainingAttempts() int {
	if j.AttemptsStarted >= j.MaxAttempts {
		return 0
	}
	return j.MaxAttempts - j.AttemptsStarted
}

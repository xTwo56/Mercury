package job

import (
	"encoding/json"
	"errors"
	"time"
)

type (
	JobID    string
	TaskType string
)

// submitted unit of work.
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

// New creates a queued job from submitted job data.
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

// RemainingAttempts reports how many execution attempts may still be started.
func (j Job) RemainingAttempts() int {
	if j.AttemptsStarted >= j.MaxAttempts {
		return 0
	}
	return j.MaxAttempts - j.AttemptsStarted
}

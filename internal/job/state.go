package job

import "fmt"

// State represents the current state of a job.
type State string

const (
	StateQueued         State = "queued"
	StateLeased         State = "leased"
	StateRunning        State = "running"
	StateRetryScheduled State = "retry_scheduled"
	StateSucceeded      State = "succeeded"
	StateFailed         State = "failed"
)

// ParseState converts value to a State, rejecting unknown states.
func ParseState(value string) (State, error) {
	switch State(value) {
	case StateQueued, StateLeased, StateRunning, StateRetryScheduled, StateSucceeded, StateFailed:
		return State(value), nil
	default:
		return "", fmt.Errorf("invalid job state %q", value)
	}
}

// CanTransition reports whether a job may move directly from one state to another.
func CanTransition(from, to State) bool {
	return from == StateQueued && to == StateLeased ||
		from == StateRetryScheduled && to == StateLeased ||
		from == StateLeased && to == StateRunning ||
		from == StateRunning && to == StateSucceeded ||
		from == StateRunning && to == StateRetryScheduled ||
		from == StateRunning && to == StateFailed
}

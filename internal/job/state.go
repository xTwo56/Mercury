package job

import "fmt"

// State identifies a durable stage in the job execution lifecycle.
type State string

const (
	// StateQueued is eligible for claiming once AvailableAt is reached.
	StateQueued State = "queued"
	// StateLeased is temporarily owned by a worker but has not started execution.
	StateLeased State = "leased"
	// StateRunning has started an attempt under an active worker lease.
	StateRunning State = "running"
	// StateRetryScheduled becomes eligible for another claim at AvailableAt.
	StateRetryScheduled State = "retry_scheduled"
	// StateSucceeded is terminal and contains a successful result.
	StateSucceeded State = "succeeded"
	// StateFailed is terminal because the execution attempt budget is exhausted.
	StateFailed State = "failed"
)

// ParseState converts a persisted or external string to a supported State.
// Rejecting unknown values prevents invalid storage data from entering the
// domain aggregate.
func ParseState(value string) (State, error) {
	switch State(value) {
	case StateQueued, StateLeased, StateRunning, StateRetryScheduled, StateSucceeded, StateFailed:
		return State(value), nil
	default:
		return "", fmt.Errorf("invalid job state %q", value)
	}
}

// CanTransition reports whether the lifecycle permits a direct state change.
// It includes worker-driven execution paths and system-driven lease recovery;
// terminal states intentionally have no outgoing transitions.
func CanTransition(from, to State) bool {
	return from == StateQueued && to == StateLeased ||
		from == StateRetryScheduled && to == StateLeased ||
		from == StateLeased && to == StateQueued ||
		from == StateLeased && to == StateRunning ||
		from == StateRunning && to == StateSucceeded ||
		from == StateRunning && to == StateRetryScheduled ||
		from == StateRunning && to == StateFailed
}

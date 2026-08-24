package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Timer is the cancellation-aware wait boundary used by task handlers.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// TimerFactory creates timers for task execution.
type TimerFactory interface {
	NewTimer(time.Duration) (Timer, error)
}

// SleepHandler executes validated sleep task payloads.
type SleepHandler struct {
	timers TimerFactory
}

// NewSleepHandler constructs a sleep handler with injectable timing.
func NewSleepHandler(timers TimerFactory) (*SleepHandler, error) {
	if timers == nil {
		return nil, errors.New("sleep timer factory must not be nil")
	}
	return &SleepHandler{timers: timers}, nil
}

// Execute waits for duration_ms or returns promptly when context is cancelled.
func (handler *SleepHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	request, err := DecodeSleepPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("validate sleep payload: %w", err)
	}
	timer, err := handler.timers.NewTimer(time.Duration(request.DurationMS) * time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("create sleep timer: %w", err)
	}
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C():
		result, err := json.Marshal(SleepPayload{DurationMS: request.DurationMS})
		if err != nil {
			return nil, fmt.Errorf("encode sleep result: %w", err)
		}
		return result, nil
	}
}

type systemTimerFactory struct{}

// NewSystemTimerFactory returns a factory backed by time.NewTimer.
func NewSystemTimerFactory() TimerFactory { return systemTimerFactory{} }

func (systemTimerFactory) NewTimer(duration time.Duration) (Timer, error) {
	return systemTimer{Timer: time.NewTimer(duration)}, nil
}

type systemTimer struct{ *time.Timer }

func (timer systemTimer) C() <-chan time.Time { return timer.Timer.C }

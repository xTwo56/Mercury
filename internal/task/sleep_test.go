package task

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSleepHandlerExecute(t *testing.T) {
	timers := &fakeTimerFactory{timer: newFakeTimer(), created: make(chan time.Duration, 1)}
	handler := newTestSleepHandler(t, timers)
	done := make(chan sleepExecutionResult, 1)
	go func() {
		payload, err := handler.Execute(context.Background(), json.RawMessage(`{"duration_ms":125}`))
		done <- sleepExecutionResult{payload: payload, err: err}
	}()

	duration := receiveDuration(t, timers.created)
	if duration != 125*time.Millisecond {
		t.Errorf("timer duration = %v, want 125ms", duration)
	}
	timers.timer.fire()
	completed := receiveSleepResult(t, done)
	if completed.err != nil {
		t.Fatalf("Execute() error = %v", completed.err)
	}
	if string(completed.payload) != `{"duration_ms":125}` {
		t.Errorf("Execute() result = %s, want exact duration JSON", completed.payload)
	}
	if !json.Valid(completed.payload) || !timers.timer.stopped {
		t.Errorf("Execute() result valid/stopped = %v/%v, want true/true", json.Valid(completed.payload), timers.timer.stopped)
	}
}

func TestSleepHandlerRejectsInvalidPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload json.RawMessage
	}{
		{name: "malformed", payload: json.RawMessage(`{"duration_ms":`)},
		{name: "missing", payload: json.RawMessage(`{}`)},
		{name: "zero", payload: json.RawMessage(`{"duration_ms":0}`)},
		{name: "negative", payload: json.RawMessage(`{"duration_ms":-1}`)},
		{name: "fractional", payload: json.RawMessage(`{"duration_ms":1.5}`)},
		{name: "unknown field", payload: json.RawMessage(`{"duration_ms":1,"extra":true}`)},
		{name: "over maximum", payload: json.RawMessage(`{"duration_ms":86400001}`)},
		{name: "trailing JSON", payload: json.RawMessage(`{"duration_ms":1}{}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timers := &fakeTimerFactory{timer: newFakeTimer()}
			handler := newTestSleepHandler(t, timers)
			if _, err := handler.Execute(context.Background(), tt.payload); err == nil {
				t.Fatal("Execute() error = nil, want payload rejection")
			}
			if len(timers.durations) != 0 {
				t.Errorf("invalid payload created %d timers", len(timers.durations))
			}
		})
	}
}

func TestSleepHandlerCancellation(t *testing.T) {
	timer := newFakeTimer()
	handler := newTestSleepHandler(t, &fakeTimerFactory{timer: timer})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := handler.Execute(ctx, json.RawMessage(`{"duration_ms":1}`))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Execute() error = %v, want context.Canceled", err)
	}
	if result != nil || !timer.stopped {
		t.Errorf("Execute() result/stopped = %s/%v, want nil/true", result, timer.stopped)
	}
}

func TestSleepHandlerTimerFailure(t *testing.T) {
	want := errors.New("timer unavailable")
	handler := newTestSleepHandler(t, &fakeTimerFactory{err: want})
	if _, err := handler.Execute(context.Background(), json.RawMessage(`{"duration_ms":1}`)); !errors.Is(err, want) {
		t.Errorf("Execute() error = %v, want timer failure", err)
	}
}

func TestNewSleepHandlerRejectsNilTimerFactory(t *testing.T) {
	if _, err := NewSleepHandler(nil); err == nil {
		t.Fatal("NewSleepHandler() error = nil, want validation error")
	}
}

func newTestSleepHandler(t *testing.T, timers TimerFactory) *SleepHandler {
	t.Helper()
	handler, err := NewSleepHandler(timers)
	if err != nil {
		t.Fatalf("NewSleepHandler() error = %v", err)
	}
	return handler
}

type fakeTimerFactory struct {
	timer     *fakeTimer
	err       error
	durations []time.Duration
	created   chan time.Duration
}

func (factory *fakeTimerFactory) NewTimer(duration time.Duration) (Timer, error) {
	factory.durations = append(factory.durations, duration)
	if factory.created != nil {
		factory.created <- duration
	}
	if factory.err != nil {
		return nil, factory.err
	}
	return factory.timer, nil
}

type fakeTimer struct {
	ticks   chan time.Time
	stopped bool
}

func newFakeTimer() *fakeTimer               { return &fakeTimer{ticks: make(chan time.Time, 1)} }
func (timer *fakeTimer) C() <-chan time.Time { return timer.ticks }
func (timer *fakeTimer) Stop() bool          { timer.stopped = true; return true }
func (timer *fakeTimer) fire()               { timer.ticks <- time.Time{} }

func receiveDuration(t *testing.T, channel <-chan time.Duration) time.Duration {
	t.Helper()
	if channel == nil {
		t.Fatal("timer creation channel is nil")
	}
	select {
	case duration := <-channel:
		return duration
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for timer creation")
		return 0
	}
}

type sleepExecutionResult struct {
	payload json.RawMessage
	err     error
}

func receiveSleepResult(t *testing.T, channel <-chan sleepExecutionResult) sleepExecutionResult {
	t.Helper()
	select {
	case result := <-channel:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sleep result")
		return sleepExecutionResult{}
	}
}

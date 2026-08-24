package task

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/xtwo56/mercury/internal/job"
)

func TestHandlerRegistryRegistrationAndLookup(t *testing.T) {
	registry := NewHandlerRegistry()
	handler := handlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	if err := registry.Register(SleepTaskType, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, err := registry.Lookup(SleepTaskType)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if got == nil {
		t.Error("Lookup() returned a nil handler")
	}
	if err := registry.Register(job.TaskType("other"), handler); !errors.Is(err, ErrHandlerRegistrySealed) {
		t.Errorf("Register() after lookup error = %v, want ErrHandlerRegistrySealed", err)
	}
}

func TestHandlerRegistryRejectsInvalidRegistration(t *testing.T) {
	valid := handlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	var typedNil *SleepHandler
	tests := []struct {
		name     string
		taskType job.TaskType
		handler  Handler
		prepare  func(*HandlerRegistry)
		want     error
	}{
		{name: "empty task type", handler: valid},
		{name: "nil handler", taskType: SleepTaskType},
		{name: "typed nil handler", taskType: SleepTaskType, handler: typedNil},
		{name: "duplicate", taskType: SleepTaskType, handler: valid, prepare: func(registry *HandlerRegistry) {
			if err := registry.Register(SleepTaskType, valid); err != nil {
				t.Fatalf("prepare Register() error = %v", err)
			}
		}, want: ErrDuplicateHandler},
		{name: "explicitly sealed", taskType: SleepTaskType, handler: valid, prepare: func(registry *HandlerRegistry) { registry.Seal() }, want: ErrHandlerRegistrySealed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewHandlerRegistry()
			if tt.prepare != nil {
				tt.prepare(registry)
			}
			err := registry.Register(tt.taskType, tt.handler)
			if err == nil {
				t.Fatal("Register() error = nil, want rejection")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("Register() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestHandlerRegistryUnknownTask(t *testing.T) {
	registry := NewHandlerRegistry()
	if _, err := registry.Lookup(job.TaskType("unknown")); !errors.Is(err, ErrUnsupportedType) {
		t.Errorf("Lookup() error = %v, want ErrUnsupportedType", err)
	}
}

func TestHandlerRegistryConcurrentLookups(t *testing.T) {
	registry := NewHandlerRegistry()
	handler := handlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	if err := registry.Register(SleepTaskType, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	registry.Seal()

	const lookups = 100
	errorsFound := make(chan error, lookups)
	var wait sync.WaitGroup
	for range lookups {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := registry.Lookup(SleepTaskType)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Errorf("concurrent Lookup() error = %v", err)
		}
	}
}

type handlerFunc func(context.Context, json.RawMessage) (json.RawMessage, error)

func (handler handlerFunc) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return handler(ctx, payload)
}

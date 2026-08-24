package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/xtwo56/mercury/internal/job"
)

var (
	// ErrDuplicateHandler indicates that a task type already has a handler.
	ErrDuplicateHandler = errors.New("task handler already registered")
	// ErrHandlerRegistrySealed indicates that handler registration has closed.
	ErrHandlerRegistrySealed = errors.New("task handler registry is sealed")
)

// Handler executes one task payload without changing job lifecycle state.
type Handler interface {
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}

// HandlerRegistry stores immutable task execution routing after first use.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[job.TaskType]Handler
	sealed   bool
}

// NewHandlerRegistry creates an empty execution-handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[job.TaskType]Handler)}
}

// Register associates a task type with one handler before the registry is sealed.
func (registry *HandlerRegistry) Register(taskType job.TaskType, handler Handler) error {
	if taskType == "" {
		return errors.New("task type must not be empty")
	}
	if isNilHandler(handler) {
		return errors.New("task handler must not be nil")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return ErrHandlerRegistrySealed
	}
	if _, exists := registry.handlers[taskType]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateHandler, taskType)
	}
	registry.handlers[taskType] = handler
	return nil
}

// Seal prevents all subsequent handler registrations.
func (registry *HandlerRegistry) Seal() {
	registry.mu.Lock()
	registry.sealed = true
	registry.mu.Unlock()
}

// Lookup returns a handler and seals the registry on first worker lookup.
func (registry *HandlerRegistry) Lookup(taskType job.TaskType) (Handler, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sealed = true
	handler, ok := registry.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedType, taskType)
	}
	return handler, nil
}

func isNilHandler(handler Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

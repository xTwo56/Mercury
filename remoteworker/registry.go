package remoteworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

var (
	// ErrDuplicateHandler means a task type already has an execution handler.
	ErrDuplicateHandler = errors.New("remote worker handler already registered")
	// ErrRegistrySealed means registration was attempted after execution began.
	ErrRegistrySealed = errors.New("remote worker handler registry is sealed")
	// ErrUnsupportedTask means no registered handler accepts a returned task type.
	ErrUnsupportedTask = errors.New("remote worker task type is unsupported")
)

// Handler executes one task payload and returns a JSON result. It does not
// mutate Mercury lifecycle state; Runtime starts, heartbeats, and reports the
// outcome around this call. A handler must stop when ctx is cancelled.
type Handler interface {
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, json.RawMessage) (json.RawMessage, error)

// Execute calls function.
func (function HandlerFunc) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return function(ctx, payload)
}

// Registry stores task handlers and becomes immutable when a Runtime starts.
// Its sorted type snapshot is sent with every claim so Mercury never leases a
// task this process cannot execute.
type Registry struct {
	mu       sync.RWMutex
	handlers map[workerclient.TaskType]Handler
	sealed   bool
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[workerclient.TaskType]Handler)}
}

// Register associates one non-empty task type with one non-nil handler.
func (registry *Registry) Register(taskType workerclient.TaskType, handler Handler) error {
	if taskType == "" {
		return errors.New("remote worker task type must not be empty")
	}
	if nilHandler(handler) {
		return errors.New("remote worker handler must not be nil")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return ErrRegistrySealed
	}
	if _, exists := registry.handlers[taskType]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateHandler, taskType)
	}
	registry.handlers[taskType] = handler
	return nil
}

func (registry *Registry) sealAndTypes() []workerclient.TaskType {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sealed = true
	types := make([]workerclient.TaskType, 0, len(registry.handlers))
	for taskType := range registry.handlers {
		types = append(types, taskType)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })
	return types
}

func (registry *Registry) lookup(taskType workerclient.TaskType) (Handler, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	handler, ok := registry.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedTask, taskType)
	}
	return handler, nil
}

func nilHandler(handler Handler) bool {
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

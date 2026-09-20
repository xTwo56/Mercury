package remoteworker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

func TestRegistryRegistrationAndSeal(t *testing.T) {
	registry := NewRegistry()
	handler := HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`null`), nil
	})
	if err := registry.Register("zeta", handler); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", handler); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", handler); !errors.Is(err, ErrDuplicateHandler) {
		t.Fatalf("duplicate error = %v", err)
	}
	if types := registry.sealAndTypes(); !reflect.DeepEqual(types, []workerclient.TaskType{"alpha", "zeta"}) {
		t.Fatalf("types = %v", types)
	}
	if err := registry.Register("later", handler); !errors.Is(err, ErrRegistrySealed) {
		t.Fatalf("sealed error = %v", err)
	}
}

func TestRegistryRejectsInvalidRegistration(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })); err == nil {
		t.Fatal("empty task type accepted")
	}
	if err := registry.Register("task", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
}

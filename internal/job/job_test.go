package job

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	location := time.FixedZone("test", 5*60*60+30*60)
	createdAt := time.Date(2026, time.August, 14, 12, 0, 0, 0, location)
	delayedUntil := createdAt.Add(30 * time.Minute)

	tests := []struct {
		name        string
		id          JobID
		taskType    TaskType
		payload     json.RawMessage
		createdAt   time.Time
		availableAt time.Time
		want        Job
		wantErr     bool
	}{
		{
			name:        "immediate job",
			id:          JobID("job-1"),
			taskType:    TaskType("send-email"),
			payload:     json.RawMessage(`{"recipient":"user@example.com"}`),
			createdAt:   createdAt,
			availableAt: createdAt,
			want: Job{
				ID:          JobID("job-1"),
				TaskType:    TaskType("send-email"),
				Payload:     json.RawMessage(`{"recipient":"user@example.com"}`),
				State:       StateQueued,
				CreatedAt:   createdAt.UTC(),
				AvailableAt: createdAt.UTC(),
			},
		},
		{
			name:        "delayed job",
			id:          JobID("job-2"),
			taskType:    TaskType("record-value"),
			payload:     json.RawMessage(`42`),
			createdAt:   createdAt,
			availableAt: delayedUntil,
			want: Job{
				ID:          JobID("job-2"),
				TaskType:    TaskType("record-value"),
				Payload:     json.RawMessage(`42`),
				State:       StateQueued,
				CreatedAt:   createdAt.UTC(),
				AvailableAt: delayedUntil.UTC(),
			},
		},
		{name: "empty ID", taskType: TaskType("send-email"), payload: json.RawMessage(`{}`), createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "empty task type", id: JobID("job-1"), payload: json.RawMessage(`{}`), createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "nil payload", id: JobID("job-1"), taskType: TaskType("send-email"), createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "empty payload", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage{}, createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "whitespace-only payload", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage(`   `), createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "invalid payload", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage(`{"recipient":}`), createdAt: createdAt, availableAt: createdAt, wantErr: true},
		{name: "zero created at", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage(`{}`), availableAt: createdAt, wantErr: true},
		{name: "zero available at", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage(`{}`), createdAt: createdAt, wantErr: true},
		{name: "available before creation", id: JobID("job-1"), taskType: TaskType("send-email"), payload: json.RawMessage(`{}`), createdAt: createdAt, availableAt: createdAt.Add(-time.Nanosecond), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.id, tt.taskType, tt.payload, tt.createdAt, tt.availableAt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNewCopiesPayload(t *testing.T) {
	payload := json.RawMessage(`{"value":"original"}`)
	now := time.Date(2026, time.August, 14, 6, 30, 0, 0, time.UTC)

	got, err := New(JobID("job-1"), TaskType("test"), payload, now, now)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	payload[10] = 'X'
	if string(got.Payload) != `{"value":"original"}` {
		t.Errorf("Job.Payload = %q after source mutation, want an independent copy", got.Payload)
	}
}

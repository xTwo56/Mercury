package job

import "testing"

func TestParseState(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    State
		wantErr bool
	}{
		{name: "queued", value: "queued", want: StateQueued},
		{name: "leased", value: "leased", want: StateLeased},
		{name: "running", value: "running", want: StateRunning},
		{name: "succeeded", value: "succeeded", want: StateSucceeded},
		{name: "empty", value: "", wantErr: true},
		{name: "unknown", value: "completed", wantErr: true},
		{name: "wrong case", value: "Queued", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseState(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseState(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParseState(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestCanTransition(t *testing.T) {
	tests := []struct {
		name string
		from State
		to   State
		want bool
	}{
		{name: "queued to leased", from: StateQueued, to: StateLeased, want: true},
		{name: "leased to running", from: StateLeased, to: StateRunning, want: true},
		{name: "running to succeeded", from: StateRunning, to: StateSucceeded, want: true},
		{name: "queued to running", from: StateQueued, to: StateRunning, want: false},
		{name: "leased to queued", from: StateLeased, to: StateQueued, want: false},
		{name: "running to queued", from: StateRunning, to: StateQueued, want: false},
		{name: "running to leased", from: StateRunning, to: StateLeased, want: false},
		{name: "queued to queued", from: StateQueued, to: StateQueued, want: false},
		{name: "leased to leased", from: StateLeased, to: StateLeased, want: false},
		{name: "running to running", from: StateRunning, to: StateRunning, want: false},
		{name: "succeeded to queued", from: StateSucceeded, to: StateQueued, want: false},
		{name: "succeeded to leased", from: StateSucceeded, to: StateLeased, want: false},
		{name: "succeeded to running", from: StateSucceeded, to: StateRunning, want: false},
		{name: "succeeded to succeeded", from: StateSucceeded, to: StateSucceeded, want: false},
		{name: "unknown source", from: State("unknown"), to: StateLeased, want: false},
		{name: "unknown destination", from: StateQueued, to: State("unknown"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("CanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

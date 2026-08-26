package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtwo56/mercury/internal/job"
)

func TestFingerprintSubmissionCanonicalAndSensitiveFields(t *testing.T) {
	three := 3
	available := time.Date(2026, 8, 26, 12, 0, 0, 0, time.FixedZone("offset", 5*60*60+30*60))
	base := Submission{TaskType: job.TaskType("sleep"), Payload: json.RawMessage(`{"duration_ms":1,"nested":{"a":true,"b":null}}`)}
	baseFingerprint := mustFingerprint(t, base)

	equivalent := base
	equivalent.Payload = json.RawMessage(" { \"nested\" : { \"b\":null, \"a\":true }, \"duration_ms\" : 1.0 } ")
	if got := mustFingerprint(t, equivalent); got != baseFingerprint {
		t.Error("canonically equivalent JSON produced different fingerprints")
	}

	tests := []struct {
		name   string
		mutate func(*Submission)
	}{
		{name: "task type", mutate: func(s *Submission) { s.TaskType = job.TaskType("other") }},
		{name: "payload", mutate: func(s *Submission) { s.Payload = json.RawMessage(`{"duration_ms":2,"nested":{"a":true,"b":null}}`) }},
		{name: "explicit attempts", mutate: func(s *Submission) { s.MaxAttempts = &three }},
		{name: "explicit availability", mutate: func(s *Submission) { s.AvailableAt = &available }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := base
			tt.mutate(&changed)
			if got := mustFingerprint(t, changed); got == baseFingerprint {
				t.Error("logical submission change did not change fingerprint")
			}
		})
	}

	utcEquivalent := available.UTC()
	withAvailability := base
	withAvailability.AvailableAt = &available
	withUTC := base
	withUTC.AvailableAt = &utcEquivalent
	if mustFingerprint(t, withAvailability) != mustFingerprint(t, withUTC) {
		t.Error("equal availability instants produced different fingerprints")
	}
}

func mustFingerprint(t *testing.T, submission Submission) [32]byte {
	t.Helper()
	fingerprint, err := fingerprintSubmission(submission)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

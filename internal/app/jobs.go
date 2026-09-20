// Package app coordinates Mercury's user-facing job use cases. JobService
// validates a task submission, applies application defaults, constructs the
// domain aggregate, and delegates durable creation or retrieval to a narrow
// repository interface. HTTP details, domain lifecycle transitions, and
// PostgreSQL transaction mechanics remain in their respective packages.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/task"
)

// defaultMaxAttempts is applied only when a submitter omits an attempt limit.
const defaultMaxAttempts = 3

// ErrJobNotFound is the application-level classification for a missing job.
// Transport adapters can match it without depending on storage-specific errors.
var ErrJobNotFound = errors.New("job not found")

// ErrInvalidSubmission identifies fields that passed task validation but cannot
// form a valid domain Job, such as an invalid attempt or availability value.
var ErrInvalidSubmission = errors.New("invalid job submission")

// ErrIdempotencyConflict identifies reuse of an idempotency key with a
// different logical submission.
var ErrIdempotencyConflict = errors.New("idempotency key conflict")

// JobRepository is the persistence boundary required by JobService. The
// implementation owns database transactions and concurrency control;
// CreateIdempotent must atomically create or return the job already bound to
// the key and report whether creation occurred.
type JobRepository interface {
	Create(context.Context, job.Job) error
	CreateIdempotent(context.Context, job.Job, string, []byte) (job.Job, bool, error)
	GetByID(context.Context, job.JobID) (job.Job, error)
}

// Clock supplies the creation instant used for submission defaults and domain
// construction. Injection keeps the use case deterministic in tests.
type Clock interface {
	Now() time.Time
}

// IDGenerator creates externally visible job IDs without coupling the use case
// to a particular identifier format or randomness source.
type IDGenerator interface {
	NewJobID() (job.JobID, error)
}

// Submission contains caller-controlled job fields after transport decoding.
// Pointer fields preserve the difference between omission and an explicit
// value, which affects both defaulting and idempotency fingerprints.
type Submission struct {
	TaskType       job.TaskType
	Payload        json.RawMessage
	MaxAttempts    *int
	AvailableAt    *time.Time
	IdempotencyKey *string
}

// SubmissionResult contains the persisted Job and reports whether an
// idempotency key replayed an existing row rather than creating this candidate.
type SubmissionResult struct {
	Job      job.Job
	Replayed bool
}

// JobService orchestrates task validation, domain construction, persistence,
// and translation of repository-specific error classifications. Its injected
// collaborators keep the application layer independent of HTTP and PostgreSQL.
type JobService struct {
	repository            JobRepository
	tasks                 *task.Registry
	clock                 Clock
	ids                   IDGenerator
	isNotFound            func(error) bool
	isIdempotencyConflict func(error) bool
}

// NewJobService validates and stores the collaborators required by job use
// cases. Error-classifier functions adapt repository sentinel errors without
// importing a concrete storage implementation.
func NewJobService(repository JobRepository, tasks *task.Registry, clock Clock, ids IDGenerator, isNotFound, isIdempotencyConflict func(error) bool) (*JobService, error) {
	if repository == nil || tasks == nil || clock == nil || ids == nil || isNotFound == nil || isIdempotencyConflict == nil {
		return nil, errors.New("job service dependencies must not be nil")
	}
	return &JobService{repository: repository, tasks: tasks, clock: clock, ids: ids, isNotFound: isNotFound, isIdempotencyConflict: isIdempotencyConflict}, nil
}

// Submit validates the registered task contract, resolves omitted defaults,
// creates a queued domain Job, and persists it. Without an idempotency key each
// call creates independently. With a key, a deterministic fingerprint is sent
// to the repository's atomic create-or-replay operation; a replay returns the
// existing Job in its current state. Repository and generator errors retain
// their causes, while invalid domain input is classified as ErrInvalidSubmission.
func (service *JobService) Submit(ctx context.Context, submission Submission) (SubmissionResult, error) {
	// Admission validation runs before generating an ID or touching storage. A
	// built-in contract may validate task fields precisely, while a configured
	// external contract validates only generic JSON and delegates semantics to
	// its remote handler.
	if err := service.tasks.Validate(submission.TaskType, submission.Payload); err != nil {
		return SubmissionResult{}, err
	}
	now := service.clock.Now()
	availableAt := now
	if submission.AvailableAt != nil {
		availableAt = *submission.AvailableAt
	}
	maxAttempts := defaultMaxAttempts
	if submission.MaxAttempts != nil {
		maxAttempts = *submission.MaxAttempts
	}
	id, err := service.ids.NewJobID()
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("generate job ID: %w", err)
	}
	created, err := job.New(id, submission.TaskType, submission.Payload, maxAttempts, now, availableAt)
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("%w: %v", ErrInvalidSubmission, err)
	}
	if submission.IdempotencyKey == nil {
		if err := service.repository.Create(ctx, created); err != nil {
			return SubmissionResult{}, fmt.Errorf("persist job: %w", err)
		}
		return SubmissionResult{Job: created}, nil
	}
	// Fingerprint the caller's logical input rather than resolved defaults or
	// generated fields, preserving omitted-versus-explicit request semantics.
	fingerprint, err := fingerprintSubmission(submission)
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("%w: fingerprint payload: %v", ErrInvalidSubmission, err)
	}
	persisted, wasCreated, err := service.repository.CreateIdempotent(ctx, created, *submission.IdempotencyKey, fingerprint[:])
	if err != nil {
		if service.isIdempotencyConflict(err) {
			return SubmissionResult{}, fmt.Errorf("persist job: %w", ErrIdempotencyConflict)
		}
		return SubmissionResult{}, fmt.Errorf("persist job: %w", err)
	}
	return SubmissionResult{Job: persisted, Replayed: !wasCreated}, nil
}

// Get returns the latest persisted Job. It translates only the configured
// repository not-found classification, preserving other wrapped causes for
// logging and operational error handling above this layer.
func (service *JobService) Get(ctx context.Context, id job.JobID) (job.Job, error) {
	loaded, err := service.repository.GetByID(ctx, id)
	if err != nil {
		if service.isNotFound(err) {
			return job.Job{}, fmt.Errorf("get job %q: %w", id, ErrJobNotFound)
		}
		return job.Job{}, fmt.Errorf("get job %q: %w", id, err)
	}
	return loaded, nil
}

// SystemClock supplies the process wall-clock time for production submissions.
type SystemClock struct{}

// Now returns the current time; job.New normalizes stored timestamps to UTC.
func (SystemClock) Now() time.Time { return time.Now() }

// RandomIDGenerator creates RFC 4122 version 4 job IDs using cryptographic
// randomness, making independently generated public identifiers impractical to
// predict or collide.
type RandomIDGenerator struct{}

// NewJobID creates a random RFC 4122 version 4 identifier and propagates a
// randomness-source failure without producing a partial ID.
func (RandomIDGenerator) NewJobID() (job.JobID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])
	return job.JobID(encoded), nil
}

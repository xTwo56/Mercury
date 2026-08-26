// Package app coordinates Mercury application use cases.
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

const defaultMaxAttempts = 3

// ErrJobNotFound indicates that the requested job does not exist.
var ErrJobNotFound = errors.New("job not found")

// ErrInvalidSubmission indicates that submitted fields cannot form a job.
var ErrInvalidSubmission = errors.New("invalid job submission")

// ErrIdempotencyConflict indicates reuse of a key for another submission.
var ErrIdempotencyConflict = errors.New("idempotency key conflict")

// JobRepository is the persistence boundary required by job use cases.
type JobRepository interface {
	Create(context.Context, job.Job) error
	CreateIdempotent(context.Context, job.Job, string, []byte) (job.Job, bool, error)
	GetByID(context.Context, job.JobID) (job.Job, error)
}

// Clock supplies submission timestamps.
type Clock interface {
	Now() time.Time
}

// IDGenerator creates externally visible job IDs.
type IDGenerator interface {
	NewJobID() (job.JobID, error)
}

// Submission contains caller-controlled job fields.
type Submission struct {
	TaskType       job.TaskType
	Payload        json.RawMessage
	MaxAttempts    *int
	AvailableAt    *time.Time
	IdempotencyKey *string
}

// SubmissionResult identifies the persisted job and whether it was replayed.
type SubmissionResult struct {
	Job      job.Job
	Replayed bool
}

// JobService submits and retrieves jobs.
type JobService struct {
	repository            JobRepository
	tasks                 *task.Registry
	clock                 Clock
	ids                   IDGenerator
	isNotFound            func(error) bool
	isIdempotencyConflict func(error) bool
}

// NewJobService constructs the job application service.
func NewJobService(repository JobRepository, tasks *task.Registry, clock Clock, ids IDGenerator, isNotFound, isIdempotencyConflict func(error) bool) (*JobService, error) {
	if repository == nil || tasks == nil || clock == nil || ids == nil || isNotFound == nil || isIdempotencyConflict == nil {
		return nil, errors.New("job service dependencies must not be nil")
	}
	return &JobService{repository: repository, tasks: tasks, clock: clock, ids: ids, isNotFound: isNotFound, isIdempotencyConflict: isIdempotencyConflict}, nil
}

// Submit validates, constructs, and persists a submitted job.
func (service *JobService) Submit(ctx context.Context, submission Submission) (SubmissionResult, error) {
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

// Get returns a job while translating repository not-found errors.
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

// SystemClock supplies the current wall-clock time.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// RandomIDGenerator creates UUID-shaped IDs from cryptographic randomness.
type RandomIDGenerator struct{}

// NewJobID creates a random RFC 4122 version 4 identifier.
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

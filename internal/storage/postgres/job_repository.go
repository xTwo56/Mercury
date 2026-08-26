// Package postgres persists Mercury domain objects in PostgreSQL.
package postgres

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/xtwo56/mercury/internal/job"
	"github.com/xtwo56/mercury/internal/storage/postgres/generated"
)

// ErrJobNotFound indicates that no job exists with the requested ID.
var ErrJobNotFound = errors.New("job not found")

// ErrNoJobAvailable indicates that no job is currently eligible to be claimed.
var ErrNoJobAvailable = errors.New("no job available")

// ErrIdempotencyConflict indicates that a key belongs to another submission.
var ErrIdempotencyConflict = errors.New("idempotency key conflict")

// MaxRecoveryBatchSize is the largest batch accepted by expired-lease recovery.
const MaxRecoveryBatchSize = 1000

type transactionalDB interface {
	generated.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

// JobRepository stores and retrieves jobs from PostgreSQL.
type JobRepository struct {
	db      transactionalDB
	queries *generated.Queries
}

// NewJobRepository creates a job repository backed by db.
func NewJobRepository(db transactionalDB) *JobRepository {
	return &JobRepository{db: db, queries: generated.New(db)}
}

// Create inserts a job in its current domain state.
func (r *JobRepository) Create(ctx context.Context, j job.Job) error {
	maxAttempts, err := postgresInteger(j.MaxAttempts)
	if err != nil {
		return fmt.Errorf("create job %q: max attempts: %w", j.ID, err)
	}
	attemptsStarted, err := postgresInteger(j.AttemptsStarted)
	if err != nil {
		return fmt.Errorf("create job %q: attempts started: %w", j.ID, err)
	}

	leaseWorkerID, leaseToken, leaseExpiresAt := nullableLease(j.Lease)
	err = r.queries.CreateJob(ctx, generated.CreateJobParams{
		ID:              string(j.ID),
		TaskType:        string(j.TaskType),
		Payload:         append([]byte(nil), j.Payload...),
		State:           string(j.State),
		MaxAttempts:     maxAttempts,
		AttemptsStarted: attemptsStarted,
		CreatedAt:       timestamptz(j.CreatedAt),
		AvailableAt:     timestamptz(j.AvailableAt),
		LeaseWorkerID:   leaseWorkerID,
		LeaseToken:      leaseToken,
		LeaseExpiresAt:  leaseExpiresAt,
		StartedAt:       nullableTimestamptz(j.StartedAt),
		Result:          nullableJSON(j.Result),
		CompletedAt:     nullableTimestamptz(j.CompletedAt),
		LastError:       nullableString(j.LastError),
		FailedAt:        nullableTimestamptz(j.FailedAt),
	})
	if err != nil {
		return fmt.Errorf("create job %q: %w", j.ID, err)
	}
	return nil
}

// CreateIdempotent atomically creates a job or replays the job bound to key.
func (r *JobRepository) CreateIdempotent(ctx context.Context, j job.Job, key string, fingerprint []byte) (job.Job, bool, error) {
	if key == "" {
		return job.Job{}, false, errors.New("create idempotent job: key must not be empty")
	}
	if len(fingerprint) != 32 {
		return job.Job{}, false, errors.New("create idempotent job: fingerprint must be 32 bytes")
	}
	maxAttempts, err := postgresInteger(j.MaxAttempts)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("create idempotent job %q: max attempts: %w", j.ID, err)
	}
	attemptsStarted, err := postgresInteger(j.AttemptsStarted)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("create idempotent job %q: attempts started: %w", j.ID, err)
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("create idempotent job %q: begin transaction: %w", j.ID, err)
	}
	queries := r.queries.WithTx(tx)
	leaseWorkerID, leaseToken, leaseExpiresAt := nullableLease(j.Lease)
	inserted, err := queries.CreateIdempotentJob(ctx, generated.CreateIdempotentJobParams{
		ID: string(j.ID), TaskType: string(j.TaskType), Payload: append([]byte(nil), j.Payload...),
		State: string(j.State), MaxAttempts: maxAttempts, AttemptsStarted: attemptsStarted,
		CreatedAt: timestamptz(j.CreatedAt), AvailableAt: timestamptz(j.AvailableAt),
		LeaseWorkerID: leaseWorkerID, LeaseToken: leaseToken, LeaseExpiresAt: leaseExpiresAt,
		StartedAt: nullableTimestamptz(j.StartedAt), Result: nullableJSON(j.Result),
		CompletedAt: nullableTimestamptz(j.CompletedAt), LastError: nullableString(j.LastError),
		FailedAt: nullableTimestamptz(j.FailedAt), IdempotencyKey: []byte(key),
		IdempotencyFingerprint: append([]byte(nil), fingerprint...),
	})
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		inserted, err = queries.GetJobByIdempotencyKeyForUpdate(ctx, []byte(key))
	}
	if err != nil {
		return job.Job{}, false, rollbackTransaction(ctx, tx, fmt.Errorf("create idempotent job %q: persist or load replay: %w", j.ID, err))
	}
	if !created && subtle.ConstantTimeCompare(inserted.IdempotencyFingerprint, fingerprint) != 1 {
		return job.Job{}, false, rollbackTransaction(ctx, tx, fmt.Errorf("create idempotent job: %w", ErrIdempotencyConflict))
	}
	result, err := jobFromRow(inserted)
	if err != nil {
		return job.Job{}, false, rollbackTransaction(ctx, tx, fmt.Errorf("create idempotent job %q: decode: %w", j.ID, err))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, false, rollbackTransaction(ctx, tx, fmt.Errorf("create idempotent job %q: commit transaction: %w", j.ID, err))
	}
	return result, created, nil
}

// GetByID returns the complete persisted representation of a job.
func (r *JobRepository) GetByID(ctx context.Context, id job.JobID) (job.Job, error) {
	row, err := r.queries.GetJobByID(ctx, string(id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, fmt.Errorf("get job %q: %w", id, ErrJobNotFound)
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("get job %q: %w", id, err)
	}

	j, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, fmt.Errorf("get job %q: %w", id, err)
	}
	return j, nil
}

// ClaimNext atomically leases the earliest currently eligible job.
func (r *JobRepository) ClaimNext(ctx context.Context, workerID job.WorkerID, token job.LeaseToken, now, expiresAt time.Time) (job.Job, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("claim next job: begin transaction: %w", err)
	}

	queries := r.queries.WithTx(tx)
	row, err := queries.GetNextClaimableJobForUpdate(ctx, timestamptz(now))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job: %w", ErrNoJobAvailable))
	}
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job: select candidate: %w", err))
	}

	claimed, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job: decode candidate: %w", err))
	}
	if err := claimed.Claim(workerID, token, now, expiresAt); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job %q: validate claim: %w", claimed.ID, err))
	}

	leaseWorkerID, leaseToken, leaseExpiresAt := nullableLease(claimed.Lease)
	rowsAffected, err := queries.LeaseJob(ctx, generated.LeaseJobParams{
		ID:             string(claimed.ID),
		State:          string(claimed.State),
		LeaseWorkerID:  leaseWorkerID,
		LeaseToken:     leaseToken,
		LeaseExpiresAt: leaseExpiresAt,
	})
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job %q: persist lease: %w", claimed.ID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job %q: persist lease affected %d rows", claimed.ID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("claim next job %q: commit transaction: %w", claimed.ID, err))
	}
	return claimed, nil
}

// StartExecution atomically authenticates a lease and transitions its job to running.
func (r *JobRepository) StartExecution(ctx context.Context, jobID job.JobID, workerID job.WorkerID, token job.LeaseToken, now time.Time) (job.Job, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("start job %q: begin transaction: %w", jobID, err)
	}

	queries := r.queries.WithTx(tx)
	row, err := queries.GetJobByIDForUpdate(ctx, string(jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: %w", jobID, ErrJobNotFound))
	}
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: load: %w", jobID, err))
	}

	started, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: decode: %w", jobID, err))
	}
	if err := started.Start(workerID, token, now); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: validate: %w", jobID, err))
	}
	attemptsStarted, err := postgresInteger(started.AttemptsStarted)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: attempts started: %w", jobID, err))
	}

	rowsAffected, err := queries.StartJob(ctx, generated.StartJobParams{
		ID:              string(started.ID),
		State:           string(started.State),
		AttemptsStarted: attemptsStarted,
		StartedAt:       nullableTimestamptz(started.StartedAt),
	})
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: persist: %w", jobID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: persist affected %d rows", jobID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("start job %q: commit transaction: %w", jobID, err))
	}
	return started, nil
}

// RenewLease atomically authenticates and extends a running job's lease.
func (r *JobRepository) RenewLease(ctx context.Context, jobID job.JobID, workerID job.WorkerID, token job.LeaseToken, now, newExpiresAt time.Time) (job.Job, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("renew job %q lease: begin transaction: %w", jobID, err)
	}

	queries := r.queries.WithTx(tx)
	row, err := queries.GetJobByIDForUpdate(ctx, string(jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: %w", jobID, ErrJobNotFound))
	}
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: load: %w", jobID, err))
	}

	renewed, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: decode: %w", jobID, err))
	}
	if err := renewed.RenewLease(workerID, token, now, newExpiresAt); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: validate: %w", jobID, err))
	}

	rowsAffected, err := queries.RenewJobLease(ctx, generated.RenewJobLeaseParams{
		ID:             string(renewed.ID),
		LeaseExpiresAt: timestamptz(renewed.Lease.ExpiresAt),
	})
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: persist: %w", jobID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: persist affected %d rows", jobID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("renew job %q lease: commit transaction: %w", jobID, err))
	}
	return renewed, nil
}

// CompleteExecution atomically authenticates and completes a running job.
func (r *JobRepository) CompleteExecution(ctx context.Context, jobID job.JobID, workerID job.WorkerID, token job.LeaseToken, result json.RawMessage, now time.Time) (job.Job, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("complete job %q: begin transaction: %w", jobID, err)
	}

	queries := r.queries.WithTx(tx)
	row, err := queries.GetJobByIDForUpdate(ctx, string(jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: %w", jobID, ErrJobNotFound))
	}
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: load: %w", jobID, err))
	}

	completed, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: decode: %w", jobID, err))
	}
	if err := completed.Complete(workerID, token, now, result); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: validate: %w", jobID, err))
	}

	rowsAffected, err := queries.CompleteJob(ctx, generated.CompleteJobParams{
		ID:          string(completed.ID),
		State:       string(completed.State),
		Result:      nullableJSON(completed.Result),
		CompletedAt: nullableTimestamptz(completed.CompletedAt),
	})
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: persist: %w", jobID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: persist affected %d rows", jobID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("complete job %q: commit transaction: %w", jobID, err))
	}
	return completed, nil
}

// FailExecution atomically authenticates and records a running job's failure.
func (r *JobRepository) FailExecution(ctx context.Context, jobID job.JobID, workerID job.WorkerID, token job.LeaseToken, now time.Time, message string, retryAt *time.Time) (job.Job, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("fail job %q: begin transaction: %w", jobID, err)
	}

	queries := r.queries.WithTx(tx)
	row, err := queries.GetJobByIDForUpdate(ctx, string(jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: %w", jobID, ErrJobNotFound))
	}
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: load: %w", jobID, err))
	}

	failed, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: decode: %w", jobID, err))
	}
	if err := failed.Fail(workerID, token, now, message, retryAt); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: validate: %w", jobID, err))
	}

	rowsAffected, err := queries.FailJob(ctx, generated.FailJobParams{
		ID:          string(failed.ID),
		State:       string(failed.State),
		AvailableAt: timestamptz(failed.AvailableAt),
		LastError:   nullableString(failed.LastError),
		FailedAt:    nullableTimestamptz(failed.FailedAt),
	})
	if err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: persist: %w", jobID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: persist affected %d rows", jobID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackTransaction(ctx, tx, fmt.Errorf("fail job %q: commit transaction: %w", jobID, err))
	}
	return failed, nil
}

// RecoverExpiredLeases atomically recovers a bounded batch of expired leases.
func (r *JobRepository) RecoverExpiredLeases(ctx context.Context, now, retryAt time.Time, batchSize int) ([]job.Job, error) {
	if now.IsZero() {
		return nil, errors.New("recover expired leases: current time must not be zero")
	}
	if retryAt.IsZero() {
		return nil, errors.New("recover expired leases: retry time must not be zero")
	}
	if !retryAt.After(now) {
		return nil, errors.New("recover expired leases: retry time must be after current time")
	}
	if batchSize <= 0 || batchSize > MaxRecoveryBatchSize {
		return nil, fmt.Errorf("recover expired leases: batch size must be between 1 and %d", MaxRecoveryBatchSize)
	}

	postgresBatchSize, err := postgresInteger(batchSize)
	if err != nil {
		return nil, fmt.Errorf("recover expired leases: batch size: %w", err)
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("recover expired leases: begin transaction: %w", err)
	}

	queries := r.queries.WithTx(tx)
	// Locks are acquired in stable order while SKIP LOCKED lets independent
	// recovery processes divide work without waiting for one another.
	rows, err := queries.GetExpiredLeasesForUpdate(ctx, generated.GetExpiredLeasesForUpdateParams{
		Now:       timestamptz(now),
		BatchSize: postgresBatchSize,
	})
	if err != nil {
		return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired leases: select candidates: %w", err))
	}

	recovered := make([]job.Job, 0, len(rows))
	for _, row := range rows {
		candidate, err := jobFromRow(row)
		if err != nil {
			return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired leases: decode candidate: %w", err))
		}
		if err := candidate.RecoverExpiredLease(now, retryAt); err != nil {
			return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired job %q: validate: %w", candidate.ID, err))
		}

		rowsAffected, err := queries.RecoverExpiredJob(ctx, generated.RecoverExpiredJobParams{
			ID:          string(candidate.ID),
			State:       string(candidate.State),
			AvailableAt: timestamptz(candidate.AvailableAt),
			StartedAt:   nullableTimestamptz(candidate.StartedAt),
			LastError:   nullableString(candidate.LastError),
			FailedAt:    nullableTimestamptz(candidate.FailedAt),
		})
		if err != nil {
			return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired job %q: persist: %w", candidate.ID, err))
		}
		if rowsAffected != 1 {
			return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired job %q: persist affected %d rows", candidate.ID, rowsAffected))
		}
		recovered = append(recovered, candidate)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, rollbackTransaction(ctx, tx, fmt.Errorf("recover expired leases: commit transaction: %w", err))
	}
	return recovered, nil
}

func rollbackTransaction(ctx context.Context, tx pgx.Tx, cause error) error {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("%w; rollback transaction: %w", cause, err)
	}
	return cause
}

func jobFromRow(row generated.Job) (job.Job, error) {
	state, err := job.ParseState(row.State)
	if err != nil {
		return job.Job{}, fmt.Errorf("decode state: %w", err)
	}
	if !row.CreatedAt.Valid || !row.AvailableAt.Valid {
		return job.Job{}, errors.New("decode timestamps: required timestamp is null")
	}
	lease, err := restoreLease(row.LeaseWorkerID, row.LeaseToken, row.LeaseExpiresAt)
	if err != nil {
		return job.Job{}, err
	}

	return job.Job{
		ID:              job.JobID(row.ID),
		TaskType:        job.TaskType(row.TaskType),
		Payload:         append(json.RawMessage(nil), row.Payload...),
		State:           state,
		MaxAttempts:     int(row.MaxAttempts),
		AttemptsStarted: int(row.AttemptsStarted),
		CreatedAt:       row.CreatedAt.Time.UTC(),
		AvailableAt:     row.AvailableAt.Time.UTC(),
		Lease:           lease,
		StartedAt:       timeFromTimestamptz(row.StartedAt),
		CompletedAt:     timeFromTimestamptz(row.CompletedAt),
		Result:          append(json.RawMessage(nil), row.Result...),
		LastError:       dereferenceString(row.LastError),
		FailedAt:        timeFromTimestamptz(row.FailedAt),
	}, nil
}

func nullableLease(lease *job.Lease) (*string, *string, pgtype.Timestamptz) {
	if lease == nil {
		return nil, nil, pgtype.Timestamptz{}
	}
	workerID := string(lease.WorkerID)
	token := string(lease.Token)
	return &workerID, &token, timestamptz(lease.ExpiresAt)
}

func restoreLease(workerID, token *string, expiresAt pgtype.Timestamptz) (*job.Lease, error) {
	if workerID == nil && token == nil && !expiresAt.Valid {
		return nil, nil
	}
	// Database constraints should make partial leases impossible. Retaining this
	// check prevents corrupted rows from becoming valid-looking domain objects.
	if workerID == nil || token == nil || !expiresAt.Valid {
		return nil, errors.New("decode lease: incomplete lease columns")
	}
	return &job.Lease{
		WorkerID:  job.WorkerID(*workerID),
		Token:     job.LeaseToken(*token),
		ExpiresAt: expiresAt.Time.UTC(),
	}, nil
}

func postgresInteger(value int) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%d is outside PostgreSQL INTEGER range", value)
	}
	return int32(value), nil
}

func timestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func nullableTimestamptz(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return timestamptz(*value)
}

func timeFromTimestamptz(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	utc := value.Time.UTC()
	return &utc
}

func nullableJSON(value json.RawMessage) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func dereferenceString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

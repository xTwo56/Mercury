// Package postgres persists Mercury domain objects in PostgreSQL.
package postgres

import (
	"context"
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
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job: %w", ErrNoJobAvailable))
	}
	if err != nil {
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job: select candidate: %w", err))
	}

	claimed, err := jobFromRow(row)
	if err != nil {
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job: decode candidate: %w", err))
	}
	if err := claimed.Claim(workerID, token, now, expiresAt); err != nil {
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job %q: validate claim: %w", claimed.ID, err))
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
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job %q: persist lease: %w", claimed.ID, err))
	}
	if rowsAffected != 1 {
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job %q: persist lease affected %d rows", claimed.ID, rowsAffected))
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, rollbackClaim(ctx, tx, fmt.Errorf("claim next job %q: commit transaction: %w", claimed.ID, err))
	}
	return claimed, nil
}

func rollbackClaim(ctx context.Context, tx pgx.Tx, cause error) error {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("%w; rollback claim transaction: %w", cause, err)
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

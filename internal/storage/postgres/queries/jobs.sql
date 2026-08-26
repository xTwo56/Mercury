-- name: CreateJob :exec
INSERT INTO jobs (
    id,
    task_type,
    payload,
    state,
    max_attempts,
    attempts_started,
    created_at,
    available_at,
    lease_worker_id,
    lease_token,
    lease_expires_at,
    started_at,
    result,
    completed_at,
    last_error,
    failed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16
);

-- name: GetJobByID :one
SELECT
    id,
    task_type,
    payload,
    state,
    max_attempts,
    attempts_started,
    created_at,
    available_at,
    lease_worker_id,
    lease_token,
    lease_expires_at,
    started_at,
    result,
    completed_at,
    last_error,
    failed_at,
    idempotency_key,
    idempotency_fingerprint
FROM jobs
WHERE id = $1;

-- name: CreateIdempotentJob :one
INSERT INTO jobs (
    id, task_type, payload, state, max_attempts, attempts_started,
    created_at, available_at, lease_worker_id, lease_token,
    lease_expires_at, started_at, result, completed_at, last_error, failed_at,
    idempotency_key, idempotency_fingerprint
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;

-- name: GetJobByIdempotencyKeyForUpdate :one
SELECT *
FROM jobs
WHERE idempotency_key = $1
FOR UPDATE;

-- name: GetNextClaimableJobForUpdate :one
SELECT
    id,
    task_type,
    payload,
    state,
    max_attempts,
    attempts_started,
    created_at,
    available_at,
    lease_worker_id,
    lease_token,
    lease_expires_at,
    started_at,
    result,
    completed_at,
    last_error,
    failed_at,
    idempotency_key,
    idempotency_fingerprint
FROM jobs
WHERE state IN ('queued', 'retry_scheduled')
  AND available_at <= sqlc.arg(now)
  AND attempts_started < max_attempts
ORDER BY available_at, created_at, id
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: LeaseJob :execrows
UPDATE jobs
SET state = sqlc.arg(state),
    lease_worker_id = sqlc.arg(lease_worker_id),
    lease_token = sqlc.arg(lease_token),
    lease_expires_at = sqlc.arg(lease_expires_at)
WHERE id = sqlc.arg(id);

-- name: FailJob :execrows
UPDATE jobs
SET state = sqlc.arg(state),
    available_at = sqlc.arg(available_at),
    last_error = sqlc.arg(last_error),
    failed_at = sqlc.arg(failed_at),
    started_at = NULL,
    lease_worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL
WHERE id = sqlc.arg(id);

-- name: RenewJobLease :execrows
UPDATE jobs
SET lease_expires_at = sqlc.arg(lease_expires_at)
WHERE id = sqlc.arg(id);

-- name: GetJobByIDForUpdate :one
SELECT
    id,
    task_type,
    payload,
    state,
    max_attempts,
    attempts_started,
    created_at,
    available_at,
    lease_worker_id,
    lease_token,
    lease_expires_at,
    started_at,
    result,
    completed_at,
    last_error,
    failed_at,
    idempotency_key,
    idempotency_fingerprint
FROM jobs
WHERE id = sqlc.arg(id)
FOR UPDATE;

-- name: StartJob :execrows
UPDATE jobs
SET state = sqlc.arg(state),
    attempts_started = sqlc.arg(attempts_started),
    started_at = sqlc.arg(started_at)
WHERE id = sqlc.arg(id);

-- name: CompleteJob :execrows
UPDATE jobs
SET state = sqlc.arg(state),
    result = sqlc.arg(result),
    completed_at = sqlc.arg(completed_at),
    lease_worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL
WHERE id = sqlc.arg(id);

-- name: GetExpiredLeasesForUpdate :many
SELECT
    id,
    task_type,
    payload,
    state,
    max_attempts,
    attempts_started,
    created_at,
    available_at,
    lease_worker_id,
    lease_token,
    lease_expires_at,
    started_at,
    result,
    completed_at,
    last_error,
    failed_at,
    idempotency_key,
    idempotency_fingerprint
FROM jobs
WHERE state IN ('leased', 'running')
  AND lease_expires_at <= sqlc.arg(now)
ORDER BY lease_expires_at, created_at, id
LIMIT sqlc.arg(batch_size)
FOR UPDATE SKIP LOCKED;

-- name: RecoverExpiredJob :execrows
UPDATE jobs
SET state = sqlc.arg(state),
    available_at = sqlc.arg(available_at),
    started_at = sqlc.arg(started_at),
    last_error = sqlc.arg(last_error),
    failed_at = sqlc.arg(failed_at),
    lease_worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL
WHERE id = sqlc.arg(id);

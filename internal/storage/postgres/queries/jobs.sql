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
    failed_at
FROM jobs
WHERE id = $1;

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
    failed_at
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

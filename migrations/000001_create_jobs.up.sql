CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    attempts_started INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    lease_worker_id TEXT,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    result JSONB,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    failed_at TIMESTAMPTZ,

    CONSTRAINT jobs_state_supported CHECK (
        state IN (
            'queued',
            'leased',
            'running',
            'retry_scheduled',
            'succeeded',
            'failed'
        )
    ),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
    CONSTRAINT jobs_attempts_started_valid CHECK (
        attempts_started >= 0 AND attempts_started <= max_attempts
    ),
    CONSTRAINT jobs_lease_fields_complete CHECK (
        (lease_worker_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT jobs_lease_state_valid CHECK (
        lease_worker_id IS NULL OR state IN ('leased', 'running')
    )
);

CREATE INDEX jobs_claimable_available_at_idx
    ON jobs (available_at, id)
    WHERE state IN ('queued', 'retry_scheduled');

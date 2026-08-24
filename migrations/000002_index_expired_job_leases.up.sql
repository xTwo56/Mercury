CREATE INDEX jobs_expired_lease_recovery_idx
    ON jobs (lease_expires_at, created_at, id)
    WHERE state IN ('leased', 'running');

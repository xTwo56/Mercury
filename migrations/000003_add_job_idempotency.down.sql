ALTER TABLE jobs
    DROP CONSTRAINT jobs_idempotency_key_unique,
    DROP CONSTRAINT jobs_idempotency_fingerprint_length,
    DROP CONSTRAINT jobs_idempotency_fields_complete,
    DROP COLUMN idempotency_fingerprint,
    DROP COLUMN idempotency_key;

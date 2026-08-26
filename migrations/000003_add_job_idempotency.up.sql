ALTER TABLE jobs
    ADD COLUMN idempotency_key BYTEA,
    ADD COLUMN idempotency_fingerprint BYTEA,
    ADD CONSTRAINT jobs_idempotency_fields_complete CHECK (
        (idempotency_key IS NULL AND idempotency_fingerprint IS NULL)
        OR
        (idempotency_key IS NOT NULL AND idempotency_fingerprint IS NOT NULL)
    ),
    ADD CONSTRAINT jobs_idempotency_fingerprint_length CHECK (
        idempotency_fingerprint IS NULL OR octet_length(idempotency_fingerprint) = 32
    ),
    ADD CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key);

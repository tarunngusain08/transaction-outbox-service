CREATE TABLE transactions (
    id                  UUID PRIMARY KEY,
    external_reference  VARCHAR(100) NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    currency            VARCHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    type                VARCHAR(10) NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    status              VARCHAR(10) NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')),
    source_account      VARCHAR(64) NOT NULL,
    destination_account VARCHAR(64) NOT NULL,
    channel             VARCHAR(24) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT uk_transactions_external_reference UNIQUE (external_reference)
);

CREATE INDEX idx_transactions_created_at ON transactions (created_at DESC);

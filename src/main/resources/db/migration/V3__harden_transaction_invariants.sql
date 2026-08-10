-- This migration intentionally performs every blocking check before its first
-- data or schema mutation. Run scripts/sql/preflight_v3_transaction_identity.sql
-- first to obtain the complete operator worklist.
DO $$
BEGIN
    IF EXISTS (
        SELECT BTRIM(external_reference)
        FROM transactions
        GROUP BY BTRIM(external_reference)
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: canonical external-reference collisions exist'
            USING HINT =
                'Run scripts/sql/preflight_v3_transaction_identity.sql and reconcile every row manually';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM transactions
        WHERE BTRIM(external_reference)
                  !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$'
    ) THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: an external reference violates the V1 ASCII identity policy';
    END IF;

    IF EXISTS (SELECT 1 FROM transactions WHERE currency <> 'INR') THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: historical non-INR transactions require manual reconciliation';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM transactions
        WHERE source_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
           OR destination_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
    ) THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: an account identifier violates the V1 ASCII identity policy';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM transactions
        WHERE channel NOT IN ('UPI', 'CARD', 'NEFT', 'IMPS', 'RTGS', 'BANK_TRANSFER', 'OTHER')
    ) THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: an unsupported historical payment channel exists';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM transactions
        WHERE created_at < TIMESTAMPTZ '1970-01-01 00:00:00+00'
           OR created_at > CURRENT_TIMESTAMP + INTERVAL '5 minutes'
    ) THEN
        RAISE EXCEPTION
            'V3 aborted before mutation: a historical transaction timestamp is outside the V1 bounds';
    END IF;
END
$$;

CREATE TABLE transaction_reference_canonicalization_audit (
    transaction_id      UUID PRIMARY KEY,
    migration_version   VARCHAR(16) NOT NULL,
    source_system       VARCHAR(32) NOT NULL,
    old_reference       VARCHAR(100) NOT NULL,
    new_reference       VARCHAR(100) NOT NULL,
    canonicalized_at    TIMESTAMPTZ NOT NULL,

    CONSTRAINT chk_reference_audit_changed
        CHECK (old_reference <> new_reference)
);

INSERT INTO transaction_reference_canonicalization_audit (
    transaction_id,
    migration_version,
    source_system,
    old_reference,
    new_reference,
    canonicalized_at
)
SELECT
    id,
    'V3',
    'DIRECT_API',
    external_reference,
    BTRIM(external_reference),
    CURRENT_TIMESTAMP
FROM transactions
WHERE external_reference <> BTRIM(external_reference);

UPDATE transactions AS transaction
SET external_reference = audit.new_reference
FROM transaction_reference_canonicalization_audit AS audit
WHERE transaction.id = audit.transaction_id;

CREATE FUNCTION reject_reference_audit_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'transaction reference canonicalization audit is immutable';
END
$$;

CREATE TRIGGER trg_reference_audit_immutable
    BEFORE INSERT OR UPDATE OR DELETE
    ON transaction_reference_canonicalization_audit
    FOR EACH ROW
    EXECUTE FUNCTION reject_reference_audit_mutation();

ALTER TABLE transactions
    ADD COLUMN source_system VARCHAR(32) NOT NULL DEFAULT 'DIRECT_API',
    ADD COLUMN received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN request_fingerprint CHAR(64),
    ADD COLUMN request_fingerprint_version SMALLINT;

ALTER TABLE transactions
    DROP CONSTRAINT uk_transactions_external_reference;

ALTER TABLE transactions
    ADD CONSTRAINT uk_transactions_source_reference
        UNIQUE (source_system, external_reference);

ALTER TABLE transactions
    ADD CONSTRAINT chk_transactions_source_system
        CHECK (source_system ~ '^[A-Z][A-Z0-9_]{0,31}$') NOT VALID,
    ADD CONSTRAINT chk_transactions_external_reference_canonical
        CHECK (external_reference ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$') NOT VALID,
    ADD CONSTRAINT chk_transactions_accounts_canonical
        CHECK (
            source_account ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
            AND destination_account ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
        ) NOT VALID,
    ADD CONSTRAINT chk_transactions_currency
        CHECK (currency = 'INR') NOT VALID,
    ADD CONSTRAINT chk_transactions_channel
        CHECK (channel IN ('UPI', 'CARD', 'NEFT', 'IMPS', 'RTGS', 'BANK_TRANSFER', 'OTHER'))
        NOT VALID,
    ADD CONSTRAINT chk_transactions_timestamp_contract
        CHECK (
            created_at >= TIMESTAMPTZ '1970-01-01 00:00:00+00'
            AND created_at <= received_at + INTERVAL '5 minutes'
            AND created_at = DATE_TRUNC('microseconds', created_at)
            AND received_at = DATE_TRUNC('microseconds', received_at)
        ) NOT VALID,
    ADD CONSTRAINT chk_transactions_fingerprint
        CHECK (
            (
                request_fingerprint IS NULL
                AND request_fingerprint_version IS NULL
            )
            OR (
                request_fingerprint ~ '^[0-9a-f]{64}$'
                AND request_fingerprint_version = 1
            )
        ) NOT VALID;

ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_source_system;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_external_reference_canonical;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_accounts_canonical;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_currency;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_channel;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_timestamp_contract;
ALTER TABLE transactions VALIDATE CONSTRAINT chk_transactions_fingerprint;

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_publication_state
        CHECK (
            (status = 'PUBLISHED' AND published_at IS NOT NULL)
            OR (status <> 'PUBLISHED' AND published_at IS NULL)
        ) NOT VALID;

ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_publication_state;

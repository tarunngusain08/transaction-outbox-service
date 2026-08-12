ALTER TABLE transactions
    ADD CONSTRAINT chk_transactions_external_reference_canonical
        CHECK (external_reference = BTRIM(external_reference));

ALTER TABLE transactions
    ADD CONSTRAINT chk_transactions_channel
        CHECK (channel IN ('UPI', 'CARD', 'NEFT', 'IMPS', 'RTGS', 'BANK_TRANSFER', 'OTHER'));

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_publication_state
        CHECK (
            (status = 'PUBLISHED' AND published_at IS NOT NULL)
            OR (status <> 'PUBLISHED' AND published_at IS NULL)
        );

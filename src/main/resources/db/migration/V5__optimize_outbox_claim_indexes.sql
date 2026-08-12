DROP INDEX idx_outbox_claimable;

CREATE INDEX idx_outbox_pending_claimable
    ON outbox_events (next_attempt_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_outbox_processing_claimable
    ON outbox_events (claimed_at, created_at)
    WHERE status = 'PROCESSING';

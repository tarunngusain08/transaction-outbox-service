CREATE TABLE outbox_events (
    id               UUID PRIMARY KEY,
    aggregate_type   VARCHAR(50) NOT NULL,
    aggregate_id     UUID NOT NULL,
    event_type       VARCHAR(100) NOT NULL,
    payload          JSONB NOT NULL,
    status           VARCHAR(12) NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    created_at       TIMESTAMPTZ NOT NULL,
    published_at     TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    retry_count      INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error       VARCHAR(1000),

    CONSTRAINT fk_outbox_transaction
        FOREIGN KEY (aggregate_id) REFERENCES transactions (id)
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (next_attempt_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_outbox_aggregate
    ON outbox_events (aggregate_type, aggregate_id);

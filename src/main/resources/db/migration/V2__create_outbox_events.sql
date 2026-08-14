CREATE TABLE outbox_events (
    id               UUID PRIMARY KEY,
    aggregate_id     UUID NOT NULL REFERENCES transactions (id),
    event_type       VARCHAR(100) NOT NULL,
    payload          JSONB NOT NULL,
    status           VARCHAR(12) NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED')),
    created_at       TIMESTAMPTZ NOT NULL,
    published_at     TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    retry_count      INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error       VARCHAR(1000),

    CONSTRAINT chk_outbox_publication_state CHECK (
        (status = 'PUBLISHED' AND published_at IS NOT NULL)
        OR (status = 'PENDING' AND published_at IS NULL)
    )
);

CREATE INDEX idx_outbox_ready
    ON outbox_events (next_attempt_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);

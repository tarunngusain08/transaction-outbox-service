package io.github.tarunngusain08.payments.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    private static final int MAX_ERROR_LENGTH = 1_000;

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 50)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 12)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    @Column(name = "claim_token")
    private UUID claimToken;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    protected OutboxEvent() {
    }

    public static OutboxEvent pending(
            UUID id,
            String aggregateType,
            UUID aggregateId,
            String eventType,
            String payload,
            Instant createdAt
    ) {
        var event = new OutboxEvent();
        event.id = id;
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.status = OutboxStatus.PENDING;
        event.createdAt = createdAt;
        event.nextAttemptAt = createdAt;
        event.retryCount = 0;
        return event;
    }

    public void claim(UUID token, Instant claimedAt) {
        this.status = OutboxStatus.PROCESSING;
        this.claimToken = token;
        this.claimedAt = claimedAt;
    }

    public boolean isClaimedBy(UUID token) {
        return status == OutboxStatus.PROCESSING && token.equals(claimToken);
    }

    public void markPublished(Instant publishedAt) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedAt;
        this.lastError = null;
        clearClaim();
    }

    public void recordFailure(String error, Instant failedAt) {
        if (retryCount < Integer.MAX_VALUE) {
            retryCount++;
        }
        lastError = truncate(error);
        clearClaim();

        long backoffSeconds = Math.min(300, 1L << Math.min(retryCount - 1, 30));
        status = OutboxStatus.PENDING;
        nextAttemptAt = failedAt.plus(Duration.ofSeconds(backoffSeconds));
    }

    private void clearClaim() {
        claimToken = null;
        claimedAt = null;
    }

    private String truncate(String value) {
        if (value == null || value.isBlank()) {
            return "Unknown Kafka publishing error";
        }
        return value.length() <= MAX_ERROR_LENGTH ? value : value.substring(0, MAX_ERROR_LENGTH);
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getLastError() {
        return lastError;
    }

    public UUID getClaimToken() {
        return claimToken;
    }

    public Instant getClaimedAt() {
        return claimedAt;
    }
}

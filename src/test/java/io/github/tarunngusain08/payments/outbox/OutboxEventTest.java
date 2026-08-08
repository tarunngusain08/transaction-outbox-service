package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventTest {

    private static final Instant CREATED_AT = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void schedulesExponentialRetriesThenMovesToFailed() {
        var event = pendingEvent();

        event.recordFailure("broker unavailable", CREATED_AT, 3);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(1));

        event.recordFailure("broker unavailable", CREATED_AT.plusSeconds(1), 3);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(2);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(3));

        event.recordFailure("broker unavailable", CREATED_AT.plusSeconds(3), 3);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(event.getRetryCount()).isEqualTo(3);
        assertThat(event.getLastError()).isEqualTo("broker unavailable");
    }

    @Test
    void marksEventPublishedAndKeepsAuditTimestamp() {
        var event = pendingEvent();
        var publishedAt = CREATED_AT.plusSeconds(2);

        event.markPublished(publishedAt);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(publishedAt);
        assertThat(event.getLastError()).isNull();
    }

    private OutboxEvent pendingEvent() {
        return OutboxEvent.pending(
                UUID.randomUUID(),
                "TRANSACTION",
                UUID.randomUUID(),
                "TRANSACTION_CREATED",
                "{}",
                CREATED_AT
        );
    }
}

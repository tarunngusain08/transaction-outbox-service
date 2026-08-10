package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventTest {

    private static final Instant CREATED_AT = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void schedulesExponentialRetriesWithoutCreatingTerminalFailure() {
        var event = pendingEvent();

        event.recordFailure("broker unavailable", CREATED_AT);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(1));

        event.recordFailure("broker unavailable", CREATED_AT.plusSeconds(1));
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(2);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(3));

        event.recordFailure("broker unavailable", CREATED_AT.plusSeconds(3));
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(3);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(7));
        assertThat(event.getLastError()).isEqualTo("broker unavailable");
    }

    @Test
    void marksEventPublishedAndKeepsAuditTimestamp() {
        var event = pendingEvent();
        var publishedAt = CREATED_AT.plusSeconds(2);
        event.claim(UUID.randomUUID(), CREATED_AT.plusSeconds(1));

        event.markPublished(publishedAt);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(publishedAt);
        assertThat(event.getLastError()).isNull();
        assertThat(event.getClaimToken()).isNull();
        assertThat(event.getClaimedAt()).isNull();
    }

    @Test
    void capsExponentialRetryDelayAtFiveMinutes() {
        var event = pendingEvent();

        for (int attempt = 1; attempt <= 10; attempt++) {
            event.recordFailure("broker unavailable", CREATED_AT);
        }

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(10);
        assertThat(event.getNextAttemptAt()).isEqualTo(CREATED_AT.plusSeconds(300));
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

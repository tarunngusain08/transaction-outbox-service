package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxEventFinalizerTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void finalizesOnlyTheWorkerThatStillOwnsTheClaim() {
        var repository = mock(OutboxEventRepository.class);
        var event = pendingEvent();
        UUID token = UUID.randomUUID();
        event.claim(token, NOW);
        when(repository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));
        var finalizer = new OutboxEventFinalizer(repository, properties());
        var staleClaim = new ClaimedOutboxEvent(
                event.getId(),
                event.getAggregateId(),
                event.getPayload(),
                UUID.randomUUID()
        );

        assertThat(finalizer.markPublished(staleClaim, NOW.plusSeconds(1)))
                .isEqualTo(OutboxDeliveryResult.SKIPPED);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PROCESSING);

        var ownedClaim = new ClaimedOutboxEvent(
                event.getId(),
                event.getAggregateId(),
                event.getPayload(),
                token
        );
        assertThat(finalizer.markPublished(ownedClaim, NOW.plusSeconds(2)))
                .isEqualTo(OutboxDeliveryResult.PUBLISHED);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getClaimToken()).isNull();
        assertThat(event.getClaimedAt()).isNull();
    }

    @Test
    void releasesAClaimWhenSchedulingARetry() {
        var repository = mock(OutboxEventRepository.class);
        var event = pendingEvent();
        UUID token = UUID.randomUUID();
        event.claim(token, NOW);
        when(repository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));
        var finalizer = new OutboxEventFinalizer(repository, properties());
        var claim = new ClaimedOutboxEvent(
                event.getId(),
                event.getAggregateId(),
                event.getPayload(),
                token
        );

        var result = finalizer.recordFailure(claim, "broker unavailable", NOW);

        assertThat(result).isEqualTo(OutboxDeliveryResult.RETRY_SCHEDULED);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getClaimToken()).isNull();
        assertThat(event.getClaimedAt()).isNull();
    }

    private OutboxProperties properties() {
        return new OutboxProperties(
                "payments.transactions.created",
                Duration.ofSeconds(1),
                50,
                8,
                Duration.ofSeconds(10),
                Duration.ofSeconds(30)
        );
    }

    private OutboxEvent pendingEvent() {
        return OutboxEvent.pending(
                UUID.randomUUID(),
                "TRANSACTION",
                UUID.randomUUID(),
                "TRANSACTION_CREATED",
                "{}",
                NOW
        );
    }
}

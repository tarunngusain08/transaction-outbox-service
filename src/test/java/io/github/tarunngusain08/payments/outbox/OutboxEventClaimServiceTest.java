package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxEventClaimServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void claimsSelectedEventsWithAnExpiringOwnershipToken() {
        var repository = mock(OutboxEventRepository.class);
        var event = pendingEvent();
        when(repository.findNextClaimableEventId(NOW, NOW.minusSeconds(30)))
                .thenReturn(Optional.of(event.getId()));
        when(repository.findById(event.getId())).thenReturn(Optional.of(event));
        var service = new OutboxEventClaimService(
                repository,
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        var claimed = service.claimNext();

        assertThat(claimed).isPresent();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PROCESSING);
        assertThat(event.getClaimedAt()).isEqualTo(NOW);
        assertThat(event.getClaimToken()).isEqualTo(claimed.orElseThrow().claimToken());
        assertThat(event.isClaimedBy(claimed.orElseThrow().claimToken())).isTrue();
    }

    @Test
    void reclaimsAnExpiredInterruptedLeaseWithoutConsumingARetry() {
        var repository = mock(OutboxEventRepository.class);
        var event = pendingEvent();
        UUID interruptedToken = UUID.randomUUID();
        event.claim(interruptedToken, NOW.minusSeconds(31));
        when(repository.findNextClaimableEventId(NOW, NOW.minusSeconds(30)))
                .thenReturn(Optional.of(event.getId()));
        when(repository.findById(event.getId())).thenReturn(Optional.of(event));
        var service = new OutboxEventClaimService(
                repository,
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        var reclaimed = service.claimNext().orElseThrow();

        assertThat(reclaimed.claimToken()).isNotEqualTo(interruptedToken);
        assertThat(event.getClaimToken()).isEqualTo(reclaimed.claimToken());
        assertThat(event.getClaimedAt()).isEqualTo(NOW);
        assertThat(event.getRetryCount()).isZero();
    }

    private OutboxProperties properties() {
        return new OutboxProperties(
                "payments.transactions.created",
                Duration.ofSeconds(1),
                50,
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
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

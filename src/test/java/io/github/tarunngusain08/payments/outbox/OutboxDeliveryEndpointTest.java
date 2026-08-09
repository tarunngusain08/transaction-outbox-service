package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxDeliveryEndpointTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void reportsBacklogAndTerminalFailureStateSeparatelyFromApplicationHealth() {
        var repository = mock(OutboxEventRepository.class);
        when(repository.countByStatus(OutboxStatus.PENDING)).thenReturn(3L);
        when(repository.countByStatus(OutboxStatus.PROCESSING)).thenReturn(2L);
        when(repository.countByStatus(OutboxStatus.FAILED)).thenReturn(1L);
        when(repository.findOldestCreatedAtByStatusIn(List.of(
                OutboxStatus.PENDING,
                OutboxStatus.PROCESSING,
                OutboxStatus.FAILED
        ))).thenReturn(NOW.minusSeconds(90));
        var endpoint = new OutboxDeliveryEndpoint(
                repository,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        var snapshot = endpoint.snapshot();

        assertThat(snapshot.pending()).isEqualTo(3);
        assertThat(snapshot.processing()).isEqualTo(2);
        assertThat(snapshot.failed()).isEqualTo(1);
        assertThat(snapshot.oldestUnpublishedAgeSeconds()).isEqualTo(90);
        assertThat(snapshot.checkedAt()).isEqualTo(NOW);
    }
}

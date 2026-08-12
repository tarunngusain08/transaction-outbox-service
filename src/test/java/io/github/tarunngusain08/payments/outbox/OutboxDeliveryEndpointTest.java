package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OutboxDeliveryEndpointTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");

    @Test
    void reportsRecoverableBacklogSeparatelyFromApplicationHealth() {
        var repository = mock(OutboxEventRepository.class);
        var state = mock(OutboxDeliveryState.class);
        when(repository.summarizeDeliveryState()).thenReturn(state);
        when(state.getPending()).thenReturn(3L);
        when(state.getProcessing()).thenReturn(2L);
        when(state.getQuarantined()).thenReturn(1L);
        when(state.getOldestUnpublishedAt()).thenReturn(NOW.minusSeconds(90));
        when(state.getOldestQuarantinedAt()).thenReturn(NOW.minusSeconds(3_600));
        var endpoint = new OutboxDeliveryEndpoint(
                repository,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        var snapshot = endpoint.snapshot();

        assertThat(snapshot.pending()).isEqualTo(3);
        assertThat(snapshot.processing()).isEqualTo(2);
        assertThat(snapshot.quarantined()).isOne();
        assertThat(snapshot.oldestUnpublishedAgeSeconds()).isEqualTo(90);
        assertThat(snapshot.oldestQuarantinedAgeSeconds()).isEqualTo(3_600);
        assertThat(snapshot.checkedAt()).isEqualTo(NOW);
        verify(repository).summarizeDeliveryState();
        verifyNoMoreInteractions(repository);
    }
}

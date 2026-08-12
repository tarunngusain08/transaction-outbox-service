package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventClaimService claimService;

    @Mock
    private OutboxEventDelivery delivery;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(claimService, delivery, properties(3));
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void claimsEachEventImmediatelyBeforeItsIndividualDelivery() {
        var first = claimedEvent();
        var second = claimedEvent();
        when(claimService.claimNext())
                .thenReturn(Optional.of(first))
                .thenReturn(Optional.of(second))
                .thenReturn(Optional.empty());
        when(delivery.deliver(first)).thenReturn(OutboxDeliveryResult.PUBLISHED);
        when(delivery.deliver(second)).thenReturn(OutboxDeliveryResult.PUBLISHED);

        publisher.publishPendingBatch();

        InOrder ordering = inOrder(claimService, delivery);
        ordering.verify(claimService).claimNext();
        ordering.verify(delivery).deliver(first);
        ordering.verify(claimService).claimNext();
        ordering.verify(delivery).deliver(second);
        ordering.verify(claimService).claimNext();
    }

    @Test
    void stopsWithoutPreclaimingAnotherEventAfterInterruption() {
        var first = claimedEvent();
        when(claimService.claimNext()).thenReturn(Optional.of(first));
        when(delivery.deliver(first)).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return OutboxDeliveryResult.INTERRUPTED;
        });

        publisher.publishPendingBatch();

        verify(delivery).deliver(first);
        verify(claimService, times(1)).claimNext();
    }

    @Test
    void neverClaimsMoreThanTheConfiguredBatchLimit() {
        publisher = new OutboxPublisher(claimService, delivery, properties(2));
        var first = claimedEvent();
        var second = claimedEvent();
        when(claimService.claimNext())
                .thenReturn(Optional.of(first))
                .thenReturn(Optional.of(second));
        when(delivery.deliver(first)).thenReturn(OutboxDeliveryResult.PUBLISHED);
        when(delivery.deliver(second)).thenReturn(OutboxDeliveryResult.PUBLISHED);

        publisher.publishPendingBatch();

        verify(claimService, times(2)).claimNext();
        verify(delivery).deliver(first);
        verify(delivery).deliver(second);
    }

    @Test
    void stopsWithoutClaimingMoreWhenProcessingFailsUnexpectedly() {
        var first = claimedEvent();
        when(claimService.claimNext()).thenReturn(Optional.of(first));
        when(delivery.deliver(first)).thenThrow(new IllegalStateException("finalize unavailable"));

        publisher.publishPendingBatch();

        verify(delivery).deliver(first);
        verify(claimService, times(1)).claimNext();
    }

    private OutboxProperties properties(int batchSize) {
        return new OutboxProperties(
                "payments.transactions.created",
                Duration.ofSeconds(1),
                batchSize,
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        );
    }

    private ClaimedOutboxEvent claimedEvent() {
        return new ClaimedOutboxEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "{}",
                UUID.randomUUID()
        );
    }
}

package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.never;
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
        publisher = new OutboxPublisher(claimService, delivery);
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void stopsBatchWhenDeliveryRestoresTheInterruptFlag() {
        var first = claimedEvent();
        var second = claimedEvent();
        when(claimService.claimBatch()).thenReturn(List.of(first, second));
        when(delivery.deliver(first)).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return OutboxDeliveryResult.INTERRUPTED;
        });

        publisher.publishPendingBatch();

        verify(delivery).deliver(first);
        verify(delivery, never()).deliver(second);
    }

    @Test
    void stopsBatchWhenInterruptedDeliveryThrowsDuringFinalization() {
        var first = claimedEvent();
        var second = claimedEvent();
        when(claimService.claimBatch()).thenReturn(List.of(first, second));
        when(delivery.deliver(first)).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("database finalization failed");
        });

        publisher.publishPendingBatch();

        verify(delivery).deliver(first);
        verify(delivery, never()).deliver(second);
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

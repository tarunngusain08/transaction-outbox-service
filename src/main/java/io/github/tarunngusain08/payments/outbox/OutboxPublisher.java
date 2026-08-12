package io.github.tarunngusain08.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventClaimService claimService;
    private final OutboxEventDelivery delivery;

    public OutboxPublisher(
            OutboxEventClaimService claimService,
            OutboxEventDelivery delivery
    ) {
        this.claimService = claimService;
        this.delivery = delivery;
    }

    public void publishPendingBatch() {
        var claimedEvents = claimService.claimBatch();

        for (var event : claimedEvents) {
            try {
                var result = delivery.deliver(event);
                if (result == OutboxDeliveryResult.INTERRUPTED
                        || Thread.currentThread().isInterrupted()) {
                    log.info("Stopping outbox batch after publisher interruption");
                    return;
                }
            } catch (RuntimeException exception) {
                log.error("Unexpected failure while processing outbox event {}", event.eventId(), exception);
                if (Thread.currentThread().isInterrupted()) {
                    log.info("Stopping outbox batch after publisher interruption");
                    return;
                }
            }
        }
    }
}

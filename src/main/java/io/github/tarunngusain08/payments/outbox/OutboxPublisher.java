package io.github.tarunngusain08.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventClaimService claimService;
    private final OutboxEventDelivery delivery;
    private final OutboxProperties properties;

    public OutboxPublisher(
            OutboxEventClaimService claimService,
            OutboxEventDelivery delivery,
            OutboxProperties properties
    ) {
        this.claimService = claimService;
        this.delivery = delivery;
        this.properties = properties;
    }

    public void publishPendingBatch() {
        for (int delivered = 0; delivered < properties.batchSize(); delivered++) {
            if (Thread.currentThread().isInterrupted()) {
                log.info("Stopping outbox batch before claiming more work after interruption");
                return;
            }

            try {
                var claim = claimService.claimNext();
                if (claim.isEmpty()) {
                    return;
                }
                var event = claim.orElseThrow();
                var result = delivery.deliver(event);
                if (result == OutboxDeliveryResult.INTERRUPTED
                        || Thread.currentThread().isInterrupted()) {
                    log.info("Stopping outbox batch after publisher interruption");
                    return;
                }
            } catch (RuntimeException exception) {
                log.error("Unexpected failure while claiming or processing an outbox event", exception);
                return;
            }
        }
    }
}

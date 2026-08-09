package io.github.tarunngusain08.payments.outbox;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class OutboxEventFinalizer {

    private final OutboxEventRepository outboxRepository;
    private final OutboxProperties properties;

    public OutboxEventFinalizer(
            OutboxEventRepository outboxRepository,
            OutboxProperties properties
    ) {
        this.outboxRepository = outboxRepository;
        this.properties = properties;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OutboxDeliveryResult markPublished(ClaimedOutboxEvent claimedEvent, Instant publishedAt) {
        var event = outboxRepository.findByIdForUpdate(claimedEvent.eventId()).orElse(null);
        if (event == null || !event.isClaimedBy(claimedEvent.claimToken())) {
            return OutboxDeliveryResult.SKIPPED;
        }

        event.markPublished(publishedAt);
        return OutboxDeliveryResult.PUBLISHED;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OutboxDeliveryResult recordFailure(
            ClaimedOutboxEvent claimedEvent,
            String error,
            Instant failedAt
    ) {
        var event = outboxRepository.findByIdForUpdate(claimedEvent.eventId()).orElse(null);
        if (event == null || !event.isClaimedBy(claimedEvent.claimToken())) {
            return OutboxDeliveryResult.SKIPPED;
        }

        event.recordFailure(error, failedAt, properties.maxRetries());
        return event.getStatus() == OutboxStatus.FAILED
                ? OutboxDeliveryResult.PERMANENTLY_FAILED
                : OutboxDeliveryResult.RETRY_SCHEDULED;
    }
}

package io.github.tarunngusain08.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxRepository;
    private final OutboxEventDelivery delivery;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxPublisher(
            OutboxEventRepository outboxRepository,
            OutboxEventDelivery delivery,
            OutboxProperties properties,
            Clock clock
    ) {
        this.outboxRepository = outboxRepository;
        this.delivery = delivery;
        this.properties = properties;
        this.clock = clock;
    }

    public void publishPendingBatch() {
        var eventIds = outboxRepository.findReadyEventIds(Instant.now(clock), properties.batchSize());

        for (var eventId : eventIds) {
            try {
                delivery.deliver(eventId);
            } catch (RuntimeException exception) {
                log.error("Unexpected failure while processing outbox event {}", eventId, exception);
            }
        }
    }
}

package io.github.tarunngusain08.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OutboxEventDelivery {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventDelivery.class);

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxEventDelivery(
            OutboxEventRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            OutboxProperties properties,
            Clock clock
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OutboxDeliveryResult deliver(UUID eventId) {
        var event = outboxRepository.findByIdForUpdate(eventId).orElse(null);
        Instant now = Instant.now(clock);

        if (event == null
                || event.getStatus() != OutboxStatus.PENDING
                || event.getNextAttemptAt().isAfter(now)) {
            return OutboxDeliveryResult.SKIPPED;
        }

        try {
            kafkaTemplate.send(
                            properties.topic(),
                            event.getAggregateId().toString(),
                            event.getPayload()
                    )
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);

            event.markPublished(Instant.now(clock));
            log.info("Published outbox event {} for transaction {}", event.getId(), event.getAggregateId());
            return OutboxDeliveryResult.PUBLISHED;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return recordFailure(event, exception);
        } catch (ExecutionException | TimeoutException exception) {
            return recordFailure(event, exception);
        } catch (RuntimeException exception) {
            return recordFailure(event, exception);
        }
    }

    private OutboxDeliveryResult recordFailure(OutboxEvent event, Exception exception) {
        event.recordFailure(rootMessage(exception), Instant.now(clock), properties.maxRetries());

        if (event.getStatus() == OutboxStatus.FAILED) {
            log.error(
                    "Outbox event {} exhausted {} Kafka publishing attempts",
                    event.getId(),
                    event.getRetryCount(),
                    exception
            );
            return OutboxDeliveryResult.PERMANENTLY_FAILED;
        }

        log.warn(
                "Kafka publishing failed for outbox event {}; retry {} scheduled at {}",
                event.getId(),
                event.getRetryCount(),
                event.getNextAttemptAt()
        );
        return OutboxDeliveryResult.RETRY_SCHEDULED;
    }

    private String rootMessage(Exception exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }
}

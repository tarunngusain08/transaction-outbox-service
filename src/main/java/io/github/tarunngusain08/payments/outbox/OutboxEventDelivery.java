package io.github.tarunngusain08.payments.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OutboxEventDelivery {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventDelivery.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxEventFinalizer finalizer;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxEventDelivery(
            KafkaTemplate<String, String> kafkaTemplate,
            OutboxEventFinalizer finalizer,
            OutboxProperties properties,
            Clock clock
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.finalizer = finalizer;
        this.properties = properties;
        this.clock = clock;
    }

    public OutboxDeliveryResult deliver(ClaimedOutboxEvent event) {
        try {
            kafkaTemplate.send(
                            properties.topic(),
                            event.aggregateId().toString(),
                            event.payload()
                    )
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);

            var result = finalizer.markPublished(event, Instant.now(clock));
            if (result == OutboxDeliveryResult.PUBLISHED) {
                log.info(
                        "Published outbox event {} for transaction {}",
                        event.eventId(),
                        event.aggregateId()
                );
            } else {
                log.warn("Ignored stale publication acknowledgement for outbox event {}", event.eventId());
            }
            return result;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            recordFailure(event, exception);
            return OutboxDeliveryResult.INTERRUPTED;
        } catch (ExecutionException | TimeoutException exception) {
            return recordFailure(event, exception);
        } catch (RuntimeException exception) {
            return recordFailure(event, exception);
        }
    }

    private OutboxDeliveryResult recordFailure(ClaimedOutboxEvent event, Exception exception) {
        var result = finalizer.recordFailure(event, rootMessage(exception), Instant.now(clock));

        if (result == OutboxDeliveryResult.PERMANENTLY_FAILED) {
            log.error(
                    "Outbox event {} exhausted Kafka publishing attempts",
                    event.eventId(),
                    exception
            );
        } else if (result == OutboxDeliveryResult.RETRY_SCHEDULED) {
            log.warn("Kafka publishing failed for outbox event {}; retry scheduled", event.eventId());
        } else {
            log.warn("Ignored stale failure result for outbox event {}", event.eventId());
        }

        return result;
    }

    private String rootMessage(Exception exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }
}

package io.github.tarunngusain08.payments.outbox.delivery;

import io.github.tarunngusain08.payments.outbox.domain.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.domain.OutboxStatus;
import io.github.tarunngusain08.payments.outbox.repository.OutboxEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 50;

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final String topic;
    private final Duration publishTimeout;

    public OutboxPublisher(
            OutboxEventRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            Clock clock,
            @Value("${payments.outbox.topic:payments.transactions.created}") String topic,
            @Value("${payments.outbox.publish-timeout:10s}") Duration publishTimeout
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.topic = topic;
        this.publishTimeout = publishTimeout;
    }

    @Scheduled(fixedDelayString = "${payments.outbox.poll-delay:1s}")
    @Transactional
    public void publishPendingEvents() {
        var events = outboxRepository.findReady(
                OutboxStatus.PENDING,
                Instant.now(clock),
                PageRequest.of(0, BATCH_SIZE)
        );

        for (var event : events) {
            if (!publish(event)) {
                return;
            }
        }
    }

    private boolean publish(OutboxEvent event) {
        try {
            kafkaTemplate.send(topic, event.getAggregateId().toString(), event.getPayload())
                    .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
            event.markPublished(Instant.now(clock));
            log.info("Published outbox event {}", event.getId());
            return true;
        } catch (InterruptedException exception) {
            event.recordFailure(message(exception), Instant.now(clock));
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            event.recordFailure(message(exception), Instant.now(clock));
            log.warn("Kafka publishing failed for outbox event {}; retry scheduled", event.getId());
            return true;
        }
    }

    private String message(Exception exception) {
        Throwable cause = exception.getCause() == null ? exception : exception.getCause();
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }
}

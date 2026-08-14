package io.github.tarunngusain08.payments.outbox.delivery;

import io.github.tarunngusain08.payments.outbox.domain.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.domain.OutboxStatus;
import io.github.tarunngusain08.payments.outbox.repository.OutboxEventRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");
    private static final String TOPIC = "payments.transactions.created";

    @Mock
    private OutboxEventRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(
                outboxRepository,
                kafkaTemplate,
                Clock.fixed(NOW, ZoneOffset.UTC),
                TOPIC,
                Duration.ofSeconds(1)
        );
    }

    @Test
    void publishesReadyEventsAndMarksThemPublished() {
        OutboxEvent event = event();
        when(outboxRepository.findReady(eq(OutboxStatus.PENDING), eq(NOW), any(Pageable.class)))
                .thenReturn(List.of(event));
        when(kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(NOW);
        assertThat(event.getRetryCount()).isZero();
    }

    @Test
    void leavesFailedEventsPendingForRetry() {
        OutboxEvent event = event();
        var failed = new CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>();
        failed.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        when(outboxRepository.findReady(eq(OutboxStatus.PENDING), eq(NOW), any(Pageable.class)))
                .thenReturn(List.of(event));
        when(kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload()))
                .thenReturn(failed);

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getLastError()).isEqualTo("Kafka unavailable");
        assertThat(event.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
    }

    private OutboxEvent event() {
        return OutboxEvent.pending(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "TRANSACTION_CREATED",
                "{\"eventType\":\"TRANSACTION_CREATED\"}",
                NOW
        );
    }
}

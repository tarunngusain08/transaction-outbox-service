package io.github.tarunngusain08.payments.outbox;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEventDeliveryTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");
    private static final String TOPIC = "payments.transactions.created";

    @Mock
    private OutboxEventRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxEventDelivery delivery;

    @BeforeEach
    void setUp() {
        var properties = new OutboxProperties(
                TOPIC,
                Duration.ofSeconds(1),
                50,
                3,
                Duration.ofSeconds(2)
        );
        delivery = new OutboxEventDelivery(
                outboxRepository,
                kafkaTemplate,
                properties,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    @Test
    void marksEventPublishedOnlyAfterKafkaAcknowledgement() {
        var event = pendingEvent();
        when(outboxRepository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));
        when(kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var result = delivery.deliver(event.getId());

        assertThat(result).isEqualTo(OutboxDeliveryResult.PUBLISHED);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(NOW);
    }

    @Test
    void leavesEventPendingAndSchedulesRetryWhenKafkaFails() {
        var event = pendingEvent();
        var failedSend = new CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>();
        failedSend.completeExceptionally(new KafkaException("broker unavailable"));
        when(outboxRepository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));
        when(kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload()))
                .thenReturn(failedSend);

        var result = delivery.deliver(event.getId());

        assertThat(result).isEqualTo(OutboxDeliveryResult.RETRY_SCHEDULED);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
        assertThat(event.getLastError()).isEqualTo("broker unavailable");
    }

    @Test
    void schedulesRetryWhenProducerFailsBeforeReturningFuture() {
        var event = pendingEvent();
        when(outboxRepository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));
        when(kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload()))
                .thenThrow(new KafkaException("producer unavailable"));

        var result = delivery.deliver(event.getId());

        assertThat(result).isEqualTo(OutboxDeliveryResult.RETRY_SCHEDULED);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getLastError()).isEqualTo("producer unavailable");
    }

    @Test
    void skipsAlreadyPublishedEvent() {
        var event = pendingEvent();
        event.markPublished(NOW);
        when(outboxRepository.findByIdForUpdate(event.getId())).thenReturn(Optional.of(event));

        assertThat(delivery.deliver(event.getId())).isEqualTo(OutboxDeliveryResult.SKIPPED);

        verify(kafkaTemplate, never()).send(TOPIC, event.getAggregateId().toString(), event.getPayload());
    }

    private OutboxEvent pendingEvent() {
        return OutboxEvent.pending(
                UUID.randomUUID(),
                "TRANSACTION",
                UUID.randomUUID(),
                "TRANSACTION_CREATED",
                "{\"eventType\":\"TRANSACTION_CREATED\"}",
                NOW
        );
    }
}

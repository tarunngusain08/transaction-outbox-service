package io.github.tarunngusain08.payments.outbox;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEventDeliveryTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");
    private static final String TOPIC = "payments.transactions.created";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private OutboxEventFinalizer finalizer;

    private OutboxEventDelivery delivery;

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @BeforeEach
    void setUp() {
        var properties = new OutboxProperties(
                TOPIC,
                Duration.ofSeconds(1),
                50,
                Duration.ofSeconds(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(10),
                Duration.ofSeconds(1)
        );
        delivery = new OutboxEventDelivery(
                kafkaTemplate,
                finalizer,
                properties,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    @Test
    void kafkaDeliveryMethodDoesNotOpenADatabaseTransaction() throws NoSuchMethodException {
        var method = OutboxEventDelivery.class.getMethod("deliver", ClaimedOutboxEvent.class);

        assertThat(method.getAnnotation(Transactional.class)).isNull();
    }

    @Test
    void marksEventPublishedOnlyAfterKafkaAcknowledgement() {
        var event = claimedEvent();
        when(kafkaTemplate.send(TOPIC, event.aggregateId().toString(), event.payload()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(finalizer.markPublished(event, NOW)).thenReturn(OutboxDeliveryResult.PUBLISHED);

        var result = delivery.deliver(event);

        assertThat(result).isEqualTo(OutboxDeliveryResult.PUBLISHED);
        verify(finalizer).markPublished(event, NOW);
    }

    @Test
    void schedulesRetryWhenKafkaAcknowledgementFails() {
        var event = claimedEvent();
        var failedSend = new CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>();
        failedSend.completeExceptionally(new KafkaException("broker unavailable"));
        when(kafkaTemplate.send(TOPIC, event.aggregateId().toString(), event.payload()))
                .thenReturn(failedSend);
        when(finalizer.recordFailure(event, "broker unavailable", NOW))
                .thenReturn(OutboxDeliveryResult.RETRY_SCHEDULED);

        var result = delivery.deliver(event);

        assertThat(result).isEqualTo(OutboxDeliveryResult.RETRY_SCHEDULED);
        verify(finalizer).recordFailure(event, "broker unavailable", NOW);
    }

    @Test
    void schedulesRetryWhenProducerFailsBeforeReturningFuture() {
        var event = claimedEvent();
        when(kafkaTemplate.send(TOPIC, event.aggregateId().toString(), event.payload()))
                .thenThrow(new KafkaException("producer unavailable"));
        when(finalizer.recordFailure(event, "producer unavailable", NOW))
                .thenReturn(OutboxDeliveryResult.RETRY_SCHEDULED);

        var result = delivery.deliver(event);

        assertThat(result).isEqualTo(OutboxDeliveryResult.RETRY_SCHEDULED);
        verify(finalizer).recordFailure(event, "producer unavailable", NOW);
    }

    @Test
    void reportsInterruptionAndKeepsTheThreadInterrupted() {
        var event = claimedEvent();
        when(kafkaTemplate.send(TOPIC, event.aggregateId().toString(), event.payload()))
                .thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        var result = delivery.deliver(event);

        assertThat(result).isEqualTo(OutboxDeliveryResult.INTERRUPTED);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verifyNoInteractions(finalizer);
    }

    private ClaimedOutboxEvent claimedEvent() {
        return new ClaimedOutboxEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "{\"eventType\":\"TRANSACTION_CREATED\"}",
                UUID.randomUUID()
        );
    }
}

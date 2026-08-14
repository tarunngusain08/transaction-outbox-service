package io.github.tarunngusain08.payments.transaction.application;

import io.github.tarunngusain08.payments.outbox.domain.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.domain.OutboxStatus;
import io.github.tarunngusain08.payments.outbox.repository.OutboxEventRepository;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.transaction.domain.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.domain.PaymentTransaction;
import io.github.tarunngusain08.payments.transaction.domain.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.domain.TransactionType;
import io.github.tarunngusain08.payments.transaction.event.TransactionCreatedEvent;
import io.github.tarunngusain08.payments.transaction.repository.PaymentTransactionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");

    @Mock
    private PaymentTransactionRepository transactionRepository;

    @Mock
    private OutboxEventRepository outboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    private TransactionService service;

    @BeforeEach
    void setUp() {
        service = new TransactionService(
                transactionRepository,
                outboxRepository,
                objectMapper,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    @Test
    void storesTheTransactionAndOutboxEventTogether() throws Exception {
        when(objectMapper.writeValueAsString(any(TransactionCreatedEvent.class)))
                .thenReturn("{\"eventType\":\"TRANSACTION_CREATED\"}");

        var response = service.create(request(Map.of("orderId", 42)));

        var transactionCaptor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(transactionRepository).saveAndFlush(transactionCaptor.capture());
        PaymentTransaction stored = transactionCaptor.getValue();
        assertThat(stored.getId()).isEqualTo(response.transactionId());
        assertThat(stored.getExternalReference()).isEqualTo("TXN-1001");
        assertThat(stored.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(stored.getCreatedAt()).isEqualTo(NOW);
        assertThat(stored.getMetadata()).containsEntry("orderId", 42);

        var eventCaptor = ArgumentCaptor.forClass(TransactionCreatedEvent.class);
        verify(objectMapper).writeValueAsString(eventCaptor.capture());
        assertThat(eventCaptor.getValue().transaction()).isEqualTo(response);

        var outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(outboxCaptor.capture());
        OutboxEvent outbox = outboxCaptor.getValue();
        assertThat(outbox.getId()).isEqualTo(eventCaptor.getValue().eventId());
        assertThat(outbox.getAggregateId()).isEqualTo(response.transactionId());
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getPayload()).contains("TRANSACTION_CREATED");
    }

    @Test
    void defaultsMissingCreatedAtAndMetadata() throws Exception {
        when(objectMapper.writeValueAsString(any(TransactionCreatedEvent.class))).thenReturn("{}");

        var response = service.create(request(null));

        assertThat(response.createdAt()).isEqualTo(NOW);
        assertThat(response.metadata()).isEmpty();
    }

    private CreateTransactionRequest request(Map<String, Object> metadata) {
        return new CreateTransactionRequest(
                "TXN-1001",
                150_000,
                "INR",
                TransactionType.DEBIT,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                null,
                metadata
        );
    }
}

package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.outbox.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
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
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");
    private static final UUID TRANSACTION_ID = UUID.fromString("8e41fc4b-0c2b-42a7-a762-41f7c85e15c8");

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
    void savesTransactionAndPendingOutboxEvent() throws Exception {
        when(objectMapper.writeValueAsString(any(TransactionCreatedEvent.class)))
                .thenReturn("{\"eventType\":\"TRANSACTION_CREATED\"}");

        var response = service.create(request());

        var transactionCaptor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(transactionRepository).save(transactionCaptor.capture());
        var savedTransaction = transactionCaptor.getValue();
        assertThat(savedTransaction.getId()).isEqualTo(TRANSACTION_ID);
        assertThat(savedTransaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(savedTransaction.getCreatedAt()).isEqualTo(NOW);

        var eventCaptor = ArgumentCaptor.forClass(TransactionCreatedEvent.class);
        verify(objectMapper).writeValueAsString(eventCaptor.capture());
        assertThat(eventCaptor.getValue().eventType()).isEqualTo(TransactionCreatedEvent.EVENT_TYPE);
        assertThat(eventCaptor.getValue().transaction()).isEqualTo(response);

        var outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(outboxCaptor.capture());
        var savedOutboxEvent = outboxCaptor.getValue();
        assertThat(savedOutboxEvent.getAggregateId()).isEqualTo(TRANSACTION_ID);
        assertThat(savedOutboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(savedOutboxEvent.getPayload()).contains("TRANSACTION_CREATED");

        assertThat(response.amount()).isEqualTo(150_000L);
        assertThat(response.metadata()).containsEntry("orderId", "ORDER-99");
    }

    @Test
    void rejectsDuplicateExternalReferenceBeforeWriting() {
        when(transactionRepository.existsByExternalReference("TXN-88213-ABC")).thenReturn(true);

        assertThatThrownBy(() -> service.create(request()))
                .isInstanceOf(DuplicateTransactionException.class)
                .hasMessageContaining("TXN-88213-ABC");

        verify(transactionRepository, never()).save(any());
        verifyNoInteractions(outboxRepository, objectMapper);
    }

    @Test
    void reportsMissingTransaction() {
        when(transactionRepository.findById(TRANSACTION_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.findById(TRANSACTION_ID))
                .isInstanceOf(TransactionNotFoundException.class)
                .hasMessageContaining(TRANSACTION_ID.toString());
    }

    private CreateTransactionRequest request() {
        return new CreateTransactionRequest(
                TRANSACTION_ID,
                "TXN-88213-ABC",
                150_000L,
                "INR",
                TransactionType.DEBIT,
                null,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                null,
                Map.of("orderId", "ORDER-99")
        );
    }
}

package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.outbox.OutboxEvent;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.validation.MetadataCanonicalizer;
import jakarta.validation.Validation;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-08T10:15:30Z");
    private static final UUID TRANSACTION_ID =
            UUID.fromString("8e41fc4b-0c2b-42a7-a762-41f7c85e15c8");

    @Mock
    private PaymentTransactionRepository transactionRepository;

    @Mock
    private PaymentTransactionInserter transactionInserter;

    @Mock
    private OutboxEventRepository outboxRepository;

    @Mock
    private TransactionEventSerializer eventSerializer;

    private TransactionService service;
    private TransactionRequestFingerprint fingerprint;

    @BeforeEach
    void setUp() {
        var validator = Validation.buildDefaultValidatorFactory().getValidator();
        fingerprint = new TransactionRequestFingerprint(new ObjectMapper());
        service = new TransactionService(
                transactionRepository,
                transactionInserter,
                outboxRepository,
                eventSerializer,
                new MetadataCanonicalizer(new ObjectMapper()),
                fingerprint,
                validator,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    @Test
    void savesServerOwnedTransactionAndPendingOutboxEvent() {
        when(transactionInserter.insertIfAbsent(any(PaymentTransaction.class))).thenReturn(true);
        when(eventSerializer.serialize(any(TransactionCreatedEvent.class)))
                .thenReturn("{\"eventType\":\"TRANSACTION_CREATED\"}");

        var result = service.create(request());
        var response = result.transaction();

        assertThat(result.created()).isTrue();

        var transactionCaptor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(transactionInserter).insertIfAbsent(transactionCaptor.capture());
        var savedTransaction = transactionCaptor.getValue();
        assertThat(savedTransaction.getId()).isNotNull();
        assertThat(savedTransaction.getSourceSystem()).isEqualTo("DIRECT_API");
        assertThat(savedTransaction.getStatus()).isEqualTo(TransactionStatus.PENDING);
        assertThat(savedTransaction.getCreatedAt()).isEqualTo(NOW);
        assertThat(savedTransaction.getReceivedAt()).isEqualTo(NOW);
        assertThat(savedTransaction.getRequestFingerprint()).hasSize(64);
        assertThat(savedTransaction.getRequestFingerprintVersion()).isEqualTo((short) 1);

        var eventCaptor = ArgumentCaptor.forClass(TransactionCreatedEvent.class);
        verify(eventSerializer).serialize(eventCaptor.capture());
        assertThat(eventCaptor.getValue().schemaVersion()).isEqualTo(2);
        assertThat(eventCaptor.getValue().producer()).isEqualTo("transaction-outbox-service");
        assertThat(eventCaptor.getValue().eventType()).isEqualTo(TransactionCreatedEvent.EVENT_TYPE);
        assertThat(eventCaptor.getValue().occurredAt()).isEqualTo(NOW);
        assertThat(eventCaptor.getValue().transaction()).isEqualTo(response);

        var outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(outboxCaptor.capture());
        var savedOutboxEvent = outboxCaptor.getValue();
        assertThat(savedOutboxEvent.getAggregateId()).isEqualTo(savedTransaction.getId());
        assertThat(savedOutboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(savedOutboxEvent.getPayload()).contains("TRANSACTION_CREATED");

        assertThat(response.amount()).isEqualTo(150_000L);
        assertThat(response.metadata()).containsEntry("orderId", "ORDER-99");
    }

    @Test
    void rejectsConflictingScopedReferenceBeforeWriting() {
        when(transactionInserter.insertIfAbsent(any(PaymentTransaction.class))).thenReturn(false);
        when(transactionRepository.findBySourceSystemAndExternalReference(
                "DIRECT_API",
                "TXN-88213-ABC"
        )).thenReturn(Optional.of(existingTransaction(
                999L,
                TransactionType.CREDIT,
                "0".repeat(64)
        )));

        assertThatThrownBy(() -> service.create(request()))
                .isInstanceOf(DuplicateTransactionException.class)
                .hasMessageContaining("TXN-88213-ABC");

        verifyNoInteractions(outboxRepository, eventSerializer);
    }

    @Test
    void returnsOriginalTransactionForAnIdenticalReplay() {
        when(transactionInserter.insertIfAbsent(any(PaymentTransaction.class))).thenReturn(false);
        var existing = existingTransaction(
                150_000L,
                TransactionType.DEBIT,
                fingerprint.calculate(request(), null, Map.of("orderId", "ORDER-99"))
        );
        when(transactionRepository.findBySourceSystemAndExternalReference(
                "DIRECT_API",
                "TXN-88213-ABC"
        )).thenReturn(Optional.of(existing));

        var result = service.create(request());

        assertThat(result.created()).isFalse();
        assertThat(result.transaction().transactionId()).isEqualTo(TRANSACTION_ID);
        verifyNoInteractions(outboxRepository, eventSerializer);
    }

    @Test
    void neverTreatsAHistoricalRowWithoutAFingerprintAsAnIdenticalReplay() {
        when(transactionInserter.insertIfAbsent(any(PaymentTransaction.class))).thenReturn(false);
        when(transactionRepository.findBySourceSystemAndExternalReference(
                "DIRECT_API",
                "TXN-88213-ABC"
        )).thenReturn(Optional.of(existingTransaction(150_000L, TransactionType.DEBIT, null)));

        assertThatThrownBy(() -> service.create(request()))
                .isInstanceOf(DuplicateTransactionException.class)
                .hasMessageContaining("DIRECT_API")
                .hasMessageContaining("TXN-88213-ABC");

        verifyNoInteractions(outboxRepository, eventSerializer);
    }

    @Test
    void canonicalizesTimestampAndIntegerMetadataBeforePersistence() {
        when(transactionInserter.insertIfAbsent(any(PaymentTransaction.class))).thenReturn(true);
        when(eventSerializer.serialize(any(TransactionCreatedEvent.class))).thenReturn("{}");
        var preciseRequest = new CreateTransactionRequest(
                "DIRECT_API",
                "TXN-PRECISION",
                1L,
                "INR",
                TransactionType.DEBIT,
                "SOURCE",
                "DESTINATION",
                PaymentChannel.UPI,
                Instant.parse("2026-08-08T10:15:29.123456789Z"),
                Map.of("attempt", 2)
        );

        service.create(preciseRequest);

        var transactionCaptor = ArgumentCaptor.forClass(PaymentTransaction.class);
        verify(transactionInserter).insertIfAbsent(transactionCaptor.capture());
        assertThat(transactionCaptor.getValue().getCreatedAt())
                .isEqualTo(Instant.parse("2026-08-08T10:15:29.123456Z"));
        assertThat(transactionCaptor.getValue().getMetadata())
                .containsEntry("attempt", 2L);
    }

    @Test
    void validatesTheDomainBeforeLookingUpAnExistingKey() {
        var invalid = new CreateTransactionRequest(
                "DIRECT_API",
                "TXN-88213-ABC",
                150_000L,
                "USD",
                TransactionType.DEBIT,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                null,
                Map.of()
        );

        assertThatThrownBy(() -> service.create(invalid))
                .isInstanceOf(InvalidTransactionException.class)
                .hasMessageContaining("currency");
        verifyNoInteractions(transactionRepository, transactionInserter, outboxRepository);
    }

    @Test
    void reportsMissingTransaction() {
        when(transactionRepository.findById(TRANSACTION_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.findById(TRANSACTION_ID))
                .isInstanceOf(TransactionNotFoundException.class)
                .hasMessageContaining(TRANSACTION_ID.toString());
    }

    private PaymentTransaction existingTransaction(
            long amount,
            TransactionType type,
            String requestFingerprint
    ) {
        return new PaymentTransaction(
                TRANSACTION_ID,
                "DIRECT_API",
                "TXN-88213-ABC",
                amount,
                "INR",
                type,
                TransactionStatus.PENDING,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                NOW,
                NOW,
                Map.of("orderId", "ORDER-99"),
                requestFingerprint,
                TransactionRequestFingerprint.VERSION
        );
    }

    private CreateTransactionRequest request() {
        return new CreateTransactionRequest(
                "DIRECT_API",
                "TXN-88213-ABC",
                150_000L,
                "INR",
                TransactionType.DEBIT,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                null,
                Map.of("orderId", "ORDER-99")
        );
    }
}

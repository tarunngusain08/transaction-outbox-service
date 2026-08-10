package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionEventSerializerTest {

    @Test
    void serializesTheVersionedEventEnvelope() throws Exception {
        var event = new TransactionCreatedEvent(
                1,
                "transaction-outbox-service",
                UUID.randomUUID(),
                "TRANSACTION_CREATED",
                Instant.parse("2026-08-08T10:15:30Z"),
                new TransactionResponse(
                        UUID.randomUUID(),
                        "DIRECT_API",
                        "SOURCE-123",
                        100L,
                        "INR",
                        TransactionType.DEBIT,
                        TransactionStatus.PENDING,
                        "payer",
                        "payee",
                        PaymentChannel.UPI,
                        Instant.parse("2026-08-08T10:15:30Z"),
                        Instant.parse("2026-08-08T10:15:30Z"),
                        Map.of()
                )
        );
        var objectMapper = new ObjectMapper();

        var serialized = new TransactionEventSerializer(objectMapper).serialize(event);
        var json = objectMapper.readTree(serialized);

        assertThat(json.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(json.path("producer").asString()).isEqualTo("transaction-outbox-service");
        assertThat(json.path("eventId").asString()).isEqualTo(event.eventId().toString());
    }
}

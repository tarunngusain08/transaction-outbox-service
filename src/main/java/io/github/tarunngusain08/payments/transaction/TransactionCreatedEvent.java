package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;

import java.time.Instant;
import java.util.UUID;

public record TransactionCreatedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        TransactionResponse transaction
) {
    public static final String EVENT_TYPE = "TRANSACTION_CREATED";
}

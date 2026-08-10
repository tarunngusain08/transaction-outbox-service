package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.PaymentTransaction;
import io.github.tarunngusain08.payments.transaction.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.TransactionType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TransactionResponse(
        UUID transactionId,
        String sourceSystem,
        String externalReference,
        long amount,
        String currency,
        TransactionType type,
        TransactionStatus status,
        String sourceAccount,
        String destinationAccount,
        PaymentChannel channel,
        Instant createdAt,
        Instant receivedAt,
        Map<String, Object> metadata
) {
    public static TransactionResponse from(PaymentTransaction transaction) {
        return new TransactionResponse(
                transaction.getId(),
                transaction.getSourceSystem(),
                transaction.getExternalReference(),
                transaction.getAmountMinor(),
                transaction.getCurrency(),
                transaction.getType(),
                transaction.getStatus(),
                transaction.getSourceAccount(),
                transaction.getDestinationAccount(),
                transaction.getChannel(),
                transaction.getCreatedAt(),
                transaction.getReceivedAt(),
                transaction.getMetadata()
        );
    }
}

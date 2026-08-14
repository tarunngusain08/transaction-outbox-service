package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.transaction.domain.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.domain.PaymentTransaction;
import io.github.tarunngusain08.payments.transaction.domain.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.domain.TransactionType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TransactionResponse(
        UUID transactionId,
        String externalReference,
        long amount,
        String currency,
        TransactionType type,
        TransactionStatus status,
        String sourceAccount,
        String destinationAccount,
        PaymentChannel channel,
        Instant createdAt,
        Map<String, Object> metadata
) {
    public static TransactionResponse from(PaymentTransaction transaction) {
        return new TransactionResponse(
                transaction.getId(),
                transaction.getExternalReference(),
                transaction.getAmountMinor(),
                transaction.getCurrency(),
                transaction.getType(),
                transaction.getStatus(),
                transaction.getSourceAccount(),
                transaction.getDestinationAccount(),
                transaction.getChannel(),
                transaction.getCreatedAt(),
                transaction.getMetadata()
        );
    }
}

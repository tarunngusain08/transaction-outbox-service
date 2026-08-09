package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.validation.SupportedCurrency;
import io.github.tarunngusain08.payments.validation.ValidMetadata;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record CreateTransactionRequest(
        UUID transactionId,

        @NotBlank
        @Size(max = 100)
        String externalReference,

        @Positive
        long amount,

        @NotBlank
        @Pattern(regexp = "^[A-Z]{3}$", message = "must be a three-letter uppercase ISO currency code")
        @SupportedCurrency
        String currency,

        @NotNull
        TransactionType type,

        TransactionStatus status,

        @NotBlank
        @Size(max = 64)
        String sourceAccount,

        @NotBlank
        @Size(max = 64)
        String destinationAccount,

        @NotNull
        PaymentChannel channel,

        Instant createdAt,

        @ValidMetadata
        Map<String, Object> metadata
) {
}

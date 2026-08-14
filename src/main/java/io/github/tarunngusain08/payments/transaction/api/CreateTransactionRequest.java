package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.transaction.domain.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.domain.TransactionType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

public record CreateTransactionRequest(
        @NotBlank
        @Size(max = 100)
        String externalReference,

        @Positive
        long amount,

        @NotBlank
        @Pattern(regexp = "^[A-Z]{3}$", message = "must be a three-letter uppercase currency code")
        String currency,

        @NotNull
        TransactionType type,

        @NotBlank
        @Size(max = 64)
        String sourceAccount,

        @NotBlank
        @Size(max = 64)
        String destinationAccount,

        @NotNull
        PaymentChannel channel,

        Instant createdAt,

        Map<String, Object> metadata
) {
}

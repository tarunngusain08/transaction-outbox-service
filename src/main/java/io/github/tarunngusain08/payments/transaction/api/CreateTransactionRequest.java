package io.github.tarunngusain08.payments.transaction.api;

import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionContract;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.validation.SupportedCurrency;
import io.github.tarunngusain08.payments.validation.ValidMetadata;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.Map;

public record CreateTransactionRequest(
        @NotBlank
        @Pattern(
                regexp = TransactionContract.SOURCE_SYSTEM_PATTERN,
                message = "must be uppercase ASCII letters, digits, or underscore"
        )
        String sourceSystem,

        @NotBlank
        @Pattern(
                regexp = TransactionContract.EXTERNAL_REFERENCE_PATTERN,
                message = "must be a case-sensitive canonical ASCII reference"
        )
        String externalReference,

        @Positive
        long amount,

        @NotBlank
        @Pattern(regexp = "^INR$", message = "version 2 accepts only uppercase INR")
        @SupportedCurrency
        String currency,

        @NotNull
        TransactionType type,

        @NotBlank
        @Pattern(
                regexp = TransactionContract.ACCOUNT_IDENTIFIER_PATTERN,
                message = "must be a case-sensitive canonical ASCII account identifier"
        )
        String sourceAccount,

        @NotBlank
        @Pattern(
                regexp = TransactionContract.ACCOUNT_IDENTIFIER_PATTERN,
                message = "must be a case-sensitive canonical ASCII account identifier"
        )
        String destinationAccount,

        @NotNull
        PaymentChannel channel,

        Instant createdAt,

        @ValidMetadata
        Map<String, Object> metadata
) {
}

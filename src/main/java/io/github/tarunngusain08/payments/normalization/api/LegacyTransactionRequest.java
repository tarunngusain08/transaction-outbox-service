package io.github.tarunngusain08.payments.normalization.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record LegacyTransactionRequest(
        @JsonProperty("txn_ref")
        @NotBlank
        @Size(max = 100)
        String transactionReference,

        @JsonProperty("txn_amount")
        @NotBlank
        String transactionAmount,

        @JsonProperty("ccy")
        @NotBlank
        @Pattern(regexp = "^[A-Za-z]{3}$", message = "must be a three-letter ISO currency code")
        String currency,

        @JsonProperty("txn_type")
        @NotBlank
        String transactionType,

        @NotNull
        @Valid
        LegacyAccount payer,

        @NotNull
        @Valid
        LegacyAccount payee,

        @NotBlank
        String mode,

        @JsonProperty("txn_date")
        @NotBlank
        String transactionDate,

        @Size(max = 500)
        String remarks
) {
    public record LegacyAccount(
            @JsonProperty("acct_no")
            @NotBlank
            @Size(max = 64)
            String accountNumber,

            @NotBlank
            @Pattern(
                    regexp = "^[A-Za-z]{4}0[A-Za-z0-9]{6}$",
                    message = "must be a valid 11-character IFSC code"
            )
            String ifsc
    ) {
    }
}

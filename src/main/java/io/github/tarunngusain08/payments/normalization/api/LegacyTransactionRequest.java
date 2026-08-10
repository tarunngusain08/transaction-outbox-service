package io.github.tarunngusain08.payments.normalization.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.tarunngusain08.payments.validation.SupportedCurrency;
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
        @Size(max = 32)
        @Pattern(
                regexp = "^[0-9]+(?:\\.[0-9]+)?$",
                message = "must use ordinary decimal notation"
        )
        String transactionAmount,

        @JsonProperty("ccy")
        @NotBlank
        @Pattern(regexp = "(?i)^INR$", message = "version 1 supports only INR")
        @SupportedCurrency
        String currency,

        @JsonProperty("txn_type")
        @NotBlank
        @Size(max = 16)
        String transactionType,

        @NotNull
        @Valid
        LegacyAccount payer,

        @NotNull
        @Valid
        LegacyAccount payee,

        @NotBlank
        @Size(max = 32)
        String mode,

        @JsonProperty("txn_date")
        @NotBlank
        @Size(min = 19, max = 19)
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

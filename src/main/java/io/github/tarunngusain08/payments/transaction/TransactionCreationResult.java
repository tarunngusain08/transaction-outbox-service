package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;

public record TransactionCreationResult(
        TransactionResponse transaction,
        boolean created
) {

    public static TransactionCreationResult created(TransactionResponse transaction) {
        return new TransactionCreationResult(transaction, true);
    }

    public static TransactionCreationResult replayed(TransactionResponse transaction) {
        return new TransactionCreationResult(transaction, false);
    }
}

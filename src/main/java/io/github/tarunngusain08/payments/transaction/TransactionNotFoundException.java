package io.github.tarunngusain08.payments.transaction;

import java.util.UUID;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(UUID transactionId) {
        super("Transaction '%s' was not found".formatted(transactionId));
    }
}

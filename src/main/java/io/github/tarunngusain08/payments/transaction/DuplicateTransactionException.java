package io.github.tarunngusain08.payments.transaction;

public class DuplicateTransactionException extends RuntimeException {

    public DuplicateTransactionException(String externalReference) {
        super("A transaction with externalReference '%s' already exists".formatted(externalReference));
    }
}

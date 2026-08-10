package io.github.tarunngusain08.payments.transaction;

public class DuplicateTransactionException extends RuntimeException {

    public DuplicateTransactionException(String sourceSystem, String externalReference) {
        super("A different transaction already exists for sourceSystem '%s' and externalReference '%s'"
                .formatted(sourceSystem, externalReference));
    }
}

package io.github.tarunngusain08.payments.outbox;

public enum OutboxDeliveryResult {
    PUBLISHED,
    RETRY_SCHEDULED,
    PERMANENTLY_FAILED,
    INTERRUPTED,
    SKIPPED
}

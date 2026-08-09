package io.github.tarunngusain08.payments.outbox;

import java.time.Instant;

public record OutboxDeliverySnapshot(
        long pending,
        long processing,
        long failed,
        Long oldestUnpublishedAgeSeconds,
        Instant checkedAt
) {
}

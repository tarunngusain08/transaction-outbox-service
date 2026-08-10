package io.github.tarunngusain08.payments.outbox;

import java.time.Instant;

public record OutboxDeliverySnapshot(
        long pending,
        long processing,
        Long oldestUnpublishedAgeSeconds,
        Instant checkedAt
) {
}

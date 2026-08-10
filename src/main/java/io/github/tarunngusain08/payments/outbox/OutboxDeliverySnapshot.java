package io.github.tarunngusain08.payments.outbox;

import java.time.Instant;

public record OutboxDeliverySnapshot(
        long pending,
        long processing,
        long quarantined,
        Long oldestUnpublishedAgeSeconds,
        Long oldestQuarantinedAgeSeconds,
        Instant checkedAt
) {
}

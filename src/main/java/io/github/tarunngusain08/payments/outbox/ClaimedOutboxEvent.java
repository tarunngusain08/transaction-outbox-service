package io.github.tarunngusain08.payments.outbox;

import java.util.UUID;

public record ClaimedOutboxEvent(
        UUID eventId,
        UUID aggregateId,
        String payload,
        UUID claimToken
) {
}

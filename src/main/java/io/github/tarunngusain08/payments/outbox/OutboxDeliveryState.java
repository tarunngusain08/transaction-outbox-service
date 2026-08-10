package io.github.tarunngusain08.payments.outbox;

import java.time.Instant;

public interface OutboxDeliveryState {

    long getPending();

    long getProcessing();

    long getQuarantined();

    Instant getOldestUnpublishedAt();

    Instant getOldestQuarantinedAt();
}

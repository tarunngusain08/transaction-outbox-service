package io.github.tarunngusain08.payments.outbox;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component
@Endpoint(id = "outbox")
public class OutboxDeliveryEndpoint {

    private final OutboxEventRepository outboxRepository;
    private final Clock clock;

    public OutboxDeliveryEndpoint(OutboxEventRepository outboxRepository, Clock clock) {
        this.outboxRepository = outboxRepository;
        this.clock = clock;
    }

    @ReadOperation
    public OutboxDeliverySnapshot snapshot() {
        Instant now = Instant.now(clock);
        OutboxDeliveryState state = outboxRepository.summarizeDeliveryState();
        Long oldestAgeSeconds = state.getOldestUnpublishedAt() == null
                ? null
                : Math.max(
                        0,
                        Duration.between(state.getOldestUnpublishedAt(), now).toSeconds()
                );
        Long oldestQuarantinedAgeSeconds = state.getOldestQuarantinedAt() == null
                ? null
                : Math.max(
                        0,
                        Duration.between(state.getOldestQuarantinedAt(), now).toSeconds()
                );

        return new OutboxDeliverySnapshot(
                state.getPending(),
                state.getProcessing(),
                state.getQuarantined(),
                oldestAgeSeconds,
                oldestQuarantinedAgeSeconds,
                now
        );
    }
}

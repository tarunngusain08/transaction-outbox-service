package io.github.tarunngusain08.payments.outbox;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
@Endpoint(id = "outbox")
public class OutboxDeliveryEndpoint {

    private static final List<OutboxStatus> UNPUBLISHED_STATUSES = List.of(
            OutboxStatus.PENDING,
            OutboxStatus.PROCESSING
    );

    private final OutboxEventRepository outboxRepository;
    private final Clock clock;

    public OutboxDeliveryEndpoint(OutboxEventRepository outboxRepository, Clock clock) {
        this.outboxRepository = outboxRepository;
        this.clock = clock;
    }

    @ReadOperation
    public OutboxDeliverySnapshot snapshot() {
        Instant now = Instant.now(clock);
        Instant oldestUnpublished = outboxRepository.findOldestCreatedAtByStatusIn(
                UNPUBLISHED_STATUSES
        );
        Instant oldestQuarantined = outboxRepository.findOldestCreatedAtByStatusIn(
                List.of(OutboxStatus.QUARANTINED)
        );
        Long oldestAgeSeconds = oldestUnpublished == null
                ? null
                : Math.max(0, Duration.between(oldestUnpublished, now).toSeconds());
        Long oldestQuarantinedAgeSeconds = oldestQuarantined == null
                ? null
                : Math.max(0, Duration.between(oldestQuarantined, now).toSeconds());

        return new OutboxDeliverySnapshot(
                outboxRepository.countByStatus(OutboxStatus.PENDING),
                outboxRepository.countByStatus(OutboxStatus.PROCESSING),
                outboxRepository.countByStatus(OutboxStatus.QUARANTINED),
                oldestAgeSeconds,
                oldestQuarantinedAgeSeconds,
                now
        );
    }
}

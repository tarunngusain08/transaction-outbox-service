package io.github.tarunngusain08.payments.outbox;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class OutboxEventClaimService {

    private final OutboxEventRepository outboxRepository;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxEventClaimService(
            OutboxEventRepository outboxRepository,
            OutboxProperties properties,
            Clock clock
    ) {
        this.outboxRepository = outboxRepository;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<ClaimedOutboxEvent> claimNext() {
        Instant now = Instant.now(clock);
        Instant expiredBefore = now.minus(properties.claimLease());
        var eventId = outboxRepository.findNextClaimableEventId(
                now,
                expiredBefore
        ).orElse(null);
        if (eventId == null) {
            return Optional.empty();
        }

        var event = outboxRepository.findById(eventId).orElse(null);
        if (event == null) {
            return Optional.empty();
        }

        UUID claimToken = UUID.randomUUID();
        event.claim(claimToken, now);
        return Optional.of(new ClaimedOutboxEvent(
                event.getId(),
                event.getAggregateId(),
                event.getPayload(),
                claimToken
        ));
    }
}

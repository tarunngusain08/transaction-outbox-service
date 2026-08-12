package io.github.tarunngusain08.payments.outbox;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
    public List<ClaimedOutboxEvent> claimBatch() {
        Instant now = Instant.now(clock);
        Instant expiredBefore = now.minus(properties.claimLease());
        var eventIds = outboxRepository.findClaimableEventIds(
                now,
                expiredBefore,
                properties.batchSize()
        );
        var claimedEvents = new ArrayList<ClaimedOutboxEvent>(eventIds.size());

        for (var eventId : eventIds) {
            var event = outboxRepository.findById(eventId).orElse(null);
            if (event == null) {
                continue;
            }

            UUID claimToken = UUID.randomUUID();
            event.claim(claimToken, now);
            claimedEvents.add(new ClaimedOutboxEvent(
                    event.getId(),
                    event.getAggregateId(),
                    event.getPayload(),
                    claimToken
            ));
        }

        return List.copyOf(claimedEvents);
    }
}

package io.github.tarunngusain08.payments.outbox;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPoller {

    private final OutboxPublisher publisher;

    public OutboxPoller(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${payments.outbox.poll-delay:1s}")
    public void publishPendingEvents() {
        publisher.publishPendingBatch();
    }
}

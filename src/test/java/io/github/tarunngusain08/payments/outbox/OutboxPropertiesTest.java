package io.github.tarunngusain08.payments.outbox;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxPropertiesTest {

    @Test
    void acceptsALeaseLongerThanEveryBoundedSendPhaseAndSafetyMargin() {
        assertThatCode(() -> properties(Duration.ofSeconds(26)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsALeaseEqualToOrShorterThanTheCompleteSendEnvelope() {
        assertThatThrownBy(() -> properties(Duration.ofSeconds(25)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("claimLease must exceed");
        assertThatThrownBy(() -> properties(Duration.ofSeconds(24)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("claimLease must exceed");
    }

    @Test
    void rejectsNonPositiveDurations() {
        assertThatThrownBy(() -> new OutboxProperties(
                "payments.transactions.created",
                Duration.ZERO,
                1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pollDelay must be positive");
    }

    @Test
    void rejectsAnEmptyBatch() {
        assertThatThrownBy(() -> new OutboxProperties(
                "payments.transactions.created",
                Duration.ofSeconds(1),
                0,
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5)
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("batchSize must be at least one");
    }

    private OutboxProperties properties(Duration claimLease) {
        return new OutboxProperties(
                "payments.transactions.created",
                Duration.ofSeconds(1),
                1,
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                claimLease,
                Duration.ofSeconds(5)
        );
    }
}

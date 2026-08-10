package io.github.tarunngusain08.payments.outbox;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "payments.outbox")
public record OutboxProperties(
        @NotBlank String topic,
        @NotNull Duration pollDelay,
        @Min(1) int batchSize,
        @NotNull Duration publishTimeout,
        @NotNull Duration producerMaxBlock,
        @NotNull Duration claimLease,
        @NotNull Duration claimSafetyMargin
) {
    public OutboxProperties {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least one");
        }
        requirePositive("pollDelay", pollDelay);
        requirePositive("publishTimeout", publishTimeout);
        requirePositive("producerMaxBlock", producerMaxBlock);
        requirePositive("claimLease", claimLease);
        requirePositive("claimSafetyMargin", claimSafetyMargin);

        if (claimLease != null
                && publishTimeout != null
                && producerMaxBlock != null
                && claimSafetyMargin != null) {
            Duration minimumLease = producerMaxBlock
                    .plus(publishTimeout)
                    .plus(claimSafetyMargin);
            if (claimLease.compareTo(minimumLease) <= 0) {
                throw new IllegalArgumentException(
                        "claimLease must exceed producerMaxBlock + publishTimeout "
                                + "+ claimSafetyMargin"
                );
            }
        }
    }

    private static void requirePositive(String name, Duration duration) {
        if (duration != null && (duration.isZero() || duration.isNegative())) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}

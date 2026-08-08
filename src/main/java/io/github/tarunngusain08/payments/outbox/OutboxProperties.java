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
        @Min(1) int maxRetries,
        @NotNull Duration publishTimeout
) {
}

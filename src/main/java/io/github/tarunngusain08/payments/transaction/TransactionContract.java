package io.github.tarunngusain08.payments.transaction;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class TransactionContract {

    public static final String DIRECT_API_SOURCE = "DIRECT_API";
    public static final String LEGACY_BANK_FEED_SOURCE = "LEGACY_BANK_FEED";
    public static final String INR = "INR";
    public static final int INR_MINOR_UNIT_EXPONENT = 2;
    public static final String SOURCE_SYSTEM_PATTERN = "^[A-Z][A-Z0-9_]{0,31}$";
    public static final String EXTERNAL_REFERENCE_PATTERN =
            "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$";
    public static final String ACCOUNT_IDENTIFIER_PATTERN =
            "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$";
    public static final Instant EARLIEST_CREATED_AT = Instant.parse("1970-01-01T00:00:00Z");
    public static final Duration MAX_FUTURE_CLOCK_SKEW = Duration.ofMinutes(5);

    private TransactionContract() {
    }

    public static Instant canonicalTimestamp(Instant timestamp) {
        return timestamp.truncatedTo(ChronoUnit.MICROS);
    }

    public static void validateCreatedAt(Instant createdAt, Instant receivedAt) {
        if (createdAt.isBefore(EARLIEST_CREATED_AT)
                || createdAt.isAfter(receivedAt.plus(MAX_FUTURE_CLOCK_SKEW))) {
            throw new InvalidTransactionException(
                    "createdAt must be between 1970-01-01T00:00:00Z and five minutes in the future"
            );
        }
    }
}

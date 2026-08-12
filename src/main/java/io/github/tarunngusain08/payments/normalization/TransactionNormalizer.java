package io.github.tarunngusain08.payments.normalization;

import io.github.tarunngusain08.payments.normalization.api.LegacyTransactionRequest;
import io.github.tarunngusain08.payments.transaction.InvalidTransactionException;
import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionContract;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.validation.CurrencySupport;
import io.github.tarunngusain08.payments.validation.MetadataCanonicalizer;
import jakarta.validation.Validator;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class TransactionNormalizer {

    static final ZoneId SOURCE_TIME_ZONE = ZoneId.of("Asia/Kolkata");
    static final DateTimeFormatter SOURCE_DATE_FORMAT = DateTimeFormatter
            .ofPattern("dd-MM-uuuu HH:mm:ss", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);
    private static final Pattern ORDINARY_DECIMAL =
            Pattern.compile("^[0-9]+(?:\\.[0-9]+)?$");
    private final MetadataCanonicalizer metadataCanonicalizer;
    private final Validator validator;
    private final Clock clock;

    public TransactionNormalizer(
            MetadataCanonicalizer metadataCanonicalizer,
            Validator validator,
            Clock clock
    ) {
        this.metadataCanonicalizer = metadataCanonicalizer;
        this.validator = validator;
        this.clock = clock;
    }

    public CreateTransactionRequest normalize(LegacyTransactionRequest source) {
        String currency = canonicalCurrency(source.currency());
        var channel = toChannel(source.mode());
        var createdAt = toInstant(source.transactionDate());
        Map<String, Object> metadata;
        try {
            metadata = metadataCanonicalizer.canonicalize(metadata(source, channel));
        } catch (InvalidTransactionException exception) {
            throw new NormalizationException(exception.getMessage(), exception);
        }

        var normalized = new CreateTransactionRequest(
                TransactionContract.LEGACY_BANK_FEED_SOURCE,
                source.transactionReference().trim(),
                toMinorUnits(source.transactionAmount(), currency),
                currency,
                toTransactionType(source.transactionType()),
                source.payer().accountNumber().trim(),
                source.payee().accountNumber().trim(),
                channel,
                createdAt,
                metadata
        );
        validateCanonicalOutput(normalized);
        return normalized;
    }

    long toMinorUnits(String rawAmount, String currencyCode) {
        String currency = canonicalCurrency(currencyCode);
        if (rawAmount == null || !ORDINARY_DECIMAL.matcher(rawAmount).matches()) {
            throw new NormalizationException(
                    "txn_amount must be a positive plain-decimal INR amount"
            );
        }

        try {
            var amount = new BigDecimal(rawAmount);
            if (amount.signum() <= 0) {
                throw new NormalizationException("txn_amount must be greater than zero");
            }

            return amount
                    .setScale(TransactionContract.INR_MINOR_UNIT_EXPONENT, RoundingMode.UNNECESSARY)
                    .movePointRight(TransactionContract.INR_MINOR_UNIT_EXPONENT)
                    .longValueExact();
        } catch (NumberFormatException | ArithmeticException exception) {
            throw new NormalizationException(
                    "txn_amount must be an exact %s amount with no fractional paise"
                            .formatted(currency),
                    exception
            );
        }
    }

    TransactionType toTransactionType(String rawType) {
        return switch (rawType.trim().toUpperCase(Locale.ROOT)) {
            case "DR", "DEBIT" -> TransactionType.DEBIT;
            case "CR", "CREDIT" -> TransactionType.CREDIT;
            default -> throw new NormalizationException("Unsupported txn_type: " + rawType);
        };
    }

    PaymentChannel toChannel(String rawMode) {
        String mode = rawMode.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
        return switch (mode) {
            case "UPI" -> PaymentChannel.UPI;
            case "CARD" -> PaymentChannel.CARD;
            case "NEFT" -> PaymentChannel.NEFT;
            case "IMPS" -> PaymentChannel.IMPS;
            case "RTGS" -> PaymentChannel.RTGS;
            case "BANK", "BANK_TRANSFER" -> PaymentChannel.BANK_TRANSFER;
            default -> PaymentChannel.OTHER;
        };
    }

    private Instant toInstant(String rawDate) {
        try {
            return LocalDateTime.parse(rawDate.trim(), SOURCE_DATE_FORMAT)
                    .atZone(SOURCE_TIME_ZONE)
                    .toInstant()
                    .truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        } catch (DateTimeException exception) {
            throw new NormalizationException(
                    "txn_date must use dd-MM-yyyy HH:mm:ss and represent a valid date",
                    exception
            );
        }
    }

    private Map<String, Object> metadata(
            LegacyTransactionRequest source,
            PaymentChannel normalizedChannel
    ) {
        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("sourceFormat", "LEGACY_BANK_FEED");
        metadata.put("payerIfsc", source.payer().ifsc().trim().toUpperCase(Locale.ROOT));
        metadata.put("payeeIfsc", source.payee().ifsc().trim().toUpperCase(Locale.ROOT));

        if (source.remarks() != null && !source.remarks().isBlank()) {
            metadata.put("remarks", source.remarks().trim());
        }
        if (normalizedChannel == PaymentChannel.OTHER) {
            metadata.put("originalMode", source.mode().trim());
        }
        return Map.copyOf(metadata);
    }

    private String canonicalCurrency(String rawCurrency) {
        try {
            return CurrencySupport.canonicalizeLegacy(rawCurrency);
        } catch (IllegalArgumentException exception) {
            throw new NormalizationException("Unsupported ccy: " + rawCurrency, exception);
        }
    }

    private void validateCanonicalOutput(CreateTransactionRequest normalized) {
        validator.validate(normalized).stream()
                .min(Comparator.comparing(violation -> violation.getPropertyPath().toString()))
                .ifPresent(violation -> {
                    throw new NormalizationException(
                            "normalized " + violation.getPropertyPath() + " "
                                    + violation.getMessage()
                    );
                });

        try {
            TransactionContract.validateCreatedAt(
                    normalized.createdAt(),
                    TransactionContract.canonicalTimestamp(Instant.now(clock))
            );
        } catch (InvalidTransactionException exception) {
            throw new NormalizationException(exception.getMessage(), exception);
        }
    }
}

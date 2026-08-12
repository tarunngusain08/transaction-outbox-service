package io.github.tarunngusain08.payments.normalization;

import io.github.tarunngusain08.payments.normalization.api.LegacyTransactionRequest;
import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionStatus;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.transaction.api.TransactionResponse;
import io.github.tarunngusain08.payments.validation.CurrencySupport;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Currency;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Service
public class TransactionNormalizer {

    static final ZoneId SOURCE_TIME_ZONE = ZoneId.of("Asia/Kolkata");
    static final DateTimeFormatter SOURCE_DATE_FORMAT = DateTimeFormatter
            .ofPattern("dd-MM-uuuu HH:mm:ss", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    public TransactionResponse normalize(LegacyTransactionRequest source) {
        String currency = source.currency().trim().toUpperCase(Locale.ROOT);
        var channel = toChannel(source.mode());

        return new TransactionResponse(
                UUID.randomUUID(),
                source.transactionReference().trim(),
                toMinorUnits(source.transactionAmount(), currency),
                currency,
                toTransactionType(source.transactionType()),
                TransactionStatus.PENDING,
                source.payer().accountNumber().trim(),
                source.payee().accountNumber().trim(),
                channel,
                toInstant(source.transactionDate()),
                metadata(source, channel)
        );
    }

    long toMinorUnits(String rawAmount, String currencyCode) {
        final Currency currency;
        try {
            currency = CurrencySupport.resolve(currencyCode);
        } catch (IllegalArgumentException exception) {
            throw new NormalizationException("Unsupported ccy: " + currencyCode, exception);
        }

        try {
            int fractionDigits = currency.getDefaultFractionDigits();
            if (fractionDigits < 0) {
                throw new NormalizationException("Currency does not define minor units: " + currencyCode);
            }

            var amount = new BigDecimal(rawAmount.trim());
            if (amount.signum() <= 0) {
                throw new NormalizationException("txn_amount must be greater than zero");
            }

            return amount
                    .setScale(fractionDigits, RoundingMode.UNNECESSARY)
                    .movePointRight(fractionDigits)
                    .longValueExact();
        } catch (NumberFormatException | ArithmeticException exception) {
            throw new NormalizationException(
                    "txn_amount must be a valid %s amount with no fractional minor units".formatted(currencyCode),
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

    private java.time.Instant toInstant(String rawDate) {
        try {
            return LocalDateTime.parse(rawDate.trim(), SOURCE_DATE_FORMAT)
                    .atZone(SOURCE_TIME_ZONE)
                    .toInstant();
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
}

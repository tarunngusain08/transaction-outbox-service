package io.github.tarunngusain08.payments.normalization;

import io.github.tarunngusain08.payments.normalization.api.LegacyTransactionRequest;
import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.validation.MetadataCanonicalizer;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionNormalizerTest {

    private final TransactionNormalizer normalizer = new TransactionNormalizer(
            new MetadataCanonicalizer(new ObjectMapper()),
            Validation.buildDefaultValidatorFactory().getValidator(),
            Clock.fixed(Instant.parse("2026-08-10T10:15:30Z"), ZoneOffset.UTC)
    );

    @Test
    void mapsLegacyPaymentToCanonicalSchema() {
        var normalized = normalizer.normalize(validRequest());

        assertThat(normalized.sourceSystem()).isEqualTo("LEGACY_BANK_FEED");
        assertThat(normalized.externalReference()).isEqualTo("TXN-88213-ABC");
        assertThat(normalized.amount()).isEqualTo(150_000L);
        assertThat(normalized.currency()).isEqualTo("INR");
        assertThat(normalized.type()).isEqualTo(TransactionType.DEBIT);
        assertThat(normalized.sourceAccount()).isEqualTo("1234567890");
        assertThat(normalized.destinationAccount()).isEqualTo("9876543210");
        assertThat(normalized.channel()).isEqualTo(PaymentChannel.UPI);
        assertThat(normalized.createdAt()).isEqualTo(Instant.parse("2026-08-08T09:02:11Z"));
        assertThat(normalized.metadata()).containsEntry("payerIfsc", "HDFC0001234");
        assertThat(normalized.metadata()).containsEntry("payeeIfsc", "ICIC0005678");
        assertThat(normalized.metadata()).containsEntry("remarks", "Grocery payment");
        assertThat(normalized.metadata()).containsEntry("sourceFormat", "LEGACY_BANK_FEED");
        assertThat(normalizer.normalize(validRequest())).isEqualTo(normalized);
    }

    @Test
    void mapsCreditCodeAndUnknownChannel() {
        var request = new LegacyTransactionRequest(
                "TXN-2",
                "42.25",
                "inr",
                "cr",
                validRequest().payer(),
                validRequest().payee(),
                "cash-counter",
                "08-08-2026 14:32:11",
                null
        );

        var normalized = normalizer.normalize(request);

        assertThat(normalized.type()).isEqualTo(TransactionType.CREDIT);
        assertThat(normalized.channel()).isEqualTo(PaymentChannel.OTHER);
        assertThat(normalized.metadata()).containsEntry("originalMode", "cash-counter");
        assertThat(normalized.metadata()).doesNotContainKey("remarks");
    }

    @Test
    void rejectsFractionalMinorUnits() {
        assertThatThrownBy(() -> normalizer.toMinorUnits("10.001", "INR"))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("no fractional paise");
    }

    @Test
    void acceptsAnyExactPlainDecimalRupeeRepresentation() {
        assertThat(normalizer.toMinorUnits("10", "INR")).isEqualTo(1_000L);
        assertThat(normalizer.toMinorUnits("10.0", "INR")).isEqualTo(1_000L);
        assertThat(normalizer.toMinorUnits("10.00", "INR")).isEqualTo(1_000L);
        assertThat(normalizer.toMinorUnits("10.000", "INR")).isEqualTo(1_000L);
    }

    @Test
    void rejectsSignedExponentWhitespaceZeroAndOverflowAmounts() {
        assertThatThrownBy(() -> normalizer.toMinorUnits("+10.00", "INR"))
                .isInstanceOf(NormalizationException.class);
        assertThatThrownBy(() -> normalizer.toMinorUnits("1e2", "INR"))
                .isInstanceOf(NormalizationException.class);
        assertThatThrownBy(() -> normalizer.toMinorUnits(" 10.00 ", "INR"))
                .isInstanceOf(NormalizationException.class);
        assertThatThrownBy(() -> normalizer.toMinorUnits("0.00", "INR"))
                .isInstanceOf(NormalizationException.class);
        assertThatThrownBy(() -> normalizer.toMinorUnits("999999999999999999999.00", "INR"))
                .isInstanceOf(NormalizationException.class);
    }

    @Test
    void rejectsUnsupportedTransactionType() {
        assertThatThrownBy(() -> normalizer.toTransactionType("REVERSAL"))
                .isInstanceOf(NormalizationException.class)
                .hasMessage("Unsupported txn_type: REVERSAL");
    }

    @Test
    void rejectsUnsupportedCurrency() {
        assertThatThrownBy(() -> normalizer.toMinorUnits("10.00", "XYZ"))
                .isInstanceOf(NormalizationException.class)
                .hasMessage("Unsupported ccy: XYZ");
    }

    @Test
    void rejectsInvalidCalendarDate() {
        var request = new LegacyTransactionRequest(
                "TXN-3",
                "10.00",
                "INR",
                "DR",
                validRequest().payer(),
                validRequest().payee(),
                "UPI",
                "31-02-2026 14:32:11",
                null
        );

        assertThatThrownBy(() -> normalizer.normalize(request))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("txn_date must use");
    }

    @Test
    void rejectsOutputThatCannotBeSubmittedToCreate() {
        var request = new LegacyTransactionRequest(
                " invalid reference ",
                "10.00",
                "INR",
                "DR",
                validRequest().payer(),
                validRequest().payee(),
                "UPI",
                "08-08-2026 14:32:11",
                null
        );

        assertThatThrownBy(() -> normalizer.normalize(request))
                .isInstanceOf(NormalizationException.class)
                .hasMessageContaining("externalReference");
    }

    private LegacyTransactionRequest validRequest() {
        return new LegacyTransactionRequest(
                " TXN-88213-ABC ",
                "1500.00",
                "INR",
                "DR",
                new LegacyTransactionRequest.LegacyAccount(" 1234567890 ", "hdfc0001234"),
                new LegacyTransactionRequest.LegacyAccount(" 9876543210 ", "icic0005678"),
                "UPI",
                "08-08-2026 14:32:11",
                " Grocery payment "
        );
    }
}

package io.github.tarunngusain08.payments.normalization;

import io.github.tarunngusain08.payments.transaction.domain.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.domain.TransactionType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionNormalizerTest {

    private final TransactionNormalizer normalizer = new TransactionNormalizer();

    @Test
    void convertsLegacyFieldsIntoTheCreateSchema() {
        var normalized = normalizer.normalize(request("1500.00", "DR", "UPI", "08-08-2026 14:32:11"));

        assertThat(normalized.externalReference()).isEqualTo("TXN-88213-ABC");
        assertThat(normalized.amount()).isEqualTo(150_000L);
        assertThat(normalized.currency()).isEqualTo("INR");
        assertThat(normalized.type()).isEqualTo(TransactionType.DEBIT);
        assertThat(normalized.channel()).isEqualTo(PaymentChannel.UPI);
        assertThat(normalized.createdAt()).isEqualTo(Instant.parse("2026-08-08T09:02:11Z"));
        assertThat(normalized.metadata())
                .containsEntry("payerIfsc", "HDFC0001234")
                .containsEntry("payeeIfsc", "ICIC0005678")
                .containsEntry("remarks", "Grocery payment");
    }

    @Test
    void mapsCreditAndUnknownChannels() {
        var normalized = normalizer.normalize(request("10.00", "CR", "CASH", "08-08-2026 14:32:11"));

        assertThat(normalized.type()).isEqualTo(TransactionType.CREDIT);
        assertThat(normalized.channel()).isEqualTo(PaymentChannel.OTHER);
        assertThat(normalized.metadata()).containsEntry("originalMode", "CASH");
    }

    @Test
    void rejectsAmountsWithFractionalMinorUnits() {
        assertThatThrownBy(() -> normalizer.normalize(
                request("10.001", "DR", "UPI", "08-08-2026 14:32:11")
        )).isInstanceOf(NormalizationException.class)
                .hasMessageContaining("fractional minor units");
    }

    @Test
    void rejectsUnsupportedTypesAndInvalidDates() {
        assertThatThrownBy(() -> normalizer.normalize(
                request("10.00", "SIDEWAYS", "UPI", "08-08-2026 14:32:11")
        )).isInstanceOf(NormalizationException.class)
                .hasMessageContaining("Unsupported txn_type");

        assertThatThrownBy(() -> normalizer.normalize(
                request("10.00", "DR", "UPI", "31-02-2026 14:32:11")
        )).isInstanceOf(NormalizationException.class)
                .hasMessageContaining("txn_date");
    }

    private LegacyTransactionRequest request(
            String amount,
            String type,
            String mode,
            String date
    ) {
        return new LegacyTransactionRequest(
                "TXN-88213-ABC",
                amount,
                "INR",
                type,
                new LegacyTransactionRequest.LegacyAccount("1234567890", "HDFC0001234"),
                new LegacyTransactionRequest.LegacyAccount("9876543210", "ICIC0005678"),
                mode,
                date,
                "Grocery payment"
        );
    }
}

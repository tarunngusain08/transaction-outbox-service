package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import io.github.tarunngusain08.payments.validation.MetadataCanonicalizer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionRequestFingerprintTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TransactionRequestFingerprint fingerprint =
            new TransactionRequestFingerprint(objectMapper);
    private final MetadataCanonicalizer canonicalizer = new MetadataCanonicalizer(objectMapper);

    @Test
    void pinsVersionOneCanonicalDigest() {
        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("zeta", "x");
        metadata.put("alpha", 1);

        assertThat(fingerprint.calculate(request(), null, canonicalizer.canonicalize(metadata)))
                .isEqualTo("9f161df0e21cda36d466827cf8e9ea3a7b6b3c52bf1c49276dee158956abd9b4");
    }

    @Test
    void canonicalMetadataOrderAndIntegerWidthsProduceTheSameDigest() {
        var first = new LinkedHashMap<String, Object>();
        first.put("zeta", "x");
        first.put("alpha", 1);
        var second = new LinkedHashMap<String, Object>();
        second.put("alpha", 1L);
        second.put("zeta", "x");

        assertThat(fingerprint.calculate(request(), null, canonicalizer.canonicalize(first)))
                .isEqualTo(fingerprint.calculate(
                        request(),
                        null,
                        canonicalizer.canonicalize(second)
                ));
    }

    @Test
    void everyClientOwnedFieldAndRequestedTimestampParticipateInTheDigest() {
        String baseline = fingerprint.calculate(request(), null, Map.of());
        var differentSource = new CreateTransactionRequest(
                "PARTNER_BANK",
                request().externalReference(),
                request().amount(),
                request().currency(),
                request().type(),
                request().sourceAccount(),
                request().destinationAccount(),
                request().channel(),
                request().createdAt(),
                request().metadata()
        );

        assertThat(fingerprint.calculate(differentSource, null, Map.of())).isNotEqualTo(baseline);
        assertThat(fingerprint.calculate(
                request(),
                Instant.parse("2026-08-08T09:02:11.123456Z"),
                Map.of()
        )).isNotEqualTo(baseline);
        assertThat(fingerprint.calculate(request(), null, Map.of("attempt", 1L)))
                .isNotEqualTo(baseline);
    }

    private CreateTransactionRequest request() {
        return new CreateTransactionRequest(
                "DIRECT_API",
                "TXN-88213-ABC",
                150_000L,
                "INR",
                TransactionType.DEBIT,
                "1234567890",
                "9876543210",
                PaymentChannel.UPI,
                null,
                Map.of()
        );
    }
}

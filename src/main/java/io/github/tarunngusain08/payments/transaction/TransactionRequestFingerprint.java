package io.github.tarunngusain08.payments.transaction;

import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;

@Component
public class TransactionRequestFingerprint {

    public static final short VERSION = 1;

    private final ObjectMapper objectMapper;

    public TransactionRequestFingerprint(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String calculate(
            CreateTransactionRequest request,
            Instant requestedCreatedAt,
            Map<String, Object> canonicalMetadata
    ) {
        var payload = new FingerprintPayload(
                VERSION,
                request.sourceSystem(),
                request.externalReference(),
                request.amount(),
                request.currency(),
                request.type().name(),
                request.sourceAccount(),
                request.destinationAccount(),
                request.channel().name(),
                requestedCreatedAt,
                canonicalMetadata
        );

        try {
            byte[] canonicalBytes = objectMapper.writeValueAsBytes(payload);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonicalBytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Could not encode the request fingerprint", exception);
        }
    }

    private record FingerprintPayload(
            short fingerprintVersion,
            String sourceSystem,
            String externalReference,
            long amount,
            String currency,
            String type,
            String sourceAccount,
            String destinationAccount,
            String channel,
            Instant requestedCreatedAt,
            Map<String, Object> metadata
    ) {
    }
}

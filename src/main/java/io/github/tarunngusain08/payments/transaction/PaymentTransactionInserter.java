package io.github.tarunngusain08.payments.transaction;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.util.UUID;

@Repository
public class PaymentTransactionInserter {

    private static final String INSERT_IF_ABSENT = """
            INSERT INTO transactions (
                id,
                source_system,
                external_reference,
                amount_minor,
                currency,
                type,
                status,
                source_account,
                destination_account,
                channel,
                created_at,
                received_at,
                metadata,
                request_fingerprint,
                request_fingerprint_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSONB), ?, ?)
            ON CONFLICT (source_system, external_reference) DO NOTHING
            RETURNING id
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public PaymentTransactionInserter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public boolean insertIfAbsent(PaymentTransaction transaction) {
        String metadata = serializeMetadata(transaction);
        var insertedIds = jdbcTemplate.query(
                INSERT_IF_ABSENT,
                statement -> {
                    statement.setObject(1, transaction.getId());
                    statement.setString(2, transaction.getSourceSystem());
                    statement.setString(3, transaction.getExternalReference());
                    statement.setLong(4, transaction.getAmountMinor());
                    statement.setString(5, transaction.getCurrency());
                    statement.setString(6, transaction.getType().name());
                    statement.setString(7, transaction.getStatus().name());
                    statement.setString(8, transaction.getSourceAccount());
                    statement.setString(9, transaction.getDestinationAccount());
                    statement.setString(10, transaction.getChannel().name());
                    statement.setTimestamp(11, Timestamp.from(transaction.getCreatedAt()));
                    statement.setTimestamp(12, Timestamp.from(transaction.getReceivedAt()));
                    statement.setString(13, metadata);
                    statement.setString(14, transaction.getRequestFingerprint());
                    statement.setShort(15, transaction.getRequestFingerprintVersion());
                },
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class)
        );

        if (insertedIds.size() > 1) {
            throw new IllegalStateException("insert-if-absent returned more than one transaction");
        }
        return !insertedIds.isEmpty();
    }

    private String serializeMetadata(PaymentTransaction transaction) {
        try {
            return objectMapper.writeValueAsString(transaction.getMetadata());
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Could not serialize canonical transaction metadata", exception);
        }
    }
}

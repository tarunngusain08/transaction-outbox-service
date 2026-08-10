package io.github.tarunngusain08.payments.integration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class MigrationUpgradeIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            DockerImageName.parse(
                    "postgres:17.10-alpine@sha256:"
                            + "742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
            ).asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("payments")
            .withUsername("payments")
            .withPassword("payments");

    @Test
    void upgradesPopulatedV2AndAuditsNonCollidingCanonicalization() throws Exception {
        String schema = createSchema("upgrade_success");
        migrateToV2(schema);
        UUID transactionId = insertV2Transaction(schema, " PADDED-REFERENCE ");

        flyway(schema, null).migrate();

        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     SELECT external_reference, source_system, received_at,
                            request_fingerprint, request_fingerprint_version
                     FROM %s.transactions
                     WHERE id = ?
                     """.formatted(schema))) {
            statement.setObject(1, transactionId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("external_reference")).isEqualTo("PADDED-REFERENCE");
                assertThat(rows.getString("source_system")).isEqualTo("DIRECT_API");
                assertThat(rows.getTimestamp("received_at")).isNotNull();
                assertThat(rows.getString("request_fingerprint")).isNull();
                assertThat(rows.getObject("request_fingerprint_version")).isNull();
            }
        }

        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     SELECT migration_version, source_system, old_reference, new_reference
                     FROM %s.transaction_reference_canonicalization_audit
                     WHERE transaction_id = ?
                     """.formatted(schema))) {
            statement.setObject(1, transactionId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("migration_version")).isEqualTo("V3");
                assertThat(rows.getString("source_system")).isEqualTo("DIRECT_API");
                assertThat(rows.getString("old_reference")).isEqualTo(" PADDED-REFERENCE ");
                assertThat(rows.getString("new_reference")).isEqualTo("PADDED-REFERENCE");
            }
        }

        assertThatThrownBy(() -> mutateAuditRow(schema, transactionId))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("canonicalization audit is row-mutation protected");
    }

    @Test
    void abortsBeforeMutationWhenCanonicalReferencesCollide() throws Exception {
        String schema = createSchema("upgrade_collision");
        migrateToV2(schema);
        UUID paddedId = insertV2Transaction(schema, " COLLISION ");
        UUID canonicalId = insertV2Transaction(schema, "COLLISION");

        assertThatThrownBy(() -> flyway(schema, null).migrate())
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("canonical external-reference collisions exist");

        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     SELECT id, external_reference
                     FROM %s.transactions
                     ORDER BY external_reference
                     """.formatted(schema));
             var rows = statement.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getObject("id", UUID.class)).isEqualTo(paddedId);
            assertThat(rows.getString("external_reference")).isEqualTo(" COLLISION ");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getObject("id", UUID.class)).isEqualTo(canonicalId);
            assertThat(rows.getString("external_reference")).isEqualTo("COLLISION");
            assertThat(rows.next()).isFalse();
        }

        try (var connection = connection();
             var statement = connection.prepareStatement(
                     "SELECT TO_REGCLASS(?)"
             )) {
            statement.setString(1, schema + ".transaction_reference_canonicalization_audit");
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isNull();
            }
        }
    }

    @Test
    void upgradesPreviousM08DatabaseAndQuarantinesHistoricalPayload() throws Exception {
        String schema = createSchema("outbox_quarantine");
        flyway(schema, MigrationVersion.fromVersion("5")).migrate();
        UUID transactionId = insertV2Transaction(schema, "OUTBOX-QUARANTINE");
        UUID eventId = insertFailedOutboxEvent(schema, transactionId);

        // V1-V6 are byte-locked to artifact 1d9d97c by AppliedMigrationIntegrityTest.
        // Reaching V6 therefore creates the exact previously deployed database state.
        flyway(schema, MigrationVersion.fromVersion("6")).migrate();
        Instant recoveredNextAttempt = readNextAttemptAt(schema, eventId);

        flyway(schema, null).migrate();

        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     SELECT status,
                            payload ->> 'schemaVersion' AS schema_version,
                            payload #>> '{transaction,externalReference}' AS external_reference,
                            next_attempt_at, retry_count, last_error,
                            quarantined_at, quarantine_reason, quarantined_from_status
                     FROM %s.outbox_events
                     WHERE id = ?
                     """.formatted(schema))) {
            statement.setObject(1, eventId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("status")).isEqualTo("QUARANTINED");
                assertThat(rows.getString("external_reference")).isEqualTo("OUTBOX-QUARANTINE");
                assertThat(rows.getString("schema_version")).isNull();
                assertThat(rows.getTimestamp("next_attempt_at").toInstant())
                        .isEqualTo(recoveredNextAttempt);
                assertThat(rows.getInt("retry_count")).isEqualTo(8);
                assertThat(rows.getString("last_error")).isEqualTo("historical broker outage");
                assertThat(rows.getTimestamp("quarantined_at")).isNotNull();
                assertThat(rows.getString("quarantine_reason"))
                        .contains("predates event schema version 2");
                assertThat(rows.getString("quarantined_from_status")).isEqualTo("PENDING");
            }
        }

        assertThatThrownBy(() -> setOutboxStatus(schema, eventId, "FAILED"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("chk_outbox_");
        assertThatThrownBy(() -> setOutboxStatus(schema, eventId, "PENDING"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("chk_outbox_delivery_state");
        assertThatThrownBy(() -> releaseQuarantinedEvent(schema, eventId))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("chk_outbox_publishable_schema_v2");
    }

    private String createSchema(String prefix) throws SQLException {
        String schema = prefix + "_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = connection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        return schema;
    }

    private void migrateToV2(String schema) {
        flyway(schema, MigrationVersion.fromVersion("2")).migrate();
    }

    private Flyway flyway(String schema, MigrationVersion target) {
        var configuration = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .defaultSchema(schema)
                .schemas(schema)
                .locations("classpath:db/migration");
        if (target != null) {
            configuration.target(target);
        }
        return configuration.load();
    }

    private UUID insertV2Transaction(String schema, String externalReference) throws SQLException {
        UUID transactionId = UUID.randomUUID();
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     INSERT INTO %s.transactions (
                         id,
                         external_reference,
                         amount_minor,
                         currency,
                         type,
                         status,
                         source_account,
                         destination_account,
                         channel,
                         created_at,
                         metadata
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSONB))
                     """.formatted(schema))) {
            statement.setObject(1, transactionId);
            statement.setString(2, externalReference);
            statement.setLong(3, 150_000L);
            statement.setString(4, "INR");
            statement.setString(5, "DEBIT");
            statement.setString(6, "PENDING");
            statement.setString(7, "1234567890");
            statement.setString(8, "9876543210");
            statement.setString(9, "UPI");
            statement.setTimestamp(10, Timestamp.from(Instant.parse("2026-08-08T09:02:11Z")));
            statement.setString(11, "{}");
            assertThat(statement.executeUpdate()).isOne();
        }
        return transactionId;
    }

    private UUID insertFailedOutboxEvent(String schema, UUID transactionId) throws SQLException {
        UUID eventId = UUID.randomUUID();
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     INSERT INTO %s.outbox_events (
                         id,
                         aggregate_type,
                         aggregate_id,
                         event_type,
                         payload,
                         status,
                         created_at,
                         next_attempt_at,
                         retry_count,
                         last_error
                     ) VALUES (?, 'TRANSACTION', ?, 'TRANSACTION_CREATED',
                               CAST(? AS JSONB), 'FAILED', ?, ?, 8, ?)
                     """.formatted(schema))) {
            Instant historicalTime = Instant.parse("2026-08-08T09:02:11Z");
            statement.setObject(1, eventId);
            statement.setObject(2, transactionId);
            statement.setString(3, historicalV1Payload(eventId, transactionId));
            statement.setTimestamp(4, Timestamp.from(historicalTime));
            statement.setTimestamp(5, Timestamp.from(historicalTime));
            statement.setString(6, "historical broker outage");
            assertThat(statement.executeUpdate()).isOne();
        }
        return eventId;
    }

    private String historicalV1Payload(UUID eventId, UUID transactionId) {
        return """
                {
                  "eventId": "%s",
                  "eventType": "TRANSACTION_CREATED",
                  "occurredAt": "2026-08-08T09:02:11Z",
                  "transaction": {
                    "transactionId": "%s",
                    "externalReference": "OUTBOX-QUARANTINE",
                    "amount": 150000,
                    "currency": "INR",
                    "type": "DEBIT",
                    "status": "PENDING",
                    "sourceAccount": "1234567890",
                    "destinationAccount": "9876543210",
                    "channel": "UPI",
                    "createdAt": "2026-08-08T09:02:11Z",
                    "metadata": {}
                  }
                }
                """.formatted(eventId, transactionId);
    }

    private void setOutboxStatus(String schema, UUID eventId, String status) throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement(
                     "UPDATE %s.outbox_events SET status = ? WHERE id = ?".formatted(schema)
             )) {
            statement.setString(1, status);
            statement.setObject(2, eventId);
            statement.executeUpdate();
        }
    }

    private void releaseQuarantinedEvent(String schema, UUID eventId) throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     UPDATE %s.outbox_events
                     SET status = 'PENDING',
                         quarantined_at = NULL,
                         quarantine_reason = NULL,
                         quarantined_from_status = NULL
                     WHERE id = ?
                     """.formatted(schema))) {
            statement.setObject(1, eventId);
            statement.executeUpdate();
        }
    }

    private Instant readNextAttemptAt(String schema, UUID eventId) throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement(
                     "SELECT next_attempt_at FROM %s.outbox_events WHERE id = ?".formatted(schema)
             )) {
            statement.setObject(1, eventId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getTimestamp("next_attempt_at").toInstant();
            }
        }
    }

    private void mutateAuditRow(String schema, UUID transactionId) throws SQLException {
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     UPDATE %s.transaction_reference_canonicalization_audit
                     SET new_reference = 'CHANGED'
                     WHERE transaction_id = ?
                     """.formatted(schema))) {
            statement.setObject(1, transactionId);
            statement.executeUpdate();
        }
    }

    private java.sql.Connection connection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
    }
}

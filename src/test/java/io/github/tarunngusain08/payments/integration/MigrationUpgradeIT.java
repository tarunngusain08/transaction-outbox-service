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
                .hasMessageContaining("canonicalization audit is immutable");
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

package io.github.tarunngusain08.payments.integration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class V7PreflightIT {

    private static final String PREFLIGHT_IN_CONTAINER = "/tmp/v7-preflight.sql";

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
    void failsClosedForUnresolvedRowsAndRejectsPostV7Reruns() throws Exception {
        flyway(MigrationVersion.fromVersion("6")).migrate();
        POSTGRES.copyFileToContainer(
                MountableFile.forHostPath(Path.of(
                        "scripts", "sql", "preflight_v7_event_compatibility.sql"
                ).toAbsolutePath()),
                PREFLIGHT_IN_CONTAINER
        );

        var emptyResult = runPreflight();
        assertThat(emptyResult.getExitCode()).isZero();
        assertThat(output(emptyResult)).contains("unresolved_historical_unpublished_events");

        setFlywayChecksum("3", 0);
        var divergentResult = runPreflight();
        assertThat(divergentResult.getExitCode()).isNotZero();
        assertThat(output(divergentResult))
                .contains("V7 preflight requires the exact canonical Flyway V1-V6 state");
        setFlywayChecksum("3", -915971206);

        insertUnresolvedHistoricalEvent();

        var unresolvedResult = runPreflight();
        assertThat(unresolvedResult.getExitCode()).isNotZero();
        assertThat(output(unresolvedResult))
                .contains("V7 release gate failed: 1 unresolved historical unpublished event(s)");

        flyway(null).migrate();

        var postV7Result = runPreflight();
        assertThat(postV7Result.getExitCode()).isNotZero();
        assertThat(output(postV7Result))
                .contains("V7 preflight requires the exact canonical Flyway V1-V6 state");
    }

    private Flyway flyway(MigrationVersion target) {
        var configuration = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration");
        if (target != null) {
            configuration.target(target);
        }
        return configuration.load();
    }

    private void insertUnresolvedHistoricalEvent() throws Exception {
        UUID transactionId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-08-08T09:02:11Z");
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        )) {
            try (var transaction = connection.prepareStatement("""
                    INSERT INTO transactions (
                        id, external_reference, amount_minor, currency, type, status,
                        source_account, destination_account, channel, created_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSONB))
                    """)) {
                transaction.setObject(1, transactionId);
                transaction.setString(2, "V7-GATE-UNRESOLVED");
                transaction.setLong(3, 150_000L);
                transaction.setString(4, "INR");
                transaction.setString(5, "DEBIT");
                transaction.setString(6, "PENDING");
                transaction.setString(7, "1234567890");
                transaction.setString(8, "9876543210");
                transaction.setString(9, "UPI");
                transaction.setTimestamp(10, Timestamp.from(createdAt));
                transaction.setString(11, "{}");
                assertThat(transaction.executeUpdate()).isOne();
            }

            try (var event = connection.prepareStatement("""
                    INSERT INTO outbox_events (
                        id, aggregate_type, aggregate_id, event_type, payload, status,
                        created_at, next_attempt_at, retry_count
                    ) VALUES (?, 'TRANSACTION', ?, 'TRANSACTION_CREATED',
                              CAST(? AS JSONB), 'PENDING', ?, ?, 0)
                    """)) {
                event.setObject(1, UUID.randomUUID());
                event.setObject(2, transactionId);
                event.setString(3, "{\"eventType\":\"TRANSACTION_CREATED\"}");
                event.setTimestamp(4, Timestamp.from(createdAt));
                event.setTimestamp(5, Timestamp.from(createdAt));
                assertThat(event.executeUpdate()).isOne();
            }
        }
    }

    private void setFlywayChecksum(String version, int checksum) throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        ); var statement = connection.prepareStatement("""
                UPDATE flyway_schema_history
                SET checksum = ?
                WHERE version = ?
                """)) {
            statement.setInt(1, checksum);
            statement.setString(2, version);
            assertThat(statement.executeUpdate()).isOne();
        }
    }

    private org.testcontainers.containers.Container.ExecResult runPreflight() throws Exception {
        return POSTGRES.execInContainer(
                "psql",
                "-X",
                "--set=ON_ERROR_STOP=1",
                "-U", POSTGRES.getUsername(),
                "-d", POSTGRES.getDatabaseName(),
                "--file=" + PREFLIGHT_IN_CONTAINER
        );
    }

    private String output(org.testcontainers.containers.Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }
}

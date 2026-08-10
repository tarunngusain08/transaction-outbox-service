package io.github.tarunngusain08.payments.integration;

import io.github.tarunngusain08.payments.PaymentServiceApplication;
import io.github.tarunngusain08.payments.outbox.OutboxEventRepository;
import io.github.tarunngusain08.payments.outbox.OutboxStatus;
import io.github.tarunngusain08.payments.transaction.PaymentChannel;
import io.github.tarunngusain08.payments.transaction.TransactionService;
import io.github.tarunngusain08.payments.transaction.TransactionType;
import io.github.tarunngusain08.payments.transaction.api.CreateTransactionRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class HistoricalOutboxCompatibilityIT {

    private static final String TOPIC = "payments.transactions.created.compatibility-it";

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

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(
                    "apache/kafka:4.1.2@sha256:"
                            + "5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e"
            ).asCompatibleSubstituteFor("apache/kafka")
    );

    @Test
    void previousM08DatabaseUpgradesWithoutPublishingHistoricalPayload() throws Exception {
        String schema = createSchema();
        flyway(schema, MigrationVersion.fromVersion("5")).migrate();
        UUID historicalTransactionId = insertHistoricalTransaction(schema);
        UUID historicalEventId = insertHistoricalFailedEvent(schema, historicalTransactionId);

        flyway(schema, MigrationVersion.fromVersion("6")).migrate();
        flyway(schema, null).migrate();
        createTopic();

        try (var context = new SpringApplicationBuilder(PaymentServiceApplication.class)
                .web(WebApplicationType.NONE)
                .run(applicationArguments(schema))) {
            var transactionService = context.getBean(TransactionService.class);
            var outboxRepository = context.getBean(OutboxEventRepository.class);
            var objectMapper = context.getBean(ObjectMapper.class);

            var created = transactionService.create(new CreateTransactionRequest(
                    "COMPATIBILITY_IT",
                    "V2-PIPELINE-" + UUID.randomUUID(),
                    150_000L,
                    "INR",
                    TransactionType.DEBIT,
                    "1234567890",
                    "9876543210",
                    PaymentChannel.UPI,
                    null,
                    Map.of("testRun", "historical-compatibility")
            ));
            UUID currentTransactionId = created.transaction().transactionId();

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                var events = outboxRepository.findAll();
                assertThat(events).anySatisfy(event -> {
                    assertThat(event.getId()).isEqualTo(historicalEventId);
                    assertThat(event.getStatus()).isEqualTo(OutboxStatus.QUARANTINED);
                    assertThat(event.getClaimToken()).isNull();
                    assertThat(event.getPublishedAt()).isNull();
                });
                assertThat(events).anySatisfy(event -> {
                    assertThat(event.getAggregateId()).isEqualTo(currentTransactionId);
                    assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
                });
            });

            var currentEvent = outboxRepository.findAll().stream()
                    .filter(event -> event.getAggregateId().equals(currentTransactionId))
                    .findFirst()
                    .orElseThrow();
            Set<UUID> publishedEventIds = consumeEventIds(objectMapper, currentEvent.getId());

            assertThat(publishedEventIds).contains(currentEvent.getId());
            assertThat(publishedEventIds).doesNotContain(historicalEventId);
        }
    }

    private String[] applicationArguments(String schema) {
        return new String[]{
            "--spring.datasource.url=" + schemaJdbcUrl(schema),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--spring.flyway.default-schema=" + schema,
            "--spring.flyway.schemas=" + schema,
            "--spring.jpa.properties.hibernate.default_schema=" + schema,
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
            "--payments.outbox.topic=" + TOPIC,
            "--payments.outbox.poll-delay=50ms",
            "--spring.main.banner-mode=off",
            "--logging.level.root=WARN"
        };
    }

    private Set<UUID> consumeEventIds(ObjectMapper objectMapper, UUID expectedEventId) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "compatibility-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Set<UUID> eventIds = new HashSet<>();
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline) && !eventIds.contains(expectedEventId)) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    var event = objectMapper.readTree(record.value());
                    assertThat(event.path("schemaVersion").asInt()).isEqualTo(2);
                    eventIds.add(UUID.fromString(event.path("eventId").asText()));
                });
            }
        }
        return eventIds;
    }

    private void createTopic() throws Exception {
        var properties = Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers()
        );
        try (var admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private String createSchema() throws SQLException {
        String schema = "historical_compatibility_"
                + UUID.randomUUID().toString().replace("-", "");
        try (var connection = connection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        return schema;
    }

    private UUID insertHistoricalTransaction(String schema) throws SQLException {
        UUID transactionId = UUID.randomUUID();
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     INSERT INTO %s.transactions (
                         id, external_reference, amount_minor, currency, type, status,
                         source_account, destination_account, channel, created_at, metadata
                     ) VALUES (?, ?, 150000, 'INR', 'DEBIT', 'PENDING',
                               '1234567890', '9876543210', 'UPI', ?, CAST('{}' AS JSONB))
                     """.formatted(schema))) {
            statement.setObject(1, transactionId);
            statement.setString(2, "HISTORICAL-V1-" + transactionId);
            statement.setTimestamp(3, Timestamp.from(Instant.parse("2026-08-08T09:02:11Z")));
            assertThat(statement.executeUpdate()).isOne();
        }
        return transactionId;
    }

    private UUID insertHistoricalFailedEvent(String schema, UUID transactionId)
            throws SQLException {
        UUID eventId = UUID.randomUUID();
        try (var connection = connection();
             var statement = connection.prepareStatement("""
                     INSERT INTO %s.outbox_events (
                         id, aggregate_type, aggregate_id, event_type, payload, status,
                         created_at, next_attempt_at, retry_count, last_error
                     ) VALUES (?, 'TRANSACTION', ?, 'TRANSACTION_CREATED', CAST(? AS JSONB),
                               'FAILED', ?, ?, 8, 'historical broker outage')
                     """.formatted(schema))) {
            Instant historicalTime = Instant.parse("2026-08-08T09:02:11Z");
            statement.setObject(1, eventId);
            statement.setObject(2, transactionId);
            statement.setString(3, historicalV1Payload(eventId, transactionId));
            statement.setTimestamp(4, Timestamp.from(historicalTime));
            statement.setTimestamp(5, Timestamp.from(historicalTime));
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
                    "externalReference": "HISTORICAL-V1",
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

    private String schemaJdbcUrl(String schema) {
        String separator = POSTGRES.getJdbcUrl().contains("?") ? "&" : "?";
        return POSTGRES.getJdbcUrl() + separator + "currentSchema=" + schema;
    }

    private java.sql.Connection connection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
    }
}
